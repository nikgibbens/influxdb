package launcher_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influxd/launcher"
	phttp "github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/influxdata/influxdb/query"
)

func TestPipeline_Write_Query_FieldKey(t *testing.T) {
	be := launcher.RunTestLauncherOrFail(t, ctx)
	be.SetupOrFail(t)
	defer be.ShutdownOrFail(t, ctx)

	resp, err := nethttp.DefaultClient.Do(
		be.MustNewHTTPRequest(
			"POST",
			fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", be.Org.ID, be.Bucket.ID),
			`cpu,region=west,server=a v0=1.2
cpu,region=west,server=b v0=33.2
cpu,region=east,server=b,area=z v1=100.0
disk,regions=north,server=b v1=101.2
mem,server=b value=45.2`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Error(err)
		}
	}()
	if resp.StatusCode != 204 {
		t.Fatal("failed call to write points")
	}

	rawQ := fmt.Sprintf(`from(bucket:"%s")
	|> range(start:-1m)
	|> filter(fn: (r) => r._measurement ==  "cpu" and (r._field == "v1" or r._field == "v0"))
	|> group(columns:["_time", "_value"], mode:"except")
	`, be.Bucket.Name)

	// Expected keys:
	//
	// _measurement=cpu,region=west,server=a,_field=v0
	// _measurement=cpu,region=west,server=b,_field=v0
	// _measurement=cpu,region=east,server=b,area=z,_field=v1
	//
	results := be.MustExecuteQuery(rawQ)
	defer results.Done()
	results.First(t).HasTablesWithCols([]int{4, 4, 5})
}

// This test initialises a default launcher writes some data,
// and checks that the queried results contain the expected number of tables
// and expected number of columns.
func TestPipeline_WriteV2_Query(t *testing.T) {
	be := launcher.RunTestLauncherOrFail(t, ctx)
	be.SetupOrFail(t)
	defer be.ShutdownOrFail(t, ctx)

	// The default gateway instance inserts some values directly such that ID lookups seem to break,
	// so go the roundabout way to insert things correctly.
	req := be.MustNewHTTPRequest(
		"POST",
		fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", be.Org.ID, be.Bucket.ID),
		fmt.Sprintf("ctr n=1i %d", time.Now().UnixNano()),
	)
	phttp.SetToken(be.Auth.Token, req)

	resp, err := nethttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Error(err)
		}
	}()

	if resp.StatusCode != nethttp.StatusNoContent {
		buf := new(bytes.Buffer)
		if _, err := io.Copy(buf, resp.Body); err != nil {
			t.Fatalf("Could not read body: %s", err)
		}
		t.Fatalf("exp status %d; got %d, body: %s", nethttp.StatusNoContent, resp.StatusCode, buf.String())
	}

	res := be.MustExecuteQuery(fmt.Sprintf(`from(bucket:"%s") |> range(start:-5m)`, be.Bucket.Name))
	defer res.Done()
	res.HasTableCount(t, 1)
}

func getMemoryUnused(t *testing.T, reg *prom.Registry) int64 {
	t.Helper()

	ms, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range ms {
		if m.GetName() == "query_control_memory_unused_bytes" {
			return int64(*m.GetMetric()[0].Gauge.Value)
		}
	}
	t.Errorf("query metric for unused memory not found")
	return 0
}

func writeBytes(t *testing.T, l *launcher.TestLauncher, tagValue string, bs int) int {
	// every point is:
	//    1 byte measurement ("m")
	//  + 2 bytes tag ("v1")
	//  + 64 bytes field
	//  + 64 bytes timestamp
	//  ---------------------------
	//  = 131 bytes
	if bs < 131 {
		bs = 131
	}
	n := bs / 131
	if n*131 < bs {
		n++
	}
	for i := 0; i < n; i++ {
		l.WritePointsOrFail(t, fmt.Sprintf(`m,t=%s f=%di %d`, tagValue, i*100, time.Now().UnixNano()))
	}
	return n * 131
}

func queryPoints(t *testing.T, l *launcher.TestLauncher, tagValue string) error {
	filterExpression := fmt.Sprintf("r.t == \"%s\"", tagValue)
	if tagValue == "" {
		filterExpression = "true"
	}
	qs := fmt.Sprintf(`from(bucket:"%s") |> range(start:-5m) |> filter(fn: (r) => %s)`, l.Bucket.Name, filterExpression)
	pkg, err := flux.Parse(qs)
	if err != nil {
		t.Fatal(err)
	}
	req := &query.Request{
		Authorization:  l.Auth,
		OrganizationID: l.Org.ID,
		Compiler: lang.ASTCompiler{
			AST: pkg,
		},
	}
	return l.QueryAndNopConsume(context.Background(), req)
}

// This test:
//  - initializes a default launcher and sets memory limits;
//  - writes some data;
//  - queries the data;
//  - verifies that the query fails (or not) and that the memory was de-allocated.
func TestPipeline_QueryMemoryLimits(t *testing.T) {
	tcs := []struct {
		name           string
		args           []string
		err            bool
		querySizeBytes int
		// max_memory - per_query_memory * concurrency
		unusedMemoryBytes int
	}{
		{
			name: "ok - initial memory bytes, memory bytes, and max memory set",
			args: []string{
				"--query-concurrency", "1",
				"--query-initial-memory-bytes", "100",
				"--query-max-memory-bytes", "1048576", // 1MB
			},
			querySizeBytes:    30000,
			err:               false,
			unusedMemoryBytes: 1048476,
		},
		{
			name: "error - memory bytes and max memory set",
			args: []string{
				"--query-concurrency", "1",
				"--query-memory-bytes", "1",
				"--query-max-memory-bytes", "100",
			},
			querySizeBytes:    2,
			err:               true,
			unusedMemoryBytes: 99,
		},
		{
			name: "error - initial memory bytes and max memory set",
			args: []string{
				"--query-concurrency", "1",
				"--query-initial-memory-bytes", "1",
				"--query-max-memory-bytes", "100",
			},
			querySizeBytes:    101,
			err:               true,
			unusedMemoryBytes: 99,
		},
		{
			name: "error - initial memory bytes, memory bytes, and max memory set",
			args: []string{
				"--query-concurrency", "1",
				"--query-initial-memory-bytes", "1",
				"--query-memory-bytes", "50",
				"--query-max-memory-bytes", "100",
			},
			querySizeBytes:    51,
			err:               true,
			unusedMemoryBytes: 99,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			l := launcher.RunTestLauncherOrFail(t, ctx, tc.args...)
			l.SetupOrFail(t)
			defer l.ShutdownOrFail(t, ctx)

			const tagValue = "t0"
			writeBytes(t, l, tagValue, tc.querySizeBytes)
			if err := queryPoints(t, l, tagValue); err != nil {
				if tc.err {
					if !strings.Contains(err.Error(), "allocation limit reached") {
						t.Errorf("query errored with unexpected error: %v", err)
					}
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			} else if tc.err {
				t.Errorf("expected error, got successful query execution")
			}

			reg := l.Registry()
			got := getMemoryUnused(t, reg)
			want := int64(tc.unusedMemoryBytes)
			if want != got {
				t.Errorf("expected unused memory %d, got %d", want, got)
			}
		})
	}
}

// This test:
//  - initializes a default launcher and sets memory limits;
//  - writes some data;
//  - launches several queries that may exceed the memory limit;
//  - verifies after each query run the used memory.
func TestPipeline_QueryMemoryLimits_MultipleQueries(t *testing.T) {
	l := launcher.RunTestLauncherOrFail(t, ctx,
		"--log-level", "error",
		"--query-queue-size", "1024",
		"--query-concurrency", "1",
		"--query-initial-memory-bytes", "100",
		"--query-memory-bytes", "50000",
		"--query-max-memory-bytes", "200000",
	)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	// one tag does not exceed memory.
	const tOK = "t0"
	writeBytes(t, l, tOK, 40000)
	// the other does.
	const tKO = "t1"
	writeBytes(t, l, tKO, 100000)

	checkMemoryUsed := func() {
		t.Helper()
		// base memory used is equal to initial memory bytes * concurrency.
		const baseMemoryUsed = int64(100)
		got := l.QueryController().GetUsedMemoryBytes()
		if baseMemoryUsed != got {
			t.Errorf("expected unused memory %d, got %d", baseMemoryUsed, got)
		}
	}

	runQueries := func(nOK, nKO int) {
		// This flock of queries should run sequentially because concurrency quota is set to 1
		wg := sync.WaitGroup{}
		wg.Add(nOK + nKO)
		for i := 0; i < nOK; i++ {
			go func(idx int) {
				t.Logf("running query %d - OK", idx)
				if err := queryPoints(t, l, tOK); err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				checkMemoryUsed()
				wg.Done()
			}(i)
		}
		for i := 0; i < nKO; i++ {
			go func(idx int) {
				t.Logf("running query %d - KO", idx)
				if err := queryPoints(t, l, tKO); err == nil {
					t.Errorf("expected error got none")
				} else if !strings.Contains(err.Error(), "allocation limit reached") {
					t.Errorf("got wrong error: %v", err)
				}
				checkMemoryUsed()
				wg.Done()
			}(i)
		}
		wg.Wait()
	}

	runQueries(100, 0)
	//runQueries(5, 5)
	//runQueries(0, 50)
}

func TestPipeline_Query_LoadSecret_Success(t *testing.T) {
	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	const key, value = "mytoken", "secrettoken"
	if err := l.SecretService().PutSecret(ctx, l.Org.ID, key, value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// write one point so we can use it
	l.WritePointsOrFail(t, fmt.Sprintf(`m,k=v1 f=%di %d`, 0, time.Now().UnixNano()))

	// we expect this request to succeed
	req := &query.Request{
		Authorization:  l.Auth,
		OrganizationID: l.Org.ID,
		Compiler: lang.FluxCompiler{
			Query: fmt.Sprintf(`
import "influxdata/influxdb/secrets"

token = secrets.get(key: "mytoken")
from(bucket: "%s")
	|> range(start: -5m)
	|> set(key: "token", value: token)
`, l.Bucket.Name),
		},
	}
	if err := l.QueryAndConsume(ctx, req, func(r flux.Result) error {
		return r.Tables().Do(func(tbl flux.Table) error {
			return tbl.Do(func(cr flux.ColReader) error {
				j := execute.ColIdx("token", cr.Cols())
				if j == -1 {
					return errors.New("cannot find table column \"token\"")
				}

				for i := 0; i < cr.Len(); i++ {
					v := execute.ValueForRow(cr, i, j)
					if got, want := v, values.NewString("secrettoken"); !got.Equal(want) {
						t.Errorf("unexpected value at row %d -want/+got:\n\t- %v\n\t+ %v", i, got, want)
					}
				}
				return nil
			})
		})
	}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestPipeline_Query_LoadSecret_Forbidden(t *testing.T) {
	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	const key, value = "mytoken", "secrettoken"
	if err := l.SecretService().PutSecret(ctx, l.Org.ID, key, value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// write one point so we can use it
	l.WritePointsOrFail(t, fmt.Sprintf(`m,k=v1 f=%di %d`, 0, time.Now().UnixNano()))

	auth := &influxdb.Authorization{
		OrgID:  l.Org.ID,
		UserID: l.User.ID,
		Permissions: []influxdb.Permission{
			{
				Action: influxdb.ReadAction,
				Resource: influxdb.Resource{
					Type:  influxdb.BucketsResourceType,
					ID:    &l.Bucket.ID,
					OrgID: &l.Org.ID,
				},
			},
		},
	}
	if err := l.AuthorizationService(t).CreateAuthorization(ctx, auth); err != nil {
		t.Fatalf("unexpected error creating authorization: %s", err)
	}
	l.Auth = auth

	// we expect this request to succeed
	req := &query.Request{
		Authorization:  l.Auth,
		OrganizationID: l.Org.ID,
		Compiler: lang.FluxCompiler{
			Query: fmt.Sprintf(`
import "influxdata/influxdb/secrets"

token = secrets.get(key: "mytoken")
from(bucket: "%s")
	|> range(start: -5m)
	|> set(key: "token", value: token)
`, l.Bucket.Name),
		},
	}
	if err := l.QueryAndNopConsume(ctx, req); err == nil {
		t.Error("expected error")
	} else if got, want := influxdb.ErrorCode(err), influxdb.EUnauthorized; got != want {
		t.Errorf("unexpected error code -want/+got:\n\t- %v\n\t+ %v", got, want)
	}
}

// We need a separate test for dynamic queries because our Flux e2e tests cannot test them now.
// Indeed, tableFind would fail while initializing the data in the input bucket, because the data is not
// written, and tableFind would complain not finding the tables.
// This will change once we make side effects drive execution and remove from/to concurrency in our e2e tests.
// See https://github.com/influxdata/flux/issues/1799.
func TestPipeline_DynamicQuery(t *testing.T) {
	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	l.WritePointsOrFail(t, `
m0,k=k0 f=0i 0
m0,k=k0 f=1i 1
m0,k=k0 f=2i 2
m0,k=k0 f=3i 3
m0,k=k0 f=4i 4
m0,k=k1 f=5i 5
m0,k=k1 f=6i 6
m1,k=k0 f=5i 7
m1,k=k2 f=0i 8
m1,k=k0 f=6i 9
m1,k=k1 f=6i 10
m1,k=k0 f=7i 11
m1,k=k0 f=5i 12
m1,k=k1 f=8i 13
m1,k=k2 f=9i 14
m1,k=k3 f=5i 15`)

	// How many points do we have in stream2 with the same values of the ones in the table with key k0 in stream1?
	// The only point matching the description is `m1,k=k2 f=0i 8`, because its value is in the set [0, 1, 2, 3, 4].
	dq := fmt.Sprintf(`
stream1 = from(bucket: "%s") |> range(start: 0) |> filter(fn: (r) => r._measurement == "m0" and r._field == "f")
stream2 = from(bucket: "%s") |> range(start: 0) |> filter(fn: (r) => r._measurement == "m1" and r._field == "f")
col = stream1 |> tableFind(fn: (key) => key.k == "k0") |> getColumn(column: "_value")
// Here is where dynamicity kicks in.
stream2 |> filter(fn: (r) => contains(value: r._value, set: col)) |> group() |> count() |> yield(name: "dynamic")`,
		l.Bucket.Name, l.Bucket.Name)
	req := &query.Request{
		Authorization:  l.Auth,
		OrganizationID: l.Org.ID,
		Compiler:       lang.FluxCompiler{Query: dq},
	}
	noRes := 0
	if err := l.QueryAndConsume(ctx, req, func(r flux.Result) error {
		noRes++
		if n := r.Name(); n != "dynamic" {
			t.Fatalf("got unexpected result: %s", n)
		}
		noTables := 0
		if err := r.Tables().Do(func(tbl flux.Table) error {
			return tbl.Do(func(cr flux.ColReader) error {
				noTables++
				j := execute.ColIdx("_value", cr.Cols())
				if j == -1 {
					return errors.New("cannot find table column \"_value\"")
				}
				if want := 1; cr.Len() != want {
					t.Fatalf("wrong number of rows in table: -want/+got:\n\t- %d\n\t+ %d", want, cr.Len())
				}
				v := execute.ValueForRow(cr, 0, j)
				if got, want := v, values.NewInt(1); !got.Equal(want) {
					t.Errorf("unexpected value at row %d -want/+got:\n\t- %v\n\t+ %v", 0, want, got)
				}
				return nil
			})
		}); err != nil {
			return err
		}
		if want := 1; noTables != want {
			t.Fatalf("wrong number of tables in result: -want/+got:\n\t- %d\n\t+ %d", want, noRes)
		}
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if want := 1; noRes != want {
		t.Fatalf("wrong number of results: -want/+got:\n\t- %d\n\t+ %d", want, noRes)
	}
}

func TestPipeline_Query_ExperimentalTo(t *testing.T) {
	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	// Last row of data tests nil field value
	data := `
#datatype,string,long,dateTime:RFC3339,double,string,string,string,string
#group,false,false,false,false,true,true,true,true
#default,_result,,,,,,,
,result,table,_time,_value,_field,_measurement,cpu,host
,,0,2018-05-22T19:53:26Z,1.0,usage_guest,cpu,cpu-total,host.local
,,0,2018-05-22T19:53:36Z,1.1,usage_guest,cpu,cpu-total,host.local
,,1,2018-05-22T19:53:26Z,2.0,usage_guest_nice,cpu,cpu-total,host.local
,,1,2018-05-22T19:53:36Z,2.1,usage_guest_nice,cpu,cpu-total,host.local
,,2,2018-05-22T19:53:26Z,91.7364670583823,usage_idle,cpu,cpu-total,host.local
,,2,2018-05-22T19:53:36Z,89.51118889861233,usage_idle,cpu,cpu-total,host.local
,,3,2018-05-22T19:53:26Z,3.0,usage_iowait,cpu,cpu-total,host.local
,,3,2018-05-22T19:53:36Z,,usage_iowait,cpu,cpu-total,host.local
`
	pivotQuery := fmt.Sprintf(`
import "csv"
import "experimental"
import "influxdata/influxdb/v1"
csv.from(csv: "%s")
    |> range(start: 2018-05-21T00:00:00Z, stop: 2018-05-23T00:00:00Z)
    |> v1.fieldsAsCols()
`, data)
	res := l.MustExecuteQuery(pivotQuery)
	defer res.Done()
	pivotedResultIterator := flux.NewSliceResultIterator(res.Results)

	toQuery := pivotQuery + fmt.Sprintf(`|> experimental.to(bucket: "%s", org: "%s") |> yield(name: "_result")`,
		l.Bucket.Name, l.Org.Name)
	res = l.MustExecuteQuery(toQuery)
	defer res.Done()
	toOutputResultIterator := flux.NewSliceResultIterator(res.Results)

	// Make sure that experimental.to() echoes its input to its output
	if err := executetest.EqualResultIterators(pivotedResultIterator, toOutputResultIterator); err != nil {
		t.Fatal(err)
	}

	csvQuery := fmt.Sprintf(`
import "csv"
csv.from(csv: "%s")
  |> filter(fn: (r) => exists r._value)
`,
		data)
	res = l.MustExecuteQuery(csvQuery)
	defer res.Done()
	csvResultIterator := flux.NewSliceResultIterator(res.Results)

	fromQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: 2018-05-15T00:00:00Z, stop: 2018-06-01T00:00:00Z)
  |> drop(columns: ["_start", "_stop"])
`,
		l.Bucket.Name)
	res = l.MustExecuteQuery(fromQuery)
	defer res.Done()
	fromResultIterator := flux.NewSliceResultIterator(res.Results)

	// Make sure that the data we stored matches the CSV
	if err := executetest.EqualResultIterators(csvResultIterator, fromResultIterator); err != nil {
		t.Fatal(err)
	}
}
