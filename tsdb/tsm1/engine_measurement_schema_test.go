package tsm1_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxdb/tsdb/tsm1"
)

func TestEngine_MeasurementCancelContext(t *testing.T) {
	e, err := NewEngine(tsm1.NewConfig(), t)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	var (
		org    influxdb.ID = 0x6000
		bucket influxdb.ID = 0x6100
	)

	e.MustWritePointsString(org, bucket, `
cpuB,host=0B,os=linux value=1.1 101
cpuB,host=AB,os=linux value=1.2 102
cpuB,host=AB,os=linux value=1.3 104
cpuB,host=CB,os=linux value=1.3 104
cpuB,host=CB,os=linux value=1.3 105
cpuB,host=DB,os=macOS value=1.3 106
memB,host=DB,os=macOS value=1.3 101`)

	// send some points to TSM data
	e.MustWriteSnapshot()

	e.MustWritePointsString(org, bucket, `
cpuB,host=0B,os=linux value=1.1 201
cpuB,host=AB,os=linux value=1.2 202
cpuB,host=AB,os=linux value=1.3 204
cpuB,host=BB,os=linux value=1.3 204
cpuB,host=BB,os=linux value=1.3 205
cpuB,host=EB,os=macOS value=1.3 206
memB,host=EB,os=macOS value=1.3 201`)

	t.Run("cancel MeasurementNames", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		iter, err := e.MeasurementNames(ctx, org, bucket, 0, math.MaxInt64)
		if err == nil {
			t.Fatal("MeasurementNames: expected error but got nothing")
		} else if err.Error() != "context canceled" {
			t.Fatalf("MeasurementNames: error %v", err)
		}

		if got := iter.Stats(); !cmp.Equal(got, cursors.CursorStats{}) {
			t.Errorf("unexpected Stats: -got/+exp\n%v", cmp.Diff(got, cursors.CursorStats{}))
		}
	})
}

func TestEngine_MeasurementNames(t *testing.T) {
	e, err := NewEngine(tsm1.NewConfig(), t)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	orgs := []struct {
		org, bucket influxdb.ID
	}{
		{
			org:    0x5020,
			bucket: 0x5100,
		},
		{
			org:    0x6000,
			bucket: 0x6100,
		},
	}

	// this org will require escaping the 0x20 byte
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
cpu,cpu0=v,cpu1=v,cpu2=v f=1 101
cpu,cpu1=v               f=1 103
cpu,cpu2=v               f=1 105
cpu,cpu0=v,cpu2=v        f=1 107
cpu,cpu2=v,cpu3=v        f=1 109
mem,mem0=v,mem1=v        f=1 101`)
	e.MustWritePointsString(orgs[1].org, orgs[1].bucket, `
cpu,cpu0=v,cpu1=v,cpu2=v f=1 101
cpu,cpu1=v               f=1 103
cpu,cpu2=v               f=1 105
cpu,cpu0=v,cpu2=v        f=1 107
cpu,cpu2=v,cpu3=v        f=1 109
mem,mem0=v,mem1=v        f=1 101`)

	// send some points to TSM data
	e.MustWriteSnapshot()

	// delete some data from the first bucket
	e.MustDeleteBucketRange(orgs[0].org, orgs[0].bucket, 0, 105)

	// leave some points in the cache
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
cpu,cpu3=v,cpu4=v,cpu5=v f=1 201
cpu,cpu4=v               f=1 203
cpu,cpu3=v               f=1 205
cpu,cpu3=v,cpu4=v        f=1 207
cpu,cpu4=v,cpu5=v        f=1 209
mem,mem1=v,mem2=v        f=1 201`)
	e.MustWritePointsString(orgs[1].org, orgs[1].bucket, `
cpu,cpu3=v,cpu4=v,cpu5=v f=1 201
cpu,cpu4=v               f=1 203
cpu,cpu3=v               f=1 205
cpu,cpu3=v,cpu4=v        f=1 207
cpu,cpu4=v,cpu5=v        f=1 209
mem,mem1=v,mem2=v        f=1 201`)

	type args struct {
		org      int
		min, max int64
		expr     string
	}

	var tests = []struct {
		name     string
		args     args
		exp      []string
		expStats cursors.CursorStats
	}{
		// ***********************
		// * queries for the first org, which has some deleted data
		// ***********************

		{
			name: "TSM and cache",
			args: args{
				org: 0,
				min: 0,
				max: 300,
			},
			exp:      []string{"cpu", "mem"},
			expStats: cursors.CursorStats{ScannedValues: 1, ScannedBytes: 8},
		},
		{
			name: "only TSM",
			args: args{
				org: 0,
				min: 0,
				max: 199,
			},
			exp:      []string{"cpu"},
			expStats: cursors.CursorStats{ScannedValues: 1, ScannedBytes: 8},
		},
		{
			name: "only cache",
			args: args{
				org: 0,
				min: 200,
				max: 299,
			},
			exp:      []string{"cpu", "mem"},
			expStats: cursors.CursorStats{ScannedValues: 2, ScannedBytes: 16},
		},
		{
			name: "one timestamp TSM/data",
			args: args{
				org: 0,
				min: 107,
				max: 107,
			},
			exp:      []string{"cpu"},
			expStats: cursors.CursorStats{ScannedValues: 1, ScannedBytes: 8},
		},
		{
			name: "one timestamp cache/data",
			args: args{
				org: 0,
				min: 207,
				max: 207,
			},
			exp:      []string{"cpu"},
			expStats: cursors.CursorStats{ScannedValues: 3, ScannedBytes: 24},
		},
		{
			name: "one timestamp TSM/nodata",
			args: args{
				org: 0,
				min: 102,
				max: 102,
			},
			exp:      nil,
			expStats: cursors.CursorStats{ScannedValues: 6, ScannedBytes: 48},
		},
		{
			name: "one timestamp cache/nodata",
			args: args{
				org: 0,
				min: 202,
				max: 202,
			},
			exp:      nil,
			expStats: cursors.CursorStats{ScannedValues: 6, ScannedBytes: 48},
		},

		// ***********************
		// * queries for the second org, which has no deleted data
		// ***********************
		{
			name: "TSM and cache",
			args: args{
				org: 1,
				min: 0,
				max: 300,
			},
			exp:      []string{"cpu", "mem"},
			expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("org%d/%s", tc.args.org, tc.name), func(t *testing.T) {
			a := tc.args

			iter, err := e.MeasurementNames(context.Background(), orgs[a.org].org, orgs[a.org].bucket, a.min, a.max)
			if err != nil {
				t.Fatalf("MeasurementNames: error %v", err)
			}

			if got := cursors.StringIteratorToSlice(iter); !cmp.Equal(got, tc.exp) {
				t.Errorf("unexpected MeasurementNames: -got/+exp\n%v", cmp.Diff(got, tc.exp))
			}

			if got := iter.Stats(); !cmp.Equal(got, tc.expStats) {
				t.Errorf("unexpected Stats: -got/+exp\n%v", cmp.Diff(got, tc.expStats))
			}
		})
	}
}
