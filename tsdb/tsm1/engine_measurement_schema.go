package tsm1

import (
	"bytes"
	"context"
	"sort"
	"strings"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxql"
)

// MeasurementNames returns an iterator which enumerates the measurements for the given
// bucket and limited to the time range (start, end].
//
// MeasurementNames will always return a StringIterator if there is no error.
//
// If the context is canceled before MeasurementNames has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementNames(ctx context.Context, orgID, bucketID influxdb.ID, start, end int64) (cursors.StringIterator, error) {
	orgBucket := tsdb.EncodeName(orgID, bucketID)
	// TODO(edd): we need to clean up how we're encoding the prefix so that we
	// don't have to remember to get it right everywhere we need to touch TSM data.
	prefix := models.EscapeMeasurement(orgBucket[:])

	var (
		tsmValues = make(map[string]struct{})
		stats     cursors.CursorStats
		canceled  bool
	)

	e.FileStore.ForEachFile(func(f TSMFile) bool {
		// Check the context before accessing each tsm file
		select {
		case <-ctx.Done():
			canceled = true
			return false
		default:
		}
		if f.OverlapsTimeRange(start, end) && f.OverlapsKeyPrefixRange(prefix, prefix) {
			iter := f.TimeRangeIterator(prefix, start, end)
			for i := 0; iter.Next(); i++ {
				sfkey := iter.Key()
				if !bytes.HasPrefix(sfkey, prefix) {
					// end of org+bucket
					break
				}

				key, _ := SeriesAndFieldFromCompositeKey(sfkey)
				name := models.ParseMeasurement(key)
				if len(name) == 0 {
					// INVARIANT: If key \x00 is missing the invariant is violated; skip key
					continue
				}

				if _, ok := tsmValues[string(name)]; ok {
					continue
				}

				if iter.HasData() {
					tsmValues[string(name)] = struct{}{}
				}
			}
			stats.Add(iter.Stats())
		}
		return true
	})

	if canceled {
		return cursors.NewStringSliceIteratorWithStats(nil, stats), ctx.Err()
	}

	// With performance in mind, we explicitly do not check the context
	// while scanning the entries in the cache.
	prefixStr := string(prefix)
	_ = e.Cache.ApplyEntryFn(func(sfkey string, entry *entry) error {
		if !strings.HasPrefix(sfkey, prefixStr) {
			return nil
		}

		// TODO(edd): consider the []byte() conversion here.
		key, _ := SeriesAndFieldFromCompositeKey([]byte(sfkey))
		name := models.ParseMeasurement(key)
		if len(name) == 0 {
			return nil
		}

		if _, ok := tsmValues[string(name)]; ok {
			return nil
		}

		stats.ScannedValues += entry.values.Len()
		stats.ScannedBytes += entry.values.Len() * 8 // sizeof timestamp

		if entry.values.Contains(start, end) {
			tsmValues[string(name)] = struct{}{}
		}
		return nil
	})

	vals := make([]string, 0, len(tsmValues))
	for val := range tsmValues {
		vals = append(vals, val)
	}
	sort.Strings(vals)

	return cursors.NewStringSliceIteratorWithStats(vals, stats), nil
}

// MeasurementTagValues returns an iterator which enumerates the tag values for the given
// bucket, measurement and tag key, filtered using the optional the predicate and limited to the
// time range (start, end].
//
// MeasurementTagValues will always return a StringIterator if there is no error.
//
// If the context is canceled before TagValues has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementTagValues(ctx context.Context, orgID, bucketID influxdb.ID, measurement, tagKey string, start, end int64, predicate influxql.Expr) (cursors.StringIterator, error) {
	return nil, nil
}

// MeasurementTagKeys returns an iterator which enumerates the tag keys for the given
// bucket and measurement, filtered using the optional the predicate and limited to the
//// time range (start, end].
//
// MeasurementTagKeys will always return a StringIterator if there is no error.
//
// If the context is canceled before MeasurementTagKeys has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementTagKeys(ctx context.Context, orgID, bucketID influxdb.ID, measurement string, start, end int64, predicate influxql.Expr) (cursors.StringIterator, error) {
	return nil, nil
}
