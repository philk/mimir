// SPDX-License-Identifier: AGPL-3.0-only

package tsdb

import (
	"context"
	"reflect"
	"testing"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	promtsdb "github.com/prometheus/prometheus/tsdb"
	promchunks "github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"
)

// Regression test for an OOO compaction failure:
// "compact ooo head: chunk iter: cannot populate chunk ...: invalid head chunk: not found"
//
// OOO head chunk refs pack only 23 bits of chunk ID (bit 23 is reserved as an "is OOO" marker),
// so OOO chunk IDs wrap in a 23-bit ring. This test simulates a wrap scenario by setting
// firstOOOChunkID close to 1<<23.
func TestCompactOOOHead_DoesNotFailWhenOOOChunkIDWraps(t *testing.T) {
	const (
		oooChunkIDMask = uint64(1 << 23)
	)

	opts := promtsdb.DefaultOptions()
	opts.NoLockfile = true
	opts.WALSegmentSize = -1 // Disable WAL/WBL for the test.
	opts.MinBlockDuration = 1000
	opts.MaxBlockDuration = 1000
	opts.OutOfOrderTimeWindow = 10_000
	opts.OutOfOrderCapMax = 2

	db, err := promtsdb.Open(t.TempDir(), nil, prometheus.NewRegistry(), opts, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	db.DisableCompactions()

	metric := labels.FromStrings("__name__", "ooo_chunk_id_wrap", "job", "test")

	// First append an in-order sample, so subsequent samples can be classified as out-of-order.
	ref := appendSample(t, db, 0, metric, 1000, 1)

	// Insert enough out-of-order samples to:
	// - flush one OOO chunk to disk (by hitting OutOfOrderCapMax)
	// - leave one in-memory OOO head chunk that overlaps the flushed one
	//
	// This results in overlapping OOO chunks, which are represented via multiMeta during compaction.
	app := db.Appender(context.Background())
	_, err = app.Append(ref, metric, 800, 1)
	require.NoError(t, err)
	_, err = app.Append(ref, metric, 900, 2)
	require.NoError(t, err)
	_, err = app.Append(ref, metric, 850, 3)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// Simulate a series that has already garbage collected almost all its OOO chunks.
	// With 2 mmapped chunks present after compaction mmaps the current OOO head chunk,
	// the (firstOOOChunkID + pos) calculation will hit the wrap boundary.
	setFirstOOOChunkID(t, db.Head(), ref, promchunks.HeadChunkID(oooChunkIDMask-1))

	require.NoError(t, db.CompactOOOHead(context.Background()))
}

func appendSample(t *testing.T, db *promtsdb.DB, ref storage.SeriesRef, lset labels.Labels, ts int64, v float64) storage.SeriesRef {
	t.Helper()

	app := db.Appender(context.Background())
	newRef, err := app.Append(ref, lset, ts, v)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	return newRef
}

func setFirstOOOChunkID(t *testing.T, head *promtsdb.Head, seriesRef storage.SeriesRef, firstOOOChunkID promchunks.HeadChunkID) {
	t.Helper()

	// Reach into the promtsdb.Head internals:
	// head.series -> stripeSeries -> series[shard][ref] -> memSeries.ooo.firstOOOChunkID.
	headV := reflect.ValueOf(head).Elem()
	seriesV := headV.FieldByName("series")
	require.True(t, seriesV.IsValid(), "expected Head.series to exist")
	require.False(t, seriesV.IsNil(), "expected Head.series to be non-nil")

	stripesV := seriesV.Elem()
	sizeV := stripesV.FieldByName("size")
	require.True(t, sizeV.IsValid(), "expected stripeSeries.size to exist")
	stripeSize := int(sizeV.Int())
	require.Greater(t, stripeSize, 0, "expected stripeSeries.size > 0")

	seriesSliceV := stripesV.FieldByName("series")
	require.True(t, seriesSliceV.IsValid(), "expected stripeSeries.series to exist")
	require.Equal(t, stripeSize, seriesSliceV.Len(), "unexpected stripeSeries.series length")

	shard := int(uint64(seriesRef) & uint64(stripeSize-1))
	seriesMapV := seriesSliceV.Index(shard)

	memSeriesPtrV := seriesMapV.MapIndex(reflect.ValueOf(promchunks.HeadSeriesRef(seriesRef)))
	require.True(t, memSeriesPtrV.IsValid(), "expected series to exist in stripeSeries")
	require.False(t, memSeriesPtrV.IsNil(), "expected series pointer to be non-nil")

	// Map lookups return non-addressable values. Create an addressable view into memSeries.
	memSeriesV := reflect.NewAt(memSeriesPtrV.Type().Elem(), unsafe.Pointer(memSeriesPtrV.Pointer())).Elem()

	oooPtrV := memSeriesV.FieldByName("ooo")
	require.True(t, oooPtrV.IsValid(), "expected memSeries.ooo to exist")
	require.False(t, oooPtrV.IsNil(), "expected memSeries.ooo to be non-nil")

	firstV := oooPtrV.Elem().FieldByName("firstOOOChunkID")
	require.True(t, firstV.IsValid(), "expected memSeriesOOOFields.firstOOOChunkID to exist")
	require.True(t, firstV.CanAddr(), "expected memSeriesOOOFields.firstOOOChunkID to be addressable")

	// The field is unexported, so we need unsafe to modify it.
	reflect.NewAt(firstV.Type(), unsafe.Pointer(firstV.UnsafeAddr())).Elem().SetUint(uint64(firstOOOChunkID))
}
