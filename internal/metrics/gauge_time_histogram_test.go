package metrics

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"
)

var testBuckets = []float64{
	(1 * time.Second).Seconds(),
	(10 * time.Second).Seconds(),
	(1 * time.Minute).Seconds(),
}

func TestTimeGaugeHistogram(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	tgh := NewTimeGaugeHistogram("test_executing_query", "help", reg, testBuckets)

	assert.Len(t, tgh.descs, len(testBuckets)+1)
	assert.Len(t, tgh.valuesCache, len(testBuckets)+1)
	assert.Nil(t, tgh.getter)

	getter := func() []time.Time {
		return []time.Time{
			time.Now(),
			time.Now().Add(-5 * time.Second),
			time.Now().Add(-30 * time.Second),
			time.Now().Add(-2 * time.Minute),
		}
	}

	tgh.AssignQueryGetter(getter)
	tgh.mu.Lock()
	tgh.recalc()
	tgh.mu.Unlock()

	for _, value := range tgh.valuesCache {
		assert.Equal(t, float64(1), value)
	}

	mfs, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, mfs, 1)
	require.Len(t, mfs[0].Metric, len(testBuckets)+1)

	byLe := map[string]float64{}
	for _, m := range mfs[0].Metric {
		var le string
		for _, lp := range m.Label {
			if lp.GetName() == "le" {
				le = lp.GetValue()
				break
			}
		}
		byLe[le] = m.Gauge.GetValue()
	}

	for _, le := range []string{"1", "10", "60", "+Inf"} {
		assert.Equal(t, float64(1), byLe[le], "le=%s", le)
	}
}

func TestTimeGaugeHistogramConcurrentAccess(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	tgh := NewTimeGaugeHistogram("test_concurrent_executing_query", "help", reg, testBuckets)
	tgh.AssignQueryGetter(func() []time.Time {
		return []time.Time{time.Now().Add(-5 * time.Second)}
	})

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = reg.Gather()
		}()
	}
	wg.Wait()

	mfs, err := reg.Gather()
	require.NoError(t, err)
	var found10 float64
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			var le string
			for _, lp := range m.Label {
				if lp.GetName() == "le" {
					le = lp.GetValue()
					break
				}
			}
			if le == "10" {
				found10 = m.Gauge.GetValue()
			}
		}
	}
	assert.Equal(t, float64(1), found10)
}

func TestTimeGaugeHistogramWithNilGetter(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	_ = NewTimeGaugeHistogram("test_nil_getter", "help", reg, testBuckets)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			assert.Equal(t, float64(0), m.Gauge.GetValue())
		}
	}
}
