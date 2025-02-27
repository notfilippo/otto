// Copyright 2025 Filippo Rossi
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otto

import (
	"time"

	"github.com/axiomhq/hyperloglog"
)

type TrackerWindow struct {
	BucketDuration time.Duration
	BucketCount    int
}

type slidingTracker struct {
	windows map[string]*slidingWindow
}

type slidingWindow struct {
	buckets []*trackerBucket

	bucketDuration time.Duration
	bucketCount    int
}

type trackerBucket struct {
	hll   *hyperloglog.Sketch
	start time.Time
}

func newSlidingTracker(config map[string]TrackerWindow) *slidingTracker {
	current := time.Now()
	windows := make(map[string]*slidingWindow)
	for name, window := range config {
		buckets := make([]*trackerBucket, window.BucketCount)
		for i := range window.BucketCount {
			buckets[i] = &trackerBucket{
				start: current,
				hll:   hyperloglog.New16(),
			}
		}

		windows[name] = &slidingWindow{
			buckets:        buckets,
			bucketDuration: window.BucketDuration,
			bucketCount:    window.BucketCount,
		}
	}

	return &slidingTracker{windows}
}

func (t *slidingTracker) track(key uint64) {
	current := time.Now()
	for _, window := range t.windows {
		index := int(current.UnixNano()/int64(window.bucketDuration)) % window.bucketCount
		bucket := window.buckets[index]

		if current.Sub(bucket.start) >= window.bucketDuration {
			bucket.start = current.Truncate(window.bucketDuration)
			bucket.hll = hyperloglog.New16()
		}

		bucket.hll.InsertHash(key)
	}
}

func (t *slidingTracker) stats() map[string]uint64 {
	current := time.Now()
	result := make(map[string]uint64)
	for name, window := range t.windows {
		merged := hyperloglog.New16()
		windowDuration := time.Duration(window.bucketCount) * window.bucketDuration
		for _, bucket := range window.buckets {
			if current.Sub(bucket.start) < windowDuration {
				merged.Merge(bucket.hll)
			}
		}

		result[name] = merged.Estimate()
	}

	return result
}
