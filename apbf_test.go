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
	"fmt"
	"math/bits"
	"testing"
)

func TestApbfContainsImmediately(t *testing.T) {
	bloom := newApbf(10, 20, 64)
	bloom.insert("42")
	if !bloom.contains("42") {
		t.Error("bloom filter should contain value just inserted")
	}
}

func TestApbfContainsInWindow(t *testing.T) {
	bloom := newApbf(10, 20, 64)
	bloom.insert("42")

	for i := range bloom.window() {
		bloom.insert(fmt.Sprintf("dummy-%d", i))

		if !bloom.contains("42") {
			t.Errorf("bloom filter should contain value inserted inside window")
		}
	}
}

func TestApbfContainsForgets(t *testing.T) {
	bloom := newApbf(10, 20, 64)
	bloom.insert("42")

	for i := range bloom.window() + bloom.slack() {
		bloom.insert(fmt.Sprintf("dummy-%d", i))
	}

	if bloom.contains("42") {
		t.Errorf("bloom filter should not contain value inserted outside of window + slack")
	}
}

func TestApbfInsert(t *testing.T) {
	bloom := newApbf(10, 20, 64)
	bloom.insert("42")

	for i := range bloom.k {
		slice := bloom.slice(i)
		var ones int
		for _, chunk := range slice {
			ones += bits.OnesCount8(uint8(chunk))
		}
		if ones != 1 {
			t.Errorf("expected only one bit set: %d bits set", ones)
		}
	}

	for j := range bloom.l {
		i := bloom.k + j
		slice := bloom.slice(i)
		var ones int
		for _, chunk := range slice {
			ones += bits.OnesCount8(uint8(chunk))
		}
		if ones != 0 {
			t.Errorf("expected zero bits set: %d bits set", ones)
		}
	}
}

func TestApbfShift(t *testing.T) {
	bloom := newApbf(10, 20, 256)
	for i := range bloom.g {
		bloom.insert(fmt.Sprintf("dummy-%d", i))
	}

	if bloom.p != 0 {
		t.Errorf("expected bloom.p to be zero: %d", bloom.p)
	}

	bloom.insert("42")
	if bloom.p != bloom.k + bloom.l - 1 {
		t.Errorf("expected p to shift: %d", bloom.p)
	}
}
