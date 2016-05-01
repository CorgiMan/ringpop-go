// Copyright (c) 2015 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package hashring

import "sort"

func radixSort(hashes, swap []uint64) {
	var counts [4][65536]int

	// count digit frequency for the 4 digits of base 65536
	for _, h := range hashes {
		counts[0][h%65536]++
		h >>= 16
		counts[1][h%65536]++
		h >>= 16
		counts[2][h%65536]++
		h >>= 16
		counts[3][h%65536]++
	}

	// calculate last index of digit i with value j
	for i := 0; i < 4; i++ {
		for j := 1; j < len(counts[i]); j++ {
			counts[i][j] = counts[i][j-1]
		}
	}

	// insert the values at the correct position
	for i := 0; i < 4; i++ {
		for _, h := range hashes {
			counts[0][h%65536]--
			ix := counts[0][h%65536]
			swap[ix] = h
		}
		hashes, swap = swap, hashes
	}
}

func indexOf(slice []uint64, v uint64) int {
	if len(slice) <= 1 {
		return 0
	}
	mid := len(slice) / 2
	if slice[mid] < v {
		return mid + 1 + indexOf(slice[mid+1:], v)
	}
	return indexOf(slice[:mid+1], v)
}

func remove(slice []uint64, vs []uint64) []uint64 {
	sort.Sort(uint64slice(vs))

	j, k := 0, 0
	for i := range slice {
		// set j to the correct position
		for j < len(vs) && vs[j] < slice[i] {
			j++
		}

		// store when element is not in vs
		if j >= len(vs) || slice[i] < vs[j] {
			slice[k] = slice[i]
			k++
		}
	}

	return slice[:k]
}

type uint64slice []uint64

func (a uint64slice) Len() int           { return len(a) }
func (a uint64slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64slice) Less(i, j int) bool { return a[i] < a[j] }
