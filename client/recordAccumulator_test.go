package client

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomRecord(n int) []byte {
	record := make([]byte, n)
	for i := 0; i < n; i++ {
		record[i] = letters[rand.Int63()%int64(len(letters))]
	}
	return record
}

func TestAppend(t *testing.T) {
	accumulator := newRecordAccumulator(23)
	cases := []struct {
		record []byte
		err    error
		idx    int
	}{
		{record: randomRecord(10), err: nil, idx: 0},
		{record: randomRecord(3), err: nil, idx: 1},
		{record: randomRecord(5), err: nil, idx: 2},
		{record: randomRecord(2), err: nil, idx: 3},
		{record: randomRecord(10), err: context.DeadlineExceeded},
		{record: randomRecord(10), err: context.DeadlineExceeded},
	}

	expectedLastOffset := 0
	for _, c := range cases {
		assert.Equal(t, accumulator.lastOffset, expectedLastOffset)

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(0))
		offset, err := accumulator.append(ctx, c.record)

		assert.Equal(t, err, c.err)
		assert.Equal(t, offset, c.idx)

		if err == nil {
			expectedLastOffset += len(c.record)
		}

		cancel()
	}

	assert.Equal(t, len(accumulator.index), 4)

	for i, offset := range accumulator.index {
		data := accumulator.buf[offset.start:offset.end]
		assert.Equal(t, data, cases[i].record)
	}
}
