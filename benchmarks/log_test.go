package benchmarks

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/ayoyu/comLog/comLog"
)

const StoreMaxBytes uint64 = 65536
const IndexMaxBytes uint64 = 65536
const letters string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const countItems int = 8000

type TestRecord struct {
	Content []byte
	Size    int
}

func randomRecord(n int) []byte {
	record := make([]byte, n)
	for i := 0; i < n; i++ {
		record[i] = letters[rand.Int63()%int64(len(letters))]
	}
	return record
}

func BenchmarkLog_ReadOnly_After_Bulk_Writes(b *testing.B) {
	log_dir, err := os.MkdirTemp("", "test_bench")
	if err != nil {
		b.Error(err)
	}
	var conf comLog.Config = comLog.Config{Data_dir: log_dir, StoreMaxBytes: StoreMaxBytes, IndexMaxBytes: IndexMaxBytes}
	log, err := comLog.NewLog(conf)
	if err != nil {
		b.Error(err)
	}
	var offsets []int64 = make([]int64, countItems)
	for i := 0; i < countItems; i++ {
		offset, _, err := log.Append(randomRecord(10))
		if err != nil {
			b.Error(err)
		}
		offsets[i] = int64(offset)
	}
	rand.Seed(time.Now().UnixNano())
	// to read for a random segment (i.e not necessarily from increasing offsets)
	rand.Shuffle(countItems, func(i, j int) {
		offsets[i], offsets[j] = offsets[j], offsets[i]
	})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int = 0
		for pb.Next() {
			_, _, err := log.Read(offsets[i%countItems])
			if err != nil {
				b.Error(err)
			}
			i++
		}
	})
	if err := log.Close(); err != nil {
		b.Error(err)
	}
	os.RemoveAll(log_dir)
}

func Log_Write_WorkLoad_Record_Length(b *testing.B, length int) {
	log_dir, err := os.MkdirTemp("", "test_bench")
	if err != nil {
		b.Error(err)
	}
	var conf comLog.Config = comLog.Config{Data_dir: log_dir, StoreMaxBytes: StoreMaxBytes, IndexMaxBytes: IndexMaxBytes}
	log, err := comLog.NewLog(conf)
	if err != nil {
		b.Error(err)
	}
	var record []byte = randomRecord(length)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := log.Append(record)
			if err != nil {
				b.Error(err)
			}
		}
	})
	if err := log.Close(); err != nil {
		b.Error(err)
	}
	os.RemoveAll(log_dir)
}

func Log_Write_Read_WorkLoad_Record_Length(b *testing.B, length int) {
	log_dir, err := os.MkdirTemp("", "test_bench")
	if err != nil {
		b.Error(err)
	}
	var conf comLog.Config = comLog.Config{Data_dir: log_dir, StoreMaxBytes: StoreMaxBytes, IndexMaxBytes: IndexMaxBytes}
	log, err := comLog.NewLog(conf)
	if err != nil {
		b.Error(err)
	}
	var record []byte = randomRecord(length)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			offset, _, err := log.Append(record)
			if err != nil {
				b.Error(err)
			}
			_, _, err = log.Read(int64(offset))
			if err != nil {
				b.Error(err)
			}
		}
	})
	if err := log.Close(); err != nil {
		b.Error(err)
	}
	os.RemoveAll(log_dir)
}

func BenchmarkLog_Write_WorkLoad_For_Record_Length_10(b *testing.B) {
	var length int = 10
	Log_Write_WorkLoad_Record_Length(b, length)
}

func BenchmarkLog_Write_WorkLoad_For_Record_Length_50(b *testing.B) {
	var length int = 50
	Log_Write_WorkLoad_Record_Length(b, length)
}

func BenchmarkLog_Write_WorkLoad_For_Record_Length_200(b *testing.B) {
	var length int = 200
	Log_Write_WorkLoad_Record_Length(b, length)
}

func BenchmarkLog_Write_Read_WorkLoad_Record_Length_10(b *testing.B) {
	var length int = 10
	Log_Write_Read_WorkLoad_Record_Length(b, length)
}

func BenchmarkLog_Write_Read_WorkLoad_Record_Length_50(b *testing.B) {
	var length int = 50
	Log_Write_Read_WorkLoad_Record_Length(b, length)
}

func BenchmarkLog_Write_Read_WorkLoad_Record_Length_200(b *testing.B) {
	var length int = 200
	Log_Write_Read_WorkLoad_Record_Length(b, length)
}
