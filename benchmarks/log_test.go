package logbench

import (
	"math/rand"
	"os"
	"testing"

	"github.com/ayoyu/comLog/comLog"
)

const StoreMaxBytes uint64 = 65536
const IndexMaxBytes uint64 = 65536
const letters string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

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
