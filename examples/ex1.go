package main

/*
Example to use the comLog package and to test also thread(goroutine) safety of the database from data race
*/
import (
	"fmt"
	"math/rand"
	"os"
	"sync"

	"github.com/ayoyu/comLog/comLog"
)

const StoreMaxBytes uint64 = 4096
const IndexMaxBytes uint64 = 4096

type TestLogData struct {
	record string
	offset uint64
	err    error
}
type TestLogData_2 struct {
	original    string
	record_back string
	offset      uint64
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomRecord(n int) []byte {
	record := make([]byte, n)
	for i := 0; i < n; i++ {
		record[i] = letters[rand.Int63()%int64(len(letters))]
	}
	return record
}

func LogMixAppendAndRead() error {
	// The read will be at the end
	log_dir, err := os.MkdirTemp("", "test_bench")
	fmt.Println("Log_dir: ", log_dir)
	if err != nil {
		return err
	}

	conf := comLog.Config{Data_dir: log_dir, StoreMaxBytes: StoreMaxBytes, IndexMaxBytes: IndexMaxBytes}
	log, err := comLog.NewLog(conf)
	if err != nil {
		return err
	}
	Size := 5000
	var res chan TestLogData = make(chan TestLogData, Size)
	var wait sync.WaitGroup
	for i := 0; i < Size; i++ {
		wait.Add(1)
		go func() {
			// Append
			record := randomRecord(10)
			offset, _, err := log.Append(record)
			if err != nil {
				fmt.Println("???????????????????????? Append Error: ", err)
				res <- TestLogData{record: string(record), offset: 0, err: err}
			} else {
				res <- TestLogData{record: string(record), offset: offset, err: nil}
			}
			wait.Done()

		}()
		wait.Add(1)
		go func() {
			defer wait.Done()
			AppendTestLogData := <-res
			if AppendTestLogData.err != nil {
				// this is an OK case
				fmt.Println("OK CASE ???????????????????????? Cannot Read because the append returned an error")
				return
			}
			_, record, err := log.Read(int64(AppendTestLogData.offset))
			if err != nil {
				// not an OK case -> stop to check
				fmt.Println("???????????????????????? Read Error: ", err)
				return
			}
			if string(record) != AppendTestLogData.record {
				// corrupted data case coming from data race
				fmt.Println("<!><!><!><!><!><!><!><!><!><!><!> Appended-record: ", AppendTestLogData.record, " != Read-record", string(record))
				return
			}
			fmt.Println("Succeed: Offset=", AppendTestLogData.offset, "Append-record: ", AppendTestLogData.record, " == Read-record", string(record))

		}()
	}
	wait.Wait()
	os.RemoveAll(log_dir)
	return nil
}

func LogAppend() error {
	// The read will be at the end
	log_dir, err := os.MkdirTemp("", "test_bench")
	fmt.Println("Log_dir: ", log_dir)
	if err != nil {
		return err
	}
	conf := comLog.Config{Data_dir: log_dir, StoreMaxBytes: StoreMaxBytes, IndexMaxBytes: IndexMaxBytes}
	log, err := comLog.NewLog(conf)
	if err != nil {
		return err
	}
	Size := 2000
	var res chan TestLogData = make(chan TestLogData, Size)
	var wait sync.WaitGroup
	for i := 0; i < Size; i++ {
		wait.Add(1)
		go func() {
			// Append
			record := randomRecord(10)
			offset, _, err := log.Append(record)
			if err != nil {
				fmt.Println("???????????????????????? Append Error: ", err)
				res <- TestLogData{record: string(record), offset: 0, err: err}
			} else {
				res <- TestLogData{record: string(record), offset: offset, err: nil}
			}
			wait.Done()

		}()
	}
	wait.Wait()
	// Read back appended records
	fmt.Println()
	fmt.Println("Results: ")
	var arr []TestLogData_2
	for i := 0; i < Size; i++ {
		t := <-res
		if t.err != nil {
			fmt.Println("????????????????????????? Error occur during Append", t.err, "...continue")
			continue
		}
		_, rr, err := log.Read(int64(t.offset))
		if err != nil {
			fmt.Println("???????????????????????? Error Reading: ", err)
			return err
		}
		if string(rr) != t.record {
			// the most critical error
			fmt.Println("<!><!><!><!><!><!><!><!><!><!><!> Found Incorrect data")
			fmt.Println("Read offset: ", t.offset, "record readed back: ", string(rr), "!= stored record (Append): ", t.record)
			return nil
		}
		fmt.Println("Read offset: ", t.offset, "record readed back: ", string(rr), "== stored record (Append): ", t.record)
		arr = append(arr, TestLogData_2{original: string(t.record), record_back: string(rr), offset: t.offset})
	}
	err = log.Close()
	if err != nil {
		fmt.Println("???????????????????????? Error Close: ", err)
		return err
	}
	// Second Setup: setup Again from log_dir
	conf2 := comLog.Config{Data_dir: log_dir, StoreMaxBytes: StoreMaxBytes, IndexMaxBytes: IndexMaxBytes}
	log2, err := comLog.NewLog(conf2)
	if err != nil {
		return err
	}
	for _, second := range arr {
		_, r2, err := log2.Read(int64(second.offset))
		if err != nil {
			fmt.Println("???????????????????????? (Second Time) Error Reading : ", err)
			return err
		}
		if string(r2) != second.original || string(r2) != second.record_back {
			fmt.Println("<!><!><!><!><!><!><!><!><!><!><!>  (Second Time) Found Incorrect data")
			fmt.Println("Read 2 offset: ", second.offset, "record_back_2: ", string(r2), "record_back: ", second.record_back, "original: ", second.original)
			return nil
		}
		fmt.Println("(Second Time) offset: ", second.offset, "record_back2: ", string(r2), "record_back: ", second.record_back, "original: ", second.original)
	}
	os.RemoveAll(log_dir)
	return nil
}

func main() {
	LogAppend()
	//LogMixAppendAndRead()
}