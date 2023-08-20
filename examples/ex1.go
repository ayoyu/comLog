package main

/*
Example to use the comLog package and to test also thread(goroutine) safety of the database from data race.

This example is just a draft of experiments so don't consider it as a tutorial example,
it will get removed later and more useful examples will get added.
*/
import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

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

func LogMixAppendReadWorkLoad(verbose bool) error {
	//  Append and Read are Concurrnents (Mix mode)
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
				if verbose {
					fmt.Println("???????????????????????? Append Error: ", err)
				}
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
				if verbose {
					fmt.Println("OK CASE ???????????????????????? Cannot Read because the append returned an error")
				}
				return
			}
			_, record, err := log.Read(int64(AppendTestLogData.offset))
			if err != nil {
				// not an OK case -> stop to check
				if verbose {
					fmt.Println("???????????????????????? Read Error: ", err)
				}
				return
			}
			if string(record) != AppendTestLogData.record {
				// corrupted data case coming from data race
				if verbose {
					fmt.Println("########## Corrupted Data Error ############## Appended-record: ", AppendTestLogData.record, " != Read-record", string(record))
				}
				return
			}
			if verbose {
				fmt.Println("Succeed: Offset=", AppendTestLogData.offset, "Append-record: ", AppendTestLogData.record, " == Read-record", string(record))
			}

		}()
	}
	wait.Wait()
	os.RemoveAll(log_dir)
	return nil
}

func MixWorkLoad(verbose bool) error {
	// Append and Flush are Concurrnents
	// The read will be at the end in sync mode to check results
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
		if i%100 == 0 {
			// make periodic explicit flush of the log
			wait.Add(1)
			go func() {
				err := log.Flush(comLog.INDEX_MMAP_SYNC)
				if err != nil && verbose {
					fmt.Println("???????????????????????? Log Flush/Commit error", err)
				}
				wait.Done()
			}()

			wait.Add(1)
			go func(i int) {
				defer wait.Done()
				select {
				case <-time.After(time.Millisecond * 100):
					// this operation of printing the 2 metrics is not atomic i.e. log.LastOffset() can return a response
					// but it doesn't mean that it is synchronize with what the log.SegmentsSize() get us as response
					// the Lock that was holded during log.SegmentsSize() can be given to other operations before the
					// log.LastOffset() get it and return a response.
					fmt.Printf("[Monitoring %d] Nbr of segments: %d, Last Offset %d, OldestOffset: %d\n",
						i, log.SegmentsSize(), log.LastOffset(), log.OldestOffset())
				}

			}(i)
		}

		wait.Add(1)
		go func() {
			// Append
			record := randomRecord(10)
			offset, _, err := log.Append(record)
			if err != nil {
				if verbose {
					fmt.Println("???????????????????????? Append Error: ", err)
				}
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
	if verbose {
		fmt.Printf("[Results] The final number of segemnts: %d \n", log.SegmentsSize())
	}
	var arr []TestLogData_2
	for i := 0; i < Size; i++ {
		t := <-res
		if t.err != nil {
			if verbose {
				fmt.Println("????????????????????????? Error occur during Append", t.err)
			}
			return t.err
		}
		_, rr, err := log.Read(int64(t.offset))
		if err != nil {
			if verbose {
				fmt.Println("???????????????????????? Error Reading: ", err)
			}
			return err
		}
		if string(rr) != t.record {
			// the most critical error
			if verbose {
				fmt.Println("<!><!><!><!><!><!><!><!><!><!><!> Found Incorrect data")
				fmt.Println("Read offset: ", t.offset, "record readed back: ", string(rr), "!= stored record (Append): ", t.record)
			}
			return fmt.Errorf("(Second Time) Found Incorrect data")
		}
		if verbose {
			fmt.Println("Read offset: ", t.offset, "record readed back: ", string(rr), "== stored record (Append): ", t.record)
		}
		arr = append(arr, TestLogData_2{original: string(t.record), record_back: string(rr), offset: t.offset})
	}
	err = log.Close()
	if err != nil {
		if verbose {
			fmt.Println("???????????????????????? Error Close: ", err)
		}
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
			if verbose {
				fmt.Println("???????????????????????? (Second Time) Error Reading : ", err)
			}
			return err
		}
		if string(r2) != second.original || string(r2) != second.record_back {
			if verbose {
				fmt.Println("<!><!><!><!><!><!><!><!><!><!><!>  (Second Time) Found Incorrect data")
				fmt.Println("Read 2 offset: ", second.offset, "record_back_2: ", string(r2), "record_back: ", second.record_back, "original: ", second.original)
			}
			return fmt.Errorf("(Second Time) Found Incorrect data")
		}
		if verbose {
			fmt.Println("(Second Time) offset: ", second.offset, "record_back2: ", string(r2), "record_back: ", second.record_back, "original: ", second.original)
		}
	}
	os.RemoveAll(log_dir)
	return nil
}

func main() {
	err := MixWorkLoad(true)
	if err != nil {
		panic(err.Error())
	}
	// LogMixAppendReadWorkLoad(true)
}
