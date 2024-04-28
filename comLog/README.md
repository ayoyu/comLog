# ComLog Library

## Quick start

#### Append Records

- Append the record and receive its `offset: uint64`, or also what is called the log sequence number (LSN) https://pgpedia.info/l/LSN-log-sequence-number.html in the WAL databases terminology https://pgpedia.info/w/wal-write-ahead-logging.html

```golang
package main

import (
	"fmt"

	"github.com/ayoyu/comLog/comLog"
)

func main() {
	conf := comLog.Config{
		Data_dir:      "/data/directory/",
		StoreMaxBytes: 4096,
		IndexMaxBytes: 4096,
	}

	log, err := comLog.NewLog(conf)
	if err != nil {
		panic(err.Error())
	}

	// Closing the log
	defer func() {
		if err := log.Close(); err != nil {
			panic(err.Error())
		}
	}()

	offset, nn, err := log.Append(
		[]byte(`{
			"name": "value",
			"type": "record",
			"fields": [{"name": "user_id","type": "int"},{"name": "gender","type": "string"}]
		}`))
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("Offset: %v, Nbr of written bytes: %v\n", offset, nn)
}
```

#### Explicit Flush/Commit

- Flush explicitly the log buffers (index and store) of the active segment. The `idxSyncType` parameter specifies wheter flushing should be done synchronously or asynchronously regarding the index mmap linked to the active segment.

```golang
if err := log.Flush(comLog.INDEX_MMAP_ASYNC); err != nil {
	panic(err.Error())
}
```

```golang
if err := log.Flush(comLog.INDEX_MMAP_SYNC); err != nil {
	panic(err.Error())
}
```

#### Read Records:

- Read a record with the given `offset: int64`

```golang
var (
	nn int
	record []byte
	err error
)
// Read with the offset from the append example
nn, record, err = log.Read(int64(offset))
if err != nil {
	panic(err.Error())
}

fmt.Printf("Record: %v\n, Nbr of read bytes: %v\n", string(record), nn)
```

- Polling the last written record at a certain time with `offset=-1`

```golang
var (
	nn int
	record []byte
	err error
)
nn, record, err = log.Read(-1)
if err != nil {
	panic(err.Error())
}

fmt.Printf("Record: %v\n, Nbr of read bytes: %v\n", string(record), nn)
```

Note: The read operation will make an implicit flush of the records store buffer, before reading.

#### Useful information about the status of the commit log:

- The current number of segments aka history + active segment, The oldest offset and the last tracked offset

```golang
fmt.Printf("The current nbr of segments: %d\n", log.SegmentsSize())
fmt.Printf("The oldest offset: %d\n", log.OldestOffset())
fmt.Printf("The last (newest) offset: %d\n", log.NewsetOffset())
```

#### Truncate commit log:

- To collect old segments based on some retention policy,...etc

```golang
if err := log.CollectSegments(47); err != nil {
	panic(err.Error())
}
fmt.Printf("After segments collection, the current nbr of segments : %d\n", log.SegmentsSize())
fmt.Printf("After segments collection, the oldest offset: %d\n", log.OldestOffset())
fmt.Printf("After segments collection, the last (newest) offset: %d\n", log.LastOffset())
```

NOTE: If the collect operation **deleted all segments**, the log will still be ready to receive read and write operations because the log will automatically create the new segment (i.e. the first segment).

- Remove completly the commit log

NOTE: This operation is different from the `CollectSegments` as it will **remove definitely** the log data directory, which means if another operation comes after you will receive a `no such file or directory` error.

```golang
if err := log.Remove(); err != nil {
	panic(err.Error())
}
```

#### Close the commit log:

It is always a good practice to close your resources before finishing, in this case it is very important to close the commit log to ensure the correctness and consistency of the state of the log for the next configuration/setup.

```golang
if err := log.Close(); err != nil {
	panic(err.Error())
}
```

### File format

An example of the log data directory that contains the DATA files in a binary file format:

- `.store` contains the stored records bytes
- `.index` maintains the index to lookup for records in the store file

```bash
$ ls
0.index  1140.index  1368.index  1596.index  1824.index  228.index  456.index  684.index  912.index
0.store  1140.store  1368.store  1596.store  1824.store  228.store  456.store  684.store  912.store
```

```
$ file 0.store
0.store: data

$ file 0.index
0.index: data
```

## Benchmarks

```Shell
$ make run_benchmarks
go test -benchmem -bench=. ./benchmarks
goos: linux
goarch: amd64
pkg: github.com/ayoyu/comLog/benchmarks
cpu: Intel(R) Core(TM) i5-9300H CPU @ 2.40GHz
BenchmarkLog_ReadOnly_After_Bulk_Writes-8              	 2858198	       394.9 ns/op	      16 B/op	       1 allocs/op
BenchmarkLog_Write_WorkLoad_For_Record_Length_10-8     	 1000000	      1223 ns/op	      26 B/op	       1 allocs/op
BenchmarkLog_Write_WorkLoad_For_Record_Length_50-8     	  603902	      2108 ns/op	      67 B/op	       1 allocs/op
BenchmarkLog_Write_WorkLoad_For_Record_Length_200-8    	  240582	      5116 ns/op	     220 B/op	       1 allocs/op
BenchmarkLog_Write_Read_WorkLoad_Record_Length_10-8    	   57232	     19382 ns/op	      41 B/op	       2 allocs/op
BenchmarkLog_Write_Read_WorkLoad_Record_Length_50-8    	   61903	     18209 ns/op	     130 B/op	       2 allocs/op
BenchmarkLog_Write_Read_WorkLoad_Record_Length_200-8   	   56301	     21540 ns/op	     428 B/op	       2 allocs/op
PASS
ok  	github.com/ayoyu/comLog/benchmarks	9.613s

```
