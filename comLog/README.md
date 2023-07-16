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

const (
	log_dir       string = "/tmp_dir" // don't forget to mkdir the directory
	StoreMaxBytes uint64 = 4096
	IndexMaxBytes uint64 = 4096
)

func main() {
	var conf comLog.Config = comLog.Config{
		Data_dir:      log_dir,
		StoreMaxBytes: StoreMaxBytes,
		IndexMaxBytes: IndexMaxBytes,
	}
	var (
		err error
		log *comLog.Log
	)
	log, err = comLog.NewLog(conf)
	if err != nil {
		panic(err.Error())
	}
	// Closing the log
	defer func() {
		if err := log.Close(); err != nil {
			panic(err.Error())
		}
	}()

	var (
		offset uint64
		nn     int
	)
	offset, nn, err = log.Append(
		[]byte(`{
			"name": "value",
			"type": "record",
			"fields": [{"name": "user_id","type": "int"},{"name": "gender","type": "string"}]
		}`))

	if err != nil {
		fmt.Printf("Append Error %v\n", err)
	} else {
		fmt.Printf("Offset: %v, Nbr of written bytes: %v\n", offset, nn)
	}
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
	fmt.Printf("Read Error %v\n", err)
} else {
	fmt.Printf("Record: %v\n, Nbr of read bytes: %v\n", string(record), nn)
}
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
	fmt.Printf("Polling Error %v\n", err)
} else {
	fmt.Printf("Record: %v\n, Nbr of read bytes: %v\n", string(record), nn)
}
```

Note: The read operation will make an implicit flush of the records store buffer, before reading.

#### Useful information about the status of the commit log:

- The current number of segments aka history + active segment, The oldest offset and the last tracked offset

```golang
fmt.Printf("The current nbr of segments: %d\n", log.SegmentsSize())
fmt.Printf("The oldest offset: %d\n", log.OldestOffset())
fmt.Printf("The last (newest) offset: %d\n", log.LastOffset())
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
