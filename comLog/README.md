# ComLog Library

## Quick start

#### Append a record

- Append the record `[]byte("Hello World")` and receive the `offset: uint64` of the record, or also what is called the log sequence number (LSN) https://pgpedia.info/l/LSN-log-sequence-number.html in the WAL databases terminology https://pgpedia.info/w/wal-write-ahead-logging.html

```golang
package main

import (
	"fmt"

	"github.com/ayoyu/comLog/comLog"
)

const log_dir string = "/The_name_of_data_log_directory/"
const StoreMaxBytes_ uint64 = 4096
const IndexMaxBytes_ uint64 = 4096

func main() {
	var conf comLog.Config = comLog.Config{Data_dir: log_dir, StoreMaxBytes: StoreMaxBytes_, IndexMaxBytes: IndexMaxBytes_}
	var (
		err error
		log *comLog.Log
	)
	log, err = comLog.NewLog(conf)
	if err != nil {
		panic("Error during log setup")
	}
	var (
		offset uint64
		nn     int
	)
	offset, nn, err = log.Append([]byte("Hello World"))
	if err != nil {
		fmt.Println("Append Error %w", err)
	} else {
		fmt.Println("Nbr of bytes written: ", nn, "|| offset: ", offset)
	}
	offset, nn, err = log.Append(
		[]byte(`{
			"name": "value",
			"type": "record",
			"fields": [{"name": "user_id","type": "int"},{"name": "gender","type": "string"}]
		}`))
}
```

#### Explicit Flush/Commit

- Flush the log buffers (index and store) of the active segment. The index mmap region will be synchronized asynchronously with the underlying file.

```golang
var err error = log.Flush()
if err != nil {
	fmt.Println("Error Flushing the log", err)
}
```

#### Read a record:

- Example 1: (the offset from above example)

Read back the bytes record given its offset.

```golang
// the offset from the append example
var (
	record []byte
	err error
	nn int
)
nn, record, err = log.Read(int64(offset))
if err != nil {
	fmt.Println("Read Error", err)
} else {
	fmt.Println("Nbr of bytes readed: ", nn, "|| record: ", record)
}
```

- Example 2:

Read the last record written at a certain time.

```golang
var (
	record []byte
	err error
	nn int
)
nn, record, err = log.Read(-1)
if err != nil {
	fmt.Errorf("Read Error %w", err)
} else {
	fmt.Println("Nbr of bytes readed: ", nn, "|| Last record: ", record)
}
```

Note: The read operation will make an implicit flush operation of the records buffer, before reading.

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
