# ComLog Library

## Quick start

#### Append a record

- Example:

```golang
package main

import (
	"fmt"

	"github.com/ayoyu/comLog/comLog"
)

const log_dir = "/name_of_directory/"
const StoreMaxBytes uint64 = 4096
const IndexMaxBytes uint64 = 4096

func main() {
	conf := comLog.Config{Data_dir: log_dir, StoreMaxBytes: StoreMaxBytes, IndexMaxBytes: IndexMaxBytes}
	log, err := comLog.NewLog(conf)
	if err != nil {
		panic("Error during log setup")
	}
	offset, nn, err := log.Append([]byte("Hello World"))
	if err != nil {
		fmt.Errorf("Append Error %w", err)
	} else {
		fmt.Println("Nbr of bytes written: ", nn, "|| offset: ", offset)
	}
}
```

#### Read a record:

- Example 1: (offset from above example)

```golang
// the offset from the append example
nn, record, err := log.Read(int64(offset))
if err != nil {
	fmt.Errorf("Read Error %w", err)
} else {
	fmt.Println("Nbr of bytes readed: ", nn, "|| record: ", record)
}
```

- Example 2: (Read the last written record)

```golang
nn, record, err := log.Read(-1)
if err != nil {
	fmt.Errorf("Read Error %w", err)
} else {
	fmt.Println("Nbr of bytes readed: ", nn, "|| Last record: ", record)
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
