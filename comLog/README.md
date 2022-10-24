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
