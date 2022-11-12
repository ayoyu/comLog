# ComLog

## The goal of the project

- Implement a distributed commit log service with Golang
- Learn a lot about database concurrency, commit log, distributed systems...etc And most important one having fun 😁️

What is a commitLog ? some resources to check out if you are curious:

- https://cassandra.apache.org/doc/latest/cassandra/architecture/storage_engine.html#:~:text=Commitlogs%20are%20an%20append%20only,will%20be%20applied%20to%20memtables.
- https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
- https://dev.to/heroku/what-is-a-commit-log-and-why-should-you-care-pib

## Quick start

#### Library Usage: [goto](comLog/README.md)

## Tasks:

- [x] Write the commit log library (package)
- [ ] Write the web service to expose the library
- [ ] Write the service discovery logic for distribution purpose
- [ ] Write the service coordination/consensus logic

## Benchmarks

```Shell
$ go test -benchmem -run=^$ -bench .
goos: linux
goarch: amd64
pkg: github.com/ayoyu/comLog/benchmarks
cpu: Intel(R) Core(TM) i5-9300H CPU @ 2.40GHz
BenchmarkLog_Write_WorkLoad_For_Record_Length_10-8     	 1344366	       882.9 ns/op	      26 B/op	       1 allocs/op
BenchmarkLog_Write_WorkLoad_For_Record_Length_50-8     	  976520	      1165 ns/op	      67 B/op	       1 allocs/op
BenchmarkLog_Write_WorkLoad_For_Record_Length_200-8    	  607878	      2149 ns/op	     219 B/op	       1 allocs/op
BenchmarkLog_Write_Read_WorkLoad_Record_Length_10-8    	   93140	     12611 ns/op	      42 B/op	       2 allocs/op
BenchmarkLog_Write_Read_WorkLoad_Record_Length_50-8    	   89199	     12459 ns/op	     130 B/op	       2 allocs/op
BenchmarkLog_Write_Read_WorkLoad_Record_Length_200-8   	   84942	     14321 ns/op	     427 B/op	       2 allocs/op
PASS
ok  	github.com/ayoyu/comLog/benchmarks	10.572s
```
