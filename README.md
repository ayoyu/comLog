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
$ cd benchmarks/ && go test -benchmem -run=^$ -bench .
goos: linux
goarch: amd64
pkg: github.com/ayoyu/comLog/benchmarks
cpu: Intel(R) Core(TM) i5-9300H CPU @ 2.40GHz
BenchmarkLog_ReadOnly_After_Bulk_Writes-8              	 3936056	       300.5 ns/op	      16 B/op	       1 allocs/op
BenchmarkLog_Write_WorkLoad_For_Record_Length_10-8     	 1000000	      1632 ns/op	      26 B/op	       1 allocs/op
BenchmarkLog_Write_WorkLoad_For_Record_Length_50-8     	  345273	      3197 ns/op	      67 B/op	       1 allocs/op
BenchmarkLog_Write_WorkLoad_For_Record_Length_200-8    	  131122	      9027 ns/op	     219 B/op	       1 allocs/op
BenchmarkLog_Write_Read_WorkLoad_Record_Length_10-8    	   72444	     15474 ns/op	      41 B/op	       2 allocs/op
BenchmarkLog_Write_Read_WorkLoad_Record_Length_50-8    	   65324	     19598 ns/op	     130 B/op	       2 allocs/op
BenchmarkLog_Write_Read_WorkLoad_Record_Length_200-8   	   47949	     23957 ns/op	     427 B/op	       2 allocs/op
PASS
ok  	github.com/ayoyu/comLog/benchmarks	10.893s
```
