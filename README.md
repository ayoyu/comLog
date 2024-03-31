# ComLog

## About

### tl;dr

Commit log service in Golang similar to what Kafka is (The goal is to make it distributed in the future)

### The goal of the project

- Implement a distributed commit log service with Golang
- Learn a lot about database concurrency, commit log, distributed systems...etc And most important one having fun 😁️

What is a commitLog ? some resources to check out if you are curious:

- https://kafka.apache.org/documentation/#log
- https://cassandra.apache.org/doc/latest/cassandra/architecture/storage_engine.html#:~:text=Commitlogs%20are%20an%20append%20only,will%20be%20applied%20to%20memtables.
- https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
- https://dev.to/heroku/what-is-a-commit-log-and-why-should-you-care-pib

## Quick start

#### The Library Usage: [Library README](comLog/README.md)

## Tasks:

- [x] Write the commit log library (package)
- [x] Write the rpc service to expose the library
- [ ] Write the service discovery logic for the distribution purpose
- [ ] Write the service coordination/consensus logic
