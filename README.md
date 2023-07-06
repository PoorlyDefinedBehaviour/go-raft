## Objectives

- Deterministic simulation insipired by [Tigerbeetle](https://github.com/tigerbeetledb/tigerbeetle)
- Raft implementation inspired by eatonphil's [goraft](https://github.com/eatonphil/goraft)
- Distributed kv store
- No request/response serialization 

## Running the simulator

```
go test -run ^TestSimulate$ github.com/poorlydefinedbehaviour/raft-go/src/simulator -v -race
```

## References

[In Search of an Understandable Consensus Algorithm (Extended Version)](https://raft.github.io/raft.pdf)