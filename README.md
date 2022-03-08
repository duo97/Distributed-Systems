# Distributed-Systems
Coursework for 6.824.
- [x] **Lab 1: MapReduce**

    Built a MapReduce system. Implemented a worker process that calls application Map and Reduce functions and handles reading and writing files, and a coordinator process that hands out tasks to workers and copes with failed workers.
- [ ] **Lab 2: Raft**

  Implementing Raft, a replicated state machine protocol.
  
  - [x] Election Process
  - [x] Append Entries & Commit
  - [x] Persistent State
  - [ ] Log Compaction
  
# Todos

  - [ ] Seperate the process of matching index and sending entries to reduce bytes sent through RPC calls
