package node

// 一个线程不安全的 Raft 节点，node封装了一个底层的 *raft 实例，提供了一系列的方法来操作 Raft 状态机。raft node is the most external layer of the distributed system
