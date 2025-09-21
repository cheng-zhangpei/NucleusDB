# NucleusDB: A Distributed Key-Value Database Engine for Large Model Memory Storage

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

NucleusDB is a distributed key-value database engine designed specifically to address the memory storage and management challenges of Large Language Models (LLMs) in multi-agent collaborative systems. Built from the ground up, it combines the high-performance Bitcask data model with the robust Raft consensus algorithm to provide a reliable, efficient, and scalable solution.

This project originated as an undergraduate thesis and represents a complete end-to-end implementation, progressing from a single-node database to a fully-fledged distributed system with advanced features like distributed transactions.

## 🌟 Features

*   **Optimized for LLMs:** Provides dedicated memory space management at the database level, including functions for memory search, matching, and AI-driven compression to enhance the efficiency and stability of LLMs.
*   **High-Performance Core:** Based on the **Bitcask** log-structured storage model, ensuring low-latency reads and high-throughput writes by leveraging sequential I/O and an in-memory index (`keydir`).
*   **Distributed & Consistent:** Implements the **Raft consensus algorithm** to guarantee strong data consistency across multiple nodes, supporting leader election, log replication, and conflict resolution.
*   **Advanced Transaction Support:** Offers three transaction mechanisms:
    *   `WriteBatch`: Serializes operations for strict consistency.
    *   `MVCC`: Enables high-concurrency read-write operations using Multi-Version Concurrency Control.
    *   `Distributed MVCC`: A custom implementation that extends MVCC to the distributed environment.
*   **Innovative Hybrid Timestamp Architecture:** Proposes a novel hybrid timing scheme that combines a centralized lease-based service (using ZooKeeper) with local physical clocks. This design aims to achieve high transaction throughput while ensuring isolation and preventing clock drift issues, inspired by concepts from Google Spanner and Percolator.
*   **Efficient Data Management:** Features a `merge` mechanism to reclaim space from deleted records and uses memory-mapped files to accelerate database recovery on startup.

## 📚 Research Context

​	ComDB was developed to solve the critical bottleneck of managing vast amounts of long-term memory data for AI agents. Instead of treating the database as a simple external store, this project integrates memory management logic directly into the database layer, providing a seamless interface for LLM applications.

​	The work serves as the foundational memory backend for a larger vision: **KubeNucleus**, a proposed intelligent data center management system where AI oversees cloud-native infrastructure.

​	I hope the project can not only used to convenient the inference of LLM, but also convenient the training of LLM. The final goal of this project is to let DB can understand the meaning of the data it store.

## 🔧 Getting Started

### Prerequisites

*   Go 1.22.0
*   toolchain go1.22.10
*   Protocol Buffers Compiler (`protoc`)
*   gRPC-Go plugin
*   ZooKeeper (for the distributed timestamp service,if just use single node mode, can ignore)

### Installation

[installation documentation](https://github.com/cheng-zhangpei/NucleusDB/tree/main/doc/installation.md)

## 📊 Performance

​	The current system is a personal research project developed within limited time and resources. As a result, the overall performance is modest and has not been rigorously optimized. Many potential benchmarks—especially under high concurrency or large-scale data scenarios—have not been conducted due to hardware and time constraints. The implementation prioritizes functional correctness and architectural exploration over raw speed. While core features such as distributed consensus and transaction management are functional, further optimization and stress testing are needed to evaluate its scalability and efficiency in real-world environments.	

## Issue

​	you can see Issue module of the repository or [Issue](https://github.com/cheng-zhangpei/NucleusDB/tree/main/doc/issues.md)
