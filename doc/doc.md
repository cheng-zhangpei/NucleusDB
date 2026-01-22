# NucleusDB: A Distributed Data Storage System for Multi-Agent Memory Management

## 1、Introduction

The purpose of building this system is to provide resilient memory and communication services for **large-scale AI systems**.At the outset of project design, to enable more flexible memory modifications, development began with a distributed database kernel. However, due to the kernel's complexity and insufficient stability—which led to issues like phantom reads—it will be replaced with an industry-standard stable kernel engine (Individual developers who are not database kernel development specialists simply lack the resources to undertake kernel-level work QAQ) . The prototype currently uses TinyKV but will transition to TiKV in subsequent phases. Future documentation will ignore the details regarding this project's database component, and the project will undergo refactoring to adapt to cloud-native environments.you can see [NucleusMem](https://github.com/cheng-zhangpei/NucleusMem).You can check out this repository to follow future developments.

## 1.1 Challenges

### 1.1.1 Topological Rigidity and Lack of Flexibility

Existing mainstream frameworks (such as LangGraph, AutoGen, and CrewAI) are fundamentally focused on application-level orchestration. Their agent interaction topologies are typically defined statically at startup. In long-term operational scenarios, systems lack the ability to dynamically adjust at runtime. For instance, when sudden spikes in system load require automatic scaling of Agent instances, or when a specific expert Agent needs to be hot-plugged in, existing frameworks often necessitate restarting the entire process or hard-coding all possibilities in advance. This approach fails to adapt to the dynamic scaling demands of cloud environments.

### 1.1.2 State Coupling and “Statelessness”

The core principle of cloud-native architecture is the “statelessness” of compute nodes, yet the essence of agents is “strongly stateful” (dependent on context and memory).

- **Pain point**: Most frameworks tightly bind memory lifecycles to agent processes (in-process memory).
  - Disaster Recovery Challenges: Unpersisted memory is immediately lost upon agent container failure or rescheduling.
  - Data Silos: Agent B cannot access Agent A's experience in real-time, requiring explicit message passing that hinders collaborative efficiency.
  - Inheritance Gap: Newly launched agents cannot directly mount historical “experience slices” accumulated by previous agents, preventing knowledge accumulation within the system.

### 1.1.3 The Vacuum of Collective Memory

Existing memory middleware (e.g., Mem0, Zep) is primarily designed to enhance the interaction experience between individual agents and users (User-Agent Interaction).

- **Pain Points**: They lack foundational primitives for agent swarms. They cannot handle high-concurrency read/write operations and consistency synchronization across multiple agents accessing the same memory space, making it difficult to support efficient swarm collaboration based on the “blackboard pattern” (where multiple agents share the same memory content).

## 1.2 Core Design Principles

### 1.2.1 Memory as Service

This system decouples agent memory from the reasoning process of large models, abstracting the agent's memory into a MemSpace. Large models are inherently stateless black boxes whose state depends entirely on external token inputs. MemSpace fully handles the storage of the agent's memory and state, enabling agents to freely attach and detach MemSpaces to inherit different states.

**Conceptually, a MemSpace is akin to a Persistent Volume (PV) in Kubernetes, while an Agent is a stateless Pod.** This separation brings 2 critical advantages to agent orchestration:

1. **Dynamic State Binding & Context Switching**

In traditional frameworks, an agent's memory is often bound to its process lifecycle. If the process dies, the memory is lost. In NucleusDB, an agent can dynamically **mount** a MemSpace at runtime.

- **Context Switching**: A single agent runtime can switch between different personas (e.g., "Developer" vs. "Auditor") simply by unmounting one MemSpace and mounting another.
- **State Inheritance**: When an agent scales up or migrates to a new node, it instantly inherits the complete historical context by attaching to the existing MemSpace, ensuring zero information loss.

2. **Hierarchical Memory Isolation (Private vs. Shared)**

MemSpace is not a flat storage bucket; it provides a structured isolation mechanism to ensure data privacy and collaboration efficiency.

- **Private MemSpace**: Dedicated to a single agent for storing internal thought chains (CoT), draft buffers, and sensitive credentials. It is strictly isolated from other agents to prevent privacy leakage.
- **Shared MemSpace**: Designed for collaboration. Multiple agents can mount the same Shared MemSpace simultaneously. It acts as a **"Blackboard"**, where Agent A writes a task, and Agent B reads and executes it. This shared state is synchronized across distributed nodes via the underlying consensus protocol.

In summary, **Memory as Service** transforms agent memory from an ephemeral, process-local variable into a **persistent, shareable, and programmable infrastructure resource**. This is the foundation for building resilient and scalable Multi-Agent Systems.

> If the memory of the llm can be a kind of cloud service?

### 1.2.2 Memory as Communication

Traditional multi-agent systems typically rely on explicit peer-to-peer (P2P) messaging, which results in complex and fragile communication links. Introducing a dedicated network layer component to manage agent states within such a system would lead to a highly fragmented architecture, significantly increasing operational complexity and difficulty.

Therefore, NucleusDB integrates the communication process directly into the **MemSpace** component, adopting a **"Store-and-Forward"** paradigm similar to a Blackboard architecture. Communication is no longer a transient network packet but a persistent state change. This approach naturally enables auditing, tracing, and time-travel debugging. To realize this vision, NucleusDB addresses three key challenges:

**1. Message Flow Process: The Watcher Mechanism**

Unlike traditional message queues that push data blindly, NucleusDB utilizes a storage-centric event loop.

- **Write-Driven Trigger**: When Agent A sends a message, it essentially performs a `Put` operation into a specific **Topic Area** (implemented via Key Prefixes) within the MemSpace.
- **Reactive Dispatch**: A lightweight component called **Watcher** monitors these key changes. Upon detecting a new entry, the Watcher retrieves the content and pushes the signal (e.g., the Message Key) to the subscribers' in-memory channels.
- **Pull-Based Retrieval**: The receiving Agent B, upon getting the notification, pulls the full message content from the storage layer. This separation of "Signal Notification" and "Data Retrieval" reduces memory pressure on the routing layer and ensures data consistency.

**2. Routing Determined by the Agent**

Routing decisions are offloaded from the infrastructure to the agents themselves, allowing for flexible topology definitions.

- **Topic as Channel**: Agents communicate by subscribing to specific **Topics** (e.g., "general", "task-1"). A Topic acts as a logical channel mapped to a specific key range in the underlying KV store.
- **Directed Addressing**: For P2P communication, an agent specifies a `TargetID`. The Watcher uses an in-memory routing table to map the ID to the specific channel of the destination agent instance.
- **Semantic Routing (Future Work)**: The architecture reserves the potential for LLM-based routing, where a "Router Agent" analyzes the semantic intent of a message and dynamically determines the most appropriate recipient Topic.

> I know this design wouldn't fly in industrial systems, and such uncertainty is hard to avoid in AI systems. But as a personal project, why not let it be a bit radical?

**3. Communication Traceability and Audit**

Since every communication action is fundamentally a storage transaction, NucleusDB provides native auditability without additional overhead.

- **Persistence**: All chat histories are persisted as KV pairs (e.g., `Topic/Timestamp/SenderID`). This allows the system to reconstruct the entire conversation flow even after a cluster restart.
- **Time-Travel Debugging**: Utilizing the underlying MVCC (Multi-Version Concurrency Control) mechanism, developers can query the state of the communication channel at any specific timestamp `T`. This is critical for diagnosing "hallucinations" or logical errors in multi-agent collaborations, as it allows replaying the exact inputs an agent received at a past moment.

## 2. Architecture Design

> I don't want to write about the rather “ugly” version of NucleusDB in this repository. I'll rewrite the technical documentation after I finish refactoring NucleusMem.

## 3.Future

