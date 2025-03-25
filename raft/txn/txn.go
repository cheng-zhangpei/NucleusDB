package txn

/*
时间戳分段管理**
1 ZK 存储时间戳段：

--> ZK 维护一个全局的时间戳段（例如每 5 分钟生成一个段），每个段包含一个起始时间戳和范围。
Leader 节点从 ZK 获取当前活跃的时间戳段，并在段内分配事务的时间戳（如 [start, start + segment_size)）。
时间戳段的更新周期（例如 5 分钟）由 ZK 控制。
Leader 维护段内时间戳：

--> Leader 在本地维护当前时间戳段的起始值（如 current_segment_start）。
每个事务的时间戳从 current_segment_start 递增分配。
当段内时间戳用完或段过期时，Leader 从 ZK 获取新的时间戳段。
_____________

事务提交流程：

客户端请求：客户端发送事务请求到 Raft Leader。
立即响应：Leader 将事务暂存到 ​专用事务缓冲区​（非 Raft 日志缓冲区），并立即返回客户端成功响应。
异步持久化：Leader 异步将事务写入 Raft 日志，通过 AppendEntries 同步到 Followers。
同步：事务提交后，Leader 将当前 commitTime 同步到 ZK，更新全局水位线。
-------------

ZK 维护所有节点的 commitTime，Leader 从 ZK 获取所有节点的最新 commitTime，取最小值作为全局水位线。
水位线更新触发事务可见性判断。

*/
