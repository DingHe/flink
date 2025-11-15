# 1、变更日志流涉及的类

在 Apache Flink 中，“变更日志流”（State Changelog）通常指的是 Flink 状态变更日志 (State Changelog) 机制。
该机制是 Flink 状态后端的一种优化和加速 Checkpointing/Savepointing 的功能。 
启用 Flink 变更日志流（State Changelog）的核心目的，是在不影响恢复时间的前提下，显著减少 Checkpoint 的写入时间。

  - ChangelogState

  - StateChangeOperation


# 2、什么时候要开启变更日志流

- 核心应用场景：加速 Checkpoint 写入，这是启用变更日志流最直接和主要的原因。

场景：大状态和频繁 Checkpoint

当您的 Flink 作业满足以下条件时，应考虑启用状态变更日志：

状态非常大： 作业的 Keyed State 已经达到数百 GB 甚至数 TB。

Checkpoint 延迟成为瓶颈： 由于状态太大，每次 Checkpoint 写入 Checkpoint 存储（如 HDFS、S3）的时间很长，导致 Checkpointing 间隔被拉长，或者导致整体延迟增加。

状态更新是增量的： 在 Checkpoint 间隔内，只有少量的状态数据被修改。

启用理由

在没有变更日志的情况下，每次 Checkpoint 都需要将所有被修改的 Keyed State 块完整地写入 Checkpoint 存储。

开启变更日志后，状态后端（如 RocksDB 或 Heap State Backend）会将每一次状态修改（put、add、clear 等）都记录到临时的、专用的日志存储中。

加速 Checkpoint： Checkpoint 写入时，只需要将这个时间段内产生的变更日志写入 Checkpoint 存储，而不是整个状态的快照。这个日志文件通常比完整的状态快照小得多，因此Checkpoint 写入时间会大大缩短。

减少 I/O 压力： 减少了对 Checkpoint 存储（通常是远程存储）的写入量。


 - 次要应用场景：增强状态后端功能

场景：使用云存储上的 RocksDBStateBackend

如果使用 RocksDBStateBackend，并且将 Checkpoint 存储在云存储（如 S3/OSS）上，变更日志可以优化其性能：

优化 RocksDB 增量 Checkpoint： 即使 RocksDB 默认支持增量 Checkpoint，但它仍需要上传 SST 文件。状态变更日志可以进一步补充和加速这一过程。

分离读写路径： 变更日志流通常使用一个高性能、低延迟的组件（如 Kafka/Kinesis 或专门的文件系统）来存储日志，这使得状态的写入和 Checkpoint 的恢复路径更加清晰和高效。


 - 不适合开启的场景 (权衡)

虽然变更日志有很多优势，但在以下情况下开启它可能没有收益，甚至会带来额外开销：

小状态或低吞吐量： 如果状态很小，或者 Checkpoint 写入时间本身就很短（例如几秒），那么启用变更日志带来的额外序列化和日志管理开销可能会抵消 Checkpoint 写入时间的收益。

状态完全更新（Full State Update）： 如果在 Checkpoint 间隔内，大部分 Keyed State 都被修改（例如，一个大 Map 状态的大部分条目都被更新），那么变更日志的体积会接近于完整状态快照的体积，此时收益很小。

严格限制额外基础设施： 变更日志机制可能需要额外的日志存储（尽管 Flink 也在努力将日志存储集成到 Checkpoint 存储中），如果对部署环境有严格限制，需要谨慎评估。


# 3、如何开始变更日志流

- state.changelog.enabled

启用变更日志。 设为 true 来激活该功能。

- state.backend.changelog.state-backend

变更日志存储。 设置为日志记录的存储方式。目前推荐使用内置的 embedded 模式，它将变更日志存储在 Checkpoint 目录中。

- state.backend.changelog.periodic-materialization.enabled

开启周期性物化。物化指将变更日志应用到状态后端并形成一个新的全量/增量快照。
开启后，即使 Checkpoint 失败，物化也会定期进行，以防止日志堆积过大。

虽然 State Changelog 机制旨在加速 Checkpoint，但它依赖于底层状态后端来实际存储和管理状态数据。您需要配置一个支持 Keyed State 的后端，最常见的是 RocksDB。

RocksDBStateBackend 是最常与状态变更日志配合使用的后端，因为它通常用于处理大状态。

state.backend: org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend

如果您的 Job 状态相对较小，也可以使用 Heap State Backend（不推荐用于大状态）。

state.backend: org.apache.flink.runtime.state.hashmap.HashMapStateBackend
