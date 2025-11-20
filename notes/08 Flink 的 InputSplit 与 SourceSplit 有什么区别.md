在 Flink 中，InputSplit 与 SourceSplit 都是“分片”的概念，用于切分外部数据源，使多个并行实例可以协同读取数据。

但它们属于 两个时代、两个 Source 架构，设计目标与功能差别巨大：

InputSplit → Flink Old Source API（SourceFunction 时代）

SourceSplit → Flink New Source API（FLIP-27 Source 体系）

# 1、 InputSplit 是什么

InputSplit 属于旧版 Source API（SourceFunction）

主要用于：

InputFormatSourceFunction

FileInputFormat

HadoopInputFormat

Kafka 早期 Connector

以及所有基于 OldSource 的批和流 Source

设计理念：批式模型

InputSplit 的理念来自 Hadoop MapReduce：

“将输入数据切成多个分片（split），每个分片由一个 task 处理。”

# 2、SourceSplit 是什么

SourceSplit 属于 Flink 新版 Source API（FLIP-27）

用于如下所有新式 Source：

FileSource

KafkaSource

PulsarSource

IcebergSource

Debezium-CDC Source

DataStream API 中所有基于 FLIP-27 的连接器

设计理念：流式 + 批式统一模型（Unified Source API）

SourceSplit 不只是“数据的分段”，更是 可以恢复、可序列化、具备位置偏移量、可动态产生的状态单元。

# 3、InputSplit vs SourceSplit：核心区别总结

| 目标    | InputSplit | SourceSplit                     |
| ----- | ---------- | ------------------------------- |
| 批处理   | ✔          | ✔                               |
| 流处理   | ❌          | ✔（核心目标）                         |
| 动态发现  | ❌          | ✔ split enumerator 支持           |
| 状态恢复  | 弱          | 强                               |
| 分布式协调 | 无          | 有（enumerator → reader 分配 split） |


| 类型          | Split 生成逻辑                         |
| ----------- | ---------------------------------- |
| InputSplit  | 作业启动一次性生成所有 splits                 |
| SourceSplit | SplitEnumerator 动态生成、动态分配，可持续扫描新分区 |



InputSplit 是老 Source API 的“数据切片”，不支持动态发现和状态恢复；
SourceSplit 是新 Source API 的核心抽象，承担完整的分片、偏移、checkpoint、动态分配等职责，是现代流式 Source 的基础。




