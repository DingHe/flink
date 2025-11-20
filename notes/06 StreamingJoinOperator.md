深入理解 Flink 的 StreamingJoinOperator：流式 Join 的核心执行引擎

StreamingJoinOperator 是 Flink Table/SQL Runtime 中实现 流式 Join（Stream-Stream Join） 的核心算子。它承担了流式计算最关键的职能之一：在两个无限数据流之间执行有状态、可撤回（Retraction）、基于时间的增量 Join。

本文将从整体架构、功能职责、状态管理、处理流程、watermark 驱动的状态清理等方面，对这个算子进行完整的剖析。
