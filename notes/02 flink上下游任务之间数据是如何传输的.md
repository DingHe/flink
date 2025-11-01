# 1、Flink 数据传输的 5 大步骤

  Operator Chain  -> RecordWriter  ->  ResultSubpartition  -> InputGate  -> InputChannel

# 2、关键组件

   - ResultPartition（上游输出）<br>
     每个上游 Task 会生成一个 ResultPartition，它代表一个 Task 的输出数据集。常见类型：<br>
     PIPELINED：流式传输，不落盘（默认 Stream）<br>
     BLOCKING：全量落盘（Batch 模式）<br>
     ResultPartition 内部包含多个 ResultSubpartition

   - ResultSubpartition（分区片段）<br>
     ResultPartition 会根据并行度做数据分区，例如：<br>
     并行度：4<br>
     上游每个 Task -> 4 个 ResultSubpartition

   - InputGate（下游输入）<br>
     下游 Task 初始化时，会创建 InputGate。<br>
     InputGate 的作用是：管理多个 InputChannel（来自上游不同 Task）和 从多个上游协调读取数据（公平调度）

   - InputChannel（网络读取通道）<br>
     对应上游一个 ResultSubpartition。
