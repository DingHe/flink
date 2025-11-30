在 Flink 的逻辑执行计划 JobGraph 中，三个最重要的结构是：

JobVertex：任务节点（task），表示一个 OperatorChain

JobEdge：任务节点之间的输入边

IntermediateDataSet：JobVertex 的输出数据集（对应 shuffle 数据）

三者共同构成了 Flink 的 JobGraph，也是 Flink 调度、执行、shuffle 的核心基础。

# 1、JobVertex — 可调度任务（Task）

一个 JobVertex 表示 Flink 在运行时创建的一个 Task（chain 后）。

特点：

是一个任务节点

内含 operator chain（多个算子）

有并行度（parallelism）

有多个输入（来自 JobEdge）

有多个输出（IntermediateDataSet）

会在 ExecutionGraph 中扩展成多个 ExecutionVertex

# 2、 JobEdge — 任务之间的连接关系

JobEdge 表示：

下游 JobVertex 如何从上游 JobVertex 接收输入数据

它包含：

上游 IntermediateDataSet

下游 JobVertex 的 input index

分区方式（Hash、Forward、Rebalance、Broadcast）

JobEdge = vertex-edge-vertex 中的“边”，用于连接 JobVertex。

# 3、 IntermediateDataSet — 一个 JobVertex 的输出数据集

每个 JobVertex 的每个 output 会生成一个 IntermediateDataSet：

表示该 JobVertex 产生的可被消费的数据集

对应上游一个完整的 shuffle 数据集

系统会把它拆成多个分区（partition）

在 ExecutionGraph 中称为：

IntermediateResult

IntermediateResultPartition

# 4、 三者之间的拓扑关系

JobVertex 输出 IntermediateDataSet，通过 JobEdge 连接到下游 JobVertex，使下游可以消费上游的 IntermediateDataSet。

JobVertex A  --produces-->  IntermediateDataSet  --via JobEdge-->  JobVertex B

① JobVertex = task 粒度的调度与资源分配单元

② IntermediateDataSet = task 的物理输出（shuffle）数据集

③ JobEdge = task 之间的数据流依赖

三者共同决定：

task 调度拓扑

shuffle 方式（hash、rebalance、forward）

数据分区

是否需要网络传输

上下游的数据依赖约束


JobVertex 是任务节点，IntermediateDataSet 是任务输出的数据集，JobEdge 是下游任务消费上游数据集的连接边。三者构成 JobGraph 的核心 DAG 结构。



