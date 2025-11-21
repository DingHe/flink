在 Flink 的流处理架构中，StreamGraph 是构建执行计划（ExecutionGraph）之前的中间表示层，用于描述逻辑算子之间的拓扑关系。而 StreamGraph 的核心组成部分，就是 StreamNode 与 StreamEdge。
它们构成了流式程序的“逻辑 DAG（有向无环图）”，对理解 Flink 程序运行机制、OperatorChain 优化、调度结构等都非常重要。

# 1、 StreamNode 是什么

StreamNode 代表 Flink 中一个逻辑算子（Operator）的节点。

即：一个 StreamNode 表示一个 transformation 对应的 operator，例如：

MapOperator

FilterOperator

FlatMapOperator

KeyBy之后的 Partition 操作

Source 算子

Sink 算子

它是 Flink 程序逻辑结构的最小构成单位。

## 1.1、 StreamNode 重要字段解读

① operatorName

算子的名称，比如 Map -> Filter -> KeyBy 中的 map。

② operatorFactory

算子实例工厂，用来创建运行时的 StreamOperator。

③ parallelism / maxParallelism

控制并行度。

④ inputEdges

该算子所有的输入边（StreamEdge）。

⑤ outputEdges

该算子所有的输出边。

⑥ slotSharingGroup

控制算子如何分配到 slot 中。

⑦ chainingStrategy

是否允许被 chain，例如：

ALWAYS

NEVER

HEAD

HEAD_WITH_SOURCES

这是 OperatorChain 构成的核心依据。

# 2、StreamEdge 是什么

StreamEdge 是 StreamNode 与 StreamNode 之间的连接边。

它表示数据流从上游算子流向下游算子的方式，包括：

Shuffle 模式（FORWARD / RESCALE / REBALANCE / HASH ）

Partition 类型（KeyBy、rebalance、broadcast 等）

序列化器

边在 chain 中是否可合并

# 3、为什么 Flink 要设计 StreamNode 与 StreamEdge

## 3.1、支持多层次执行计划构建

Flink 的执行计划构建过程是分阶段的：

用户定义逻辑

Transformation 图

StreamGraph（逻辑 DAG）

JobGraph（物理 DAG）

ExecutionGraph（可执行任务 DAG）

StreamNode/StreamEdge 处于逻辑阶段和物理阶段之间。

## 3.2、支持 OperatorChain 优化

OperatorChain 需要判断：

上下游是否满足 chaining 条件

partition 是否 forward

节点是否可 chain

StreamNode/StreamEdge 提供必要信息。


## 3.3、 支持流图的可视化和调试

Flink UI 中的 DAG 基本来自 StreamGraph，即：

展示节点（StreamNode）

展示边（StreamEdge）

展示 shuffle 类型（partitioner）

## 3.4、支持不同 shuffle 模式和数据分区策略

StreamEdge 的 partitioner 决定了 shuffle 行为：

| 操作类型       | 分区方式      | 是否 shuffle |
| ---------- | --------- | ---------- |
| map/filter | Forward   | 否          |
| keyBy      | Hash      | 是          |
| rebalance  | Rebalance | 是          |
| broadcast  | Broadcast | 是          |


# 4、StreamNode 与 StreamEdge 对执行层的影响

## 4.1、Task 的划分（OperatorChain）

Flink 会根据 StreamEdge 判断算子是否 chain：

如果都是 forward edge

并且上下游都允许 chain

并且 slotSharingGroup 一致

就会 chain 成一个 task。

## 4.2、Shuffle 行为（网络传输）

每个 StreamEdge 的 partitioner 决定是否跨 subtask 发送数据。

## 4.3、并行度继承

StreamNode 默认从 Transformation 中继承并行度：

Source、Map、Filter 并行度可以不同

KeyBy 后的下游 Task 并行度由 key-group 分布决定

## 4.4、 构建 JobGraph / ExecutionGraph

StreamEdge 翻译成：

IntermediateDataSet

ExecutionEdge（任务间的边）

StreamNode 翻译成：

JobVertex（chain 后的 group）

ExecutionVertex（并行的 subtask）


StreamNode 代表算子，StreamEdge 代表算子之间的数据流逻辑；它们共同构成 Flink 的逻辑执行计划 StreamGraph。
