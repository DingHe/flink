# 1、什么是 Transformation

从源码的定义来看，Transformation 是 Flink DataStream API 的逻辑算子（Logical Operator）。

当你写代码：
```java
DataStream<Integer> result = stream.map(x -> x + 1);

```

Flink 并不会立刻创建 StreamNode 或 Task，而是：

➡️ 为 map() 创建一个 MapTransformation

➡️ Transformation 描述了算子的逻辑属性，包括：

- 上游 transformation（input）

- 输出类型（TypeInformation）

- 并行度（parallelism）

- 算子（OperatorFactory）

- 是否是 keyBy 后的 key-selector

- 是否可被链式合并（chaining）

- 是否边界阻塞（shuffle、rebalance）

Transformation 完全是逻辑层的抽象，不涉及物理执行。

它就像 Spark 中 Catalyst 的 logical plan，也像 Presto planner 的 relational operator。

# 2、 Transformation 的主要子类

Flink 的 Transformation 是一个抽象类，主要子类如下：

- OneInputTransformation

例如 map, flatMap, process：

保存：

上游 transformation

OneInputStreamOperatorFactory

- TwoInputTransformation

例如 join、connect：

```java
left.connect(right).process()

```

- PartitionTransformation

例如：

keyBy

rebalance

shuffle

partitionCustom

这些都不是物理算子，而是 Transformation 层的 partition 描述。

- UnionTransformation

用于将多个 Transformations 合并为一个输出流。

- SideOutputTransformation

侧输出流也是通过 Transformation 建立逻辑 DAG。

- SourceTransformation、SinkTransformation

最初和最终的 DAG 节点。

Flink 的 Transformation 是 DataStream 的逻辑算子抽象，是用户 API 与物理执行计划之间的桥梁。它描述数据流的逻辑结构、算子行为、并行度和数据分区策略，并在 StreamGraph 构建阶段被翻译成可执行的流算子链，是 Flink 流处理编译链路中最核心的抽象模型。
