# 1、Keyed State 简介

在 Flink 中，Keyed State 是与特定 Key 关联的状态。在 Keyed Stream 中，每个唯一的 Key 都可以维护一个独立的状态实例。这些状态是 Flink 容错机制的基础。

- ValueState (值状态)

ValueState 是最简单的一种状态，它只保存一个可以读写的单值。
用于存储单个变量，如计数器、最新时间戳或某个键的最新值。

update(T value): 设置或覆盖当前值。

T value = get(): 获取当前值。

clear(): 清除当前值。

使用场景	统计每个用户最近一次登录时间、存储每个商品的实时库存数。


- ListState (列表状态)

ListState 用于存储一个元素列表。它支持向列表中追加元素，并在需要时获取整个列表。

存储某个键的历史记录、收集属于一个窗口的所有元素。

add(T value): 向列表中追加单个元素。

addAll(List<T> values): 追加一个元素列表。

Iterable<T> get(): 获取整个列表。

update(List<T> values): 用新列表覆盖旧列表。

使用场景	收集一个 Key 在窗口内接收到的所有事件，用于触发计算或存储历史轨迹。


- MapState (映射状态)

MapState 用于将 Keyed State 内部的数据组织成一个 Key-Value 映射（Map）。它允许用户对 Map 内部的 Key 进行存取。

存储一组具有内部 Key 的复杂状态，例如存储每个 Key 下不同类型（用内部 Key 区分）的指标。

put(UK key, UV value): 写入或更新 Map 中的一个内部键值对。

UV value = get(UK key): 获取 Map 中指定内部 Key 的值。

remove(UK key): 移除 Map 中的一个内部键值对

entries(): 获取 Map 中所有的键值对。

使用场景	存储一个 Key 的所有会话（以 Session ID 为内部 Key），或存储一个用户的多个偏好设置。


- AggregatingState (聚合状态)

AggregatingState 用于通过一个聚合函数 (AggregateFunction) 持续地将新的输入元素 (IN) 合并到一个累加器 (ACC) 中，并在需要时从累加器中计算出最终结果 (OUT)。

执行复杂的增量计算，如计算平均值、标准差等，其中中间状态 (ACC) 与最终结果 (OUT) 的类型可以不同。

AggregateFunction<IN, ACC, OUT>：定义了 createAccumulator、add、getResult 和 merge 方法。

add(IN value): 将新值添加到累加器中。

OUT result = get(): 获取累加器计算出的最终结果。

使用场景	计算某 Key 的平均订单价格（ACC=总金额和订单数，OUT=平均值）。


- ReducingState (归约状态)

ReducingState 用于通过一个归约函数 (ReduceFunction) 将新的输入元素 (V) 与现有状态值持续地合并为一个单一的、同类型的归约结果。

执行简单的增量归约计算，其中输入、累积值和输出的类型必须相同 (V)。

ReduceFunction<V>：定义了 reduce(V value1, V value2) 方法。

add(V value): 将新值与现有状态值合并。

V result = get(): 获取当前的归约结果。

使用场景	计算某 Key 的总和、最大值或最小值。


