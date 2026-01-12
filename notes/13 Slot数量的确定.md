- Flink 中 numberOfTaskSlots 和 CPU 核心不是一一对应关系，也不要求和线程一一对应。

一、一句话结论

Slot ≠ CPU Core

Slot ≠ Thread

一个 Slot 内可以跑多个线程

多个 Slot 可能共享同一个 CPU Core

Slot 的本质是：资源隔离 + 调度单位，而不是执行线程。

二、 Flink 的三层执行资源模型

- TaskManager（JVM 进程）

   对应 一个进程

   有固定的：CPU（操作系统层面）、Memory、Network


- Task Slot（逻辑资源隔离）

  numberOfTaskSlots = N

  把 TaskManager 的资源逻辑切分成 N 份

  每个 slot ≈ 1/N 的内存 + 网络缓冲

  CPU 不做硬隔离

  Slot 不是线程池，也不是 CPU Core

- SubTask / Operator（真正跑代码的）

  一个 subtask：

  通常绑定 一个 slot

  运行在 TaskManager 的线程池里

三、并行度、Slot、CPU 的关系公式

Job 最大并行度 ≤ 集群总 Slot 数


四、 那如何确定slot的数量呢

Slot 数量 = 每个 TaskManager 能稳定并行执行的 “重负载 subtask 数”

而这个数，主要由 CPU、内存、作业类型 决定，而不是拍脑袋。

Step 1：看单个 TaskManager 有什么资源

假设你有：

CPU cores = C
TaskManager 内存 = M (GB)

Step 2：判断作业类型（非常重要）

CPU 密集型     join / agg / window / heavy UDF

IO 密集型      Kafka / CDC / JDBC Sink

混合型         大多数实时数仓作业

Step 3：经验公式（直接可用）

1️⃣ CPU 密集型（最保守、最安全）  slots = C

示例：

CPU = 16 cores

slots = 16


2️⃣ 混合型（生产中最常见 ⭐）

slots = C / 2 ～ C

示例：

CPU = 16

slots = 8 ～ 12（常用 8）

这是 90% 生产 Flink 集群的选择

3️⃣ IO 密集型（Kafka / CDC）  slots = C / 2

示例：

CPU = 8

slots = 4
