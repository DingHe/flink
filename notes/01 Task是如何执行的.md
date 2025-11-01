# 1、涉及组件

ResourceProfile - 将 Task 运行所需的所有资源（CPU、各种内存、扩展资源）封装在一个单一的对象中
cpuCores
taskHeapMemory  任务运行时在 JVM 堆上占用的内存大小。
taskOffHeapMemory 任务运行时在 JVM 堆外占用的内存大小（如直接内存）,要用途 外部组件支持： Netty 网络缓冲区、RocksDBStateBackend 的 Native 内存、JVM Direct Buffer（例如用于文件 I/O）
managedMemory  由 Flink 自身管理和使用的内存，主要用于批处理中的排序、哈希表等。也是分配在堆外内存，由 MemoryManager 管理
networkMemory 用于网络栈（如数据传输缓冲区）的内存大小。

TaskSlot  --Flink 任务执行器 (TaskExecutor) 上的一个逻辑资源容器
resourceProfile 描述了分配给这个 TaskSlot 的 CPU、管理内存、网络内存等资源总量
tasks  存储当前在这个槽位中运行的所有任务（TaskSlotPayload）

MemoryManager 管理 TaskExecutor（TaskManager）预先分配的一大块堆外（Off-Heap）原始内存。这部分内存用于 Flink 内部的高性能操作，如排序（Sort）、哈希连接（Hash Join）、网络缓冲区、以及 RocksDBStateBackend 等。
allocatedSegments  已分配的内存页记录。键是内存的所有者（Owner）（通常是 Task 或算子实例），值是分配给该所有者的 MemorySegment 集合
reservedMemory  键是内存的所有者，值是该所有者以非 MemorySegment 形式（即大块字节）保留的内存总量（单位：字节）