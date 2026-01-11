
- JobManager的内存分为进程内存和Flink内存

- 进程内存 = Flink内存 + 元空间内存 + OverHead内存

- Flink内存分为堆外内存和堆内内存

- Flink 可以根据配置参数自动计算出各个部分的内存，实现类是JobManagerFlinkMemoryUtils

- 下面的配置memory.process.size 是进程内存


```yaml
jobmanager:
  # The host interface the JobManager will bind to. By default, this is localhost, and will prevent
  # the JobManager from communicating outside the machine/container it is running on.
  # On YARN this setting will be ignored if it is set to 'localhost', defaulting to 0.0.0.0.
  # On Kubernetes this setting will be ignored, defaulting to 0.0.0.0.
  #
  # To enable this, set the bind-host address to one that has access to an outside facing network
  # interface, such as 0.0.0.0.
  bind-host: localhost
  rpc:
    # The external address of the host on which the JobManager runs and can be
    # reached by the TaskManagers and any clients which want to connect. This setting
    # is only used in Standalone mode and may be overwritten on the JobManager side
    # by specifying the --host <hostname> parameter of the bin/jobmanager.sh executable.
    # In high availability mode, if you use the bin/start-cluster.sh script and setup
    # the conf/masters file, this will be taken care of automatically. Yarn
    # automatically configure the host name based on the hostname of the node where the
    # JobManager runs.
    address: localhost
    # The RPC port where the JobManager is reachable.
    port: 6123
  memory:
    process:
      # The total process memory size for the JobManager.
      # Note this accounts for all memory usage within the JobManager process, including JVM metaspace and other overhead.
      size: 1600m
  execution:
    # The failover strategy, i.e., how the job computation recovers from task failures.
    # Only restart tasks that may have been affected by the task failure, which typically includes
    # downstream tasks and potentially upstream tasks if their produced data is no longer available for consumption.
    failover-strategy: region

```
