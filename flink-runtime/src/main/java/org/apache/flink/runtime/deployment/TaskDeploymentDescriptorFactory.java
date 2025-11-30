/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.deployment;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.InternalExecutionGraphAccessor;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.scheduler.ClusterDatasetCorruptedException;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;
import org.apache.flink.types.Either;
import org.apache.flink.util.CompressedSerializedValue;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Factory of {@link TaskDeploymentDescriptor} to deploy {@link
 * org.apache.flink.runtime.taskmanager.Task} from {@link Execution}.
 */
// Flink JobMaster 侧的一个核心组件，其主要作用是封装创建任务部署描述符 (TaskDeploymentDescriptor, TDD) 的复杂逻辑。
// TDD 是一个包含任务执行所需所有信息的序列化对象，JobMaster 会将其发送给 TaskManager，TaskManagers 依据 TDD 来启动和运行任务。
// 收集信息： 从 Execution、ExecutionVertex 和 ExecutionGraph 中收集运行任务所需的所有元数据（如 Job 信息、Task 信息、检查点恢复信息等）
// 生成连接信息： 为任务要消费的上游分区（输入）和要生产的下游分区（输出）生成网络/Shuffle 连接描述符 (ShuffleDescriptor)
// 处理数据： 决定是否将大型元数据（如 Job/Task 信息和大量的 Shuffle 描述符）卸载到 Blob Server (BLOB: Binary Large Object) 上，以减少 JobMaster 与 TaskManager 之间的 RPC 消息大小，优化部署效率。

public class TaskDeploymentDescriptorFactory {
    /**
     * This is an expert option, that we do not want to expose in the documentation. The default
     * value is good enough for almost all cases
     */
    // 定义了一个阈值，
    // 用于判断是否应该将任务的输入 Shuffle 描述符卸载到 BLOB Server
    @Experimental
    public static final ConfigOption<Integer> OFFLOAD_SHUFFLE_DESCRIPTORS_THRESHOLD =
            key("jobmanager.task-deployment.offload-shuffle-descriptors-to-blob-server.threshold-num")
                    .intType()
                    .defaultValue(2048 * 2048)
                    .withDescription(
                            "Threshold for offloading shuffle descriptors to blob server. Once the number of shuffle descriptors"
                                    + " exceeds this value, we will offload the shuffle descriptors to blob server."
                                    + " This default value means JobManager need to serialize and transport"
                                    + " 2048 shuffle descriptors (almost 32KB) to 2048 consumers (64MB in total)");
    // 序列化后的 Job 级别配置和信息 (JobInformation)。
    // 它是一个包装类，可能包含序列化的 JobInformation 本身，或者指向 BLOB Server 上的一个永久 Blob Key（如果信息已卸载）。
    private final MaybeOffloaded<JobInformation> serializedJobInformation;
    // 当前作业的唯一标识符。
    private final JobID jobID;
    // 定义分区位置的约束。对于流处理 (STREAMING) 任务，上游分区的位置必须是已知的 (MUST_BE_KNOWN)；
    // 对于批处理 (BATCH) 任务，上游分区位置可以是未知的 (CAN_BE_UNKNOWN)，允许运行时再获取
    private final PartitionLocationConstraint partitionDeploymentConstraint;
    // 如果结果分区类型是 Hybrid（混合存储/网络），并且上游生产者尚未完成 (FINISHED)，
    // 则该标志决定是否将对应的 Shuffle 描述符标记为 UnknownShuffleDescriptor。
    // 这是针对混合模式下的优化或特殊处理。
    private final boolean nonFinishedHybridPartitionShouldBeUnknown;
    // 用于序列化 Shuffle 描述符的接口实例
    private final ShuffleDescriptorSerializer shuffleDescriptorSerializer;

    public TaskDeploymentDescriptorFactory(
            Either<SerializedValue<JobInformation>, PermanentBlobKey> jobInformationOrBlobKey,
            JobID jobID,
            PartitionLocationConstraint partitionDeploymentConstraint,
            BlobWriter blobWriter,
            boolean nonFinishedHybridPartitionShouldBeUnknown,
            int offloadShuffleDescriptorsThreshold) {
        this.serializedJobInformation = getSerializedJobInformation(jobInformationOrBlobKey);
        this.jobID = jobID;
        this.partitionDeploymentConstraint = partitionDeploymentConstraint;
        this.nonFinishedHybridPartitionShouldBeUnknown = nonFinishedHybridPartitionShouldBeUnknown;
        this.shuffleDescriptorSerializer =
                new DefaultShuffleDescriptorSerializer(
                        jobID, blobWriter, offloadShuffleDescriptorsThreshold);
    }

    public MaybeOffloaded<JobInformation> getSerializedJobInformation() {
        return serializedJobInformation;
    }
    // 负责将 JobMaster 侧的调度和元数据信息整合，创建出要发送给 TaskManager 的任务部署描述符 (TaskDeploymentDescriptor 或 TDD)。
    //
    public TaskDeploymentDescriptor createDeploymentDescriptor(
            Execution execution, // 当前任务的执行尝试实例 (Execution)，从中获取任务 ID、尝试次数和父顶点等信息。
            AllocationID allocationID, // 任务被分配到的资源槽的唯一 ID (AllocationID)。TaskManagers 用此 ID 识别资源槽。
            @Nullable JobManagerTaskRestore taskRestore, // 任务的恢复信息 (JobManagerTaskRestore)，如果任务是从检查点恢复启动，则包含状态数据的位置信息。对于新启动的任务，该参数为 null。
            Collection<ResultPartitionDeploymentDescriptor> producedPartitions) // 该任务将要产生的所有输出中间结果分区的部署描述符集合，这些信息已通过 ShuffleMaster 注册并准备好。
            throws IOException, ClusterDatasetCorruptedException {
        final ExecutionVertex executionVertex = execution.getVertex();

        return new TaskDeploymentDescriptor(
                jobID,
                serializedJobInformation,
                getSerializedTaskInformation(
                        executionVertex.getJobVertex().getTaskInformationOrBlobKey()),
                execution.getAttemptId(),
                allocationID,
                taskRestore,
                new ArrayList<>(producedPartitions),
                createInputGateDeploymentDescriptors(executionVertex));
    }
    // 核心作用是为当前任务（ExecutionVertex）创建所有输入连接的部署描述符列表，即 InputGateDeploymentDescriptor
    // 这些描述符告诉 TaskManager 上的 Task 如何以及在哪里连接和消费上游任务产生的数据。
    private List<InputGateDeploymentDescriptor> createInputGateDeploymentDescriptors(
            ExecutionVertex executionVertex) throws IOException, ClusterDatasetCorruptedException {

        // 从 ExecutionVertex 获取当前任务需要消费的所有分区组 (ConsumedPartitionGroup)。
        // 一个分区组通常对应于一个上游 JobVertex 的全部输出分区。
        List<ConsumedPartitionGroup> consumedPartitionGroups =
                executionVertex.getAllConsumedPartitionGroups();
        // 初始化一个列表，用于存储最终的 InputGateDeploymentDescriptor 实例。
        List<InputGateDeploymentDescriptor> inputGates =
                new ArrayList<>(consumedPartitionGroups.size());
        // 遍历所有需要消费的分区组。
        for (ConsumedPartitionGroup consumedPartitionGroup : consumedPartitionGroups) {
            // If the produced partition has multiple consumers registered, we
            // need to request the one matching our sub task index.
            // TODO Refactor after removing the consumers from the intermediate result partitions
            // 根据分区组中的第一个分区 ID 找到对应的 IntermediateResult 对象。
            // 该对象包含了数据流的逻辑信息。
            IntermediateResult consumedIntermediateResult =
                    executionVertex
                            .getExecutionGraphAccessor()
                            .getResultPartitionOrThrow(consumedPartitionGroup.getFirst())
                            .getIntermediateResult();

            IntermediateDataSetID resultId = consumedIntermediateResult.getId();
            ResultPartitionType partitionType = consumedIntermediateResult.getResultType();
            // 获取当前子任务需要消费的子分区（Subpartition）的索引范围。
            // 这对于非全部分区消费（如 RANGE 分区策略）是必需的。
            IndexRange subpartitionRange =
                    executionVertex
                            .getExecutionVertexInputInfo(resultId)
                            .getSubpartitionIndexRange();
            // 创建并添加 InputGate 描述符。
            inputGates.add(
                    new InputGateDeploymentDescriptor(
                            resultId,
                            partitionType,
                            subpartitionRange,
                            consumedPartitionGroup.size(),
                            getConsumedPartitionShuffleDescriptors(
                                    consumedIntermediateResult,
                                    consumedPartitionGroup,
                                    executionVertex.getExecutionGraphAccessor())));
        }

        final Map<IntermediateDataSetID, ShuffleDescriptorAndIndex[]>
                consumedClusterPartitionShuffleDescriptors;
        try {
            // 获取该分区组中所有上游分区的 ShuffleDescriptor（包含连接地址），
            // 这是 TaskManager 建立数据连接的关键。
            consumedClusterPartitionShuffleDescriptors =
                    getClusterPartitionShuffleDescriptors(executionVertex);
        } catch (Throwable e) {
            throw new ClusterDatasetCorruptedException(
                    e,
                    executionVertex
                            .getJobVertex()
                            .getJobVertex()
                            .getIntermediateDataSetIdsToConsume());
        }
        // 处理集群共享分区输入。
        for (Map.Entry<IntermediateDataSetID, ShuffleDescriptorAndIndex[]> entry :
                consumedClusterPartitionShuffleDescriptors.entrySet()) {
            // For FLIP-205, the JobGraph generating side ensure that the cluster partition is
            // produced with only one subpartition. Therefore, we always consume the partition with
            // subpartition index of 0.
            inputGates.add(
                    new InputGateDeploymentDescriptor(
                            entry.getKey(),
                            ResultPartitionType.BLOCKING_PERSISTENT,
                            0,
                            entry.getValue()));
        }

        return inputGates;
    }
    private List<MaybeOffloaded<ShuffleDescriptorGroup>> getConsumedPartitionShuffleDescriptors(
            IntermediateResult intermediateResult,
            ConsumedPartitionGroup consumedPartitionGroup,
            InternalExecutionGraphAccessor internalExecutionGraphAccessor)
            throws IOException {
        CachedShuffleDescriptors cachedShuffleDescriptors =
                intermediateResult.getCachedShuffleDescriptors(consumedPartitionGroup);
        if (cachedShuffleDescriptors == null) {
            cachedShuffleDescriptors =
                    intermediateResult.cacheShuffleDescriptors(
                            consumedPartitionGroup,
                            // compute all shuffle descriptors if it is not cached before.
                            computeConsumedPartitionShuffleDescriptors(
                                    consumedPartitionGroup, internalExecutionGraphAccessor));
        }
        cachedShuffleDescriptors.serializeShuffleDescriptors(shuffleDescriptorSerializer);

        return cachedShuffleDescriptors.getAllSerializedShuffleDescriptorGroups();
    }

    private ShuffleDescriptorAndIndex[] computeConsumedPartitionShuffleDescriptors(
            ConsumedPartitionGroup consumedPartitionGroup,
            InternalExecutionGraphAccessor internalExecutionGraphAccessor) {

        ShuffleDescriptorAndIndex[] shuffleDescriptors =
                new ShuffleDescriptorAndIndex[consumedPartitionGroup.size()];
        // Each edge is connected to a different result partition
        int i = 0;
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            shuffleDescriptors[i] =
                    new ShuffleDescriptorAndIndex(
                            getConsumedPartitionShuffleDescriptor(
                                    internalExecutionGraphAccessor.getResultPartitionOrThrow(
                                            partitionId),
                                    partitionDeploymentConstraint,
                                    nonFinishedHybridPartitionShouldBeUnknown),
                            i);
            i++;
        }
        return shuffleDescriptors;
    }

    private static Map<IntermediateDataSetID, ShuffleDescriptorAndIndex[]>
            getClusterPartitionShuffleDescriptors(ExecutionVertex executionVertex) {
        final InternalExecutionGraphAccessor internalExecutionGraphAccessor =
                executionVertex.getExecutionGraphAccessor();
        final List<IntermediateDataSetID> consumedClusterDataSetIds =
                executionVertex.getJobVertex().getJobVertex().getIntermediateDataSetIdsToConsume();
        Map<IntermediateDataSetID, ShuffleDescriptorAndIndex[]> clusterPartitionShuffleDescriptors =
                new HashMap<>();

        for (IntermediateDataSetID consumedClusterDataSetId : consumedClusterDataSetIds) {
            List<? extends ShuffleDescriptor> shuffleDescriptors =
                    internalExecutionGraphAccessor.getClusterPartitionShuffleDescriptors(
                            consumedClusterDataSetId);

            // For FLIP-205, the job graph generating side makes sure that the producer and consumer
            // of the cluster partition have the same parallelism and each consumer Task consumes
            // one output partition of the producer.
            checkState(
                    executionVertex.getTotalNumberOfParallelSubtasks() == shuffleDescriptors.size(),
                    "The parallelism (%s) of the cache consuming job vertex is "
                            + "different from the number of shuffle descriptors (%s) of the intermediate data set",
                    executionVertex.getTotalNumberOfParallelSubtasks(),
                    shuffleDescriptors.size());

            clusterPartitionShuffleDescriptors.put(
                    consumedClusterDataSetId,
                    new ShuffleDescriptorAndIndex[] {
                        new ShuffleDescriptorAndIndex(
                                shuffleDescriptors.get(executionVertex.getParallelSubtaskIndex()),
                                // For FLIP-205, the JobGraph generating side ensure that the
                                // cluster partition is produced with only one subpartition.
                                // Therefore, this index is always 0.
                                0)
                    });
        }
        return clusterPartitionShuffleDescriptors;
    }
    // 获取序列化的 Job 信息
    // 返回 serializedJobInformation 成员属性，即包含 Job 信息序列化数据或其 Blob Key 的 MaybeOffloaded 实例
    private static MaybeOffloaded<JobInformation> getSerializedJobInformation(
            Either<SerializedValue<JobInformation>, PermanentBlobKey> jobInformationOrBlobKey) {
        if (jobInformationOrBlobKey.isLeft()) {
            return new TaskDeploymentDescriptor.NonOffloaded<>(jobInformationOrBlobKey.left());
        } else {
            return new TaskDeploymentDescriptor.Offloaded<>(jobInformationOrBlobKey.right());
        }
    }
    // 获取序列化的 Task 信息。
    // 类似于 getSerializedJobInformation，将 TaskInformation 的序列化数据或其 Blob Key 封装到 MaybeOffloaded 中。
    private static MaybeOffloaded<TaskInformation> getSerializedTaskInformation(
            Either<SerializedValue<TaskInformation>, PermanentBlobKey> taskInfo) {
        return taskInfo.isLeft()
                ? new TaskDeploymentDescriptor.NonOffloaded<>(taskInfo.left())
                : new TaskDeploymentDescriptor.Offloaded<>(taskInfo.right());
    }

    public static ShuffleDescriptor getConsumedPartitionShuffleDescriptor(
            IntermediateResultPartition consumedPartition,
            PartitionLocationConstraint partitionDeploymentConstraint,
            boolean nonFinishedHybridPartitionShouldBeUnknown) {
        Execution producer = consumedPartition.getProducer().getPartitionProducer();

        ExecutionState producerState = producer.getState();
        Optional<ResultPartitionDeploymentDescriptor> consumedPartitionDescriptor =
                producer.getResultPartitionDeploymentDescriptor(consumedPartition.getPartitionId());

        ResultPartitionID consumedPartitionId =
                new ResultPartitionID(consumedPartition.getPartitionId(), producer.getAttemptId());

        return getConsumedPartitionShuffleDescriptor(
                consumedPartitionId,
                consumedPartition.getResultType(),
                consumedPartition.hasDataAllProduced(),
                producerState,
                partitionDeploymentConstraint,
                consumedPartitionDescriptor.orElse(null),
                nonFinishedHybridPartitionShouldBeUnknown);
    }

    @VisibleForTesting
    static ShuffleDescriptor getConsumedPartitionShuffleDescriptor(
            ResultPartitionID consumedPartitionId,
            ResultPartitionType resultPartitionType,
            boolean hasAllDataProduced,
            ExecutionState producerState,
            PartitionLocationConstraint partitionDeploymentConstraint,
            @Nullable ResultPartitionDeploymentDescriptor consumedPartitionDescriptor,
            boolean nonFinishedHybridPartitionShouldBeUnknown) {
        // The producing task needs to be RUNNING or already FINISHED
        if ((resultPartitionType.canBePipelinedConsumed() || hasAllDataProduced)
                && consumedPartitionDescriptor != null
                && isProducerAvailable(producerState)) {
            if (resultPartitionType.isHybridResultPartition()
                    && nonFinishedHybridPartitionShouldBeUnknown) {
                // if producer is not finished, shuffle descriptor should be unknown.
                if (producerState != ExecutionState.FINISHED) {
                    checkState(
                            partitionDeploymentConstraint
                                    == PartitionLocationConstraint.CAN_BE_UNKNOWN,
                            "partition location constraint should allow unknown shuffle descriptor when nonFinishedHybridPartitionShouldBeUnknown is true.");
                    return new UnknownShuffleDescriptor(consumedPartitionId);
                }
            }
            // partition is already registered
            return consumedPartitionDescriptor.getShuffleDescriptor();
        } else if (partitionDeploymentConstraint == PartitionLocationConstraint.CAN_BE_UNKNOWN) {
            // The producing task might not have registered the partition yet
            //
            // Currently, UnknownShuffleDescriptor will be created only if there is an intra-region
            // blocking edge in the graph. This means that when its consumer restarts, the
            // producer of the UnknownShuffleDescriptors will also restart. Therefore, it's safe to
            // cache UnknownShuffleDescriptors and there's no need to update the cache when the
            // corresponding partition becomes consumable.
            return new UnknownShuffleDescriptor(consumedPartitionId);
        } else {
            // throw respective exceptions
            throw handleConsumedPartitionShuffleDescriptorErrors(
                    consumedPartitionId, resultPartitionType, hasAllDataProduced, producerState);
        }
    }

    private static RuntimeException handleConsumedPartitionShuffleDescriptorErrors(
            ResultPartitionID consumedPartitionId,
            ResultPartitionType resultPartitionType,
            boolean hasAllDataProduced,
            ExecutionState producerState) {
        String msg;
        if (isProducerFailedOrCanceled(producerState)) {
            msg =
                    "Trying to consume an input partition whose producer has been canceled or failed. "
                            + "The producer is in state "
                            + producerState
                            + ".";
        } else {
            msg =
                    String.format(
                            "Trying to consume an input partition whose producer "
                                    + "is not ready (result type: %s, hasAllDataProduced: %s, producer state: %s, partition id: %s).",
                            resultPartitionType,
                            hasAllDataProduced,
                            producerState,
                            consumedPartitionId);
        }
        return new IllegalStateException(msg);
    }

    private static boolean isProducerAvailable(ExecutionState producerState) {
        return producerState == ExecutionState.RUNNING
                || producerState == ExecutionState.INITIALIZING
                || producerState == ExecutionState.FINISHED
                || producerState == ExecutionState.SCHEDULED
                || producerState == ExecutionState.DEPLOYING;
    }

    private static boolean isProducerFailedOrCanceled(ExecutionState producerState) {
        return producerState == ExecutionState.CANCELING
                || producerState == ExecutionState.CANCELED
                || producerState == ExecutionState.FAILED;
    }

    /**
     * Defines whether the partition's location must be known at deployment time or can be unknown
     * and, therefore, updated later.
     */
    public enum PartitionLocationConstraint {
        MUST_BE_KNOWN,  //部署时必须已知分区的位置
        CAN_BE_UNKNOWN; //分区的位置可以是未知的，稍后再更新（适用于批处理作业）

        public static PartitionLocationConstraint fromJobType(JobType jobType) {
            switch (jobType) {
                case BATCH:
                    return CAN_BE_UNKNOWN;
                case STREAMING:
                    return MUST_BE_KNOWN;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unknown JobType %s. Cannot derive partition location constraint for it.",
                                    jobType));
            }
        }
    }

    /**
     * This class represents the shuffle descriptor with it index in {@link ConsumedPartitionGroup}.
     */
    public static class ShuffleDescriptorAndIndex implements Serializable {
        private static final long serialVersionUID = 1L;

        private final ShuffleDescriptor shuffleDescriptor;

        private final int index;

        public ShuffleDescriptorAndIndex(ShuffleDescriptor shuffleDescriptor, int index) {
            this.shuffleDescriptor = shuffleDescriptor;
            this.index = index;
        }

        public ShuffleDescriptor getShuffleDescriptor() {
            return shuffleDescriptor;
        }

        public int getIndex() {
            return index;
        }
    }

    /** A set of shuffle descriptors that will be serialized together. */
    public static class ShuffleDescriptorGroup implements Serializable {
        private static final long serialVersionUID = 1L;

        private final ShuffleDescriptorAndIndex[] shuffleDescriptors;

        public ShuffleDescriptorGroup(ShuffleDescriptorAndIndex[] shuffleDescriptors) {
            this.shuffleDescriptors = checkNotNull(shuffleDescriptors);
        }

        public ShuffleDescriptorAndIndex[] getShuffleDescriptors() {
            return shuffleDescriptors;
        }
    }

    /** Serialize shuffle descriptors. */
    interface ShuffleDescriptorSerializer {
        /**
         * Serialize and try offload shuffle descriptors.
         *
         * @param shuffleDescriptorGroup to serialize
         * @param numConsumer consumers number of these shuffle descriptors, it means how many times
         *     serialized shuffle descriptor should be sent
         * @return offloaded or non-offloaded serialized shuffle descriptors
         */
        MaybeOffloaded<ShuffleDescriptorGroup> serializeAndTryOffloadShuffleDescriptor(
                ShuffleDescriptorGroup shuffleDescriptorGroup, int numConsumer) throws IOException;
    }

    private static class DefaultShuffleDescriptorSerializer implements ShuffleDescriptorSerializer {
        private final JobID jobID;
        private final BlobWriter blobWriter;
        private final int offloadShuffleDescriptorsThreshold;

        public DefaultShuffleDescriptorSerializer(
                JobID jobID, BlobWriter blobWriter, int offloadShuffleDescriptorsThreshold) {
            this.jobID = checkNotNull(jobID);
            this.blobWriter = checkNotNull(blobWriter);
            this.offloadShuffleDescriptorsThreshold = offloadShuffleDescriptorsThreshold;
        }

        @Override
        public MaybeOffloaded<ShuffleDescriptorGroup> serializeAndTryOffloadShuffleDescriptor(
                ShuffleDescriptorGroup shuffleDescriptorGroup, int numConsumer) throws IOException {

            final CompressedSerializedValue<ShuffleDescriptorGroup> compressedSerializedValue =
                    CompressedSerializedValue.fromObject(shuffleDescriptorGroup);

            final Either<SerializedValue<ShuffleDescriptorGroup>, PermanentBlobKey>
                    serializedValueOrBlobKey =
                            shouldOffload(
                                            shuffleDescriptorGroup.getShuffleDescriptors(),
                                            numConsumer)
                                    ? BlobWriter.offloadWithException(
                                            compressedSerializedValue, jobID, blobWriter)
                                    : Either.Left(compressedSerializedValue);

            if (serializedValueOrBlobKey.isLeft()) {
                return new TaskDeploymentDescriptor.NonOffloaded<>(serializedValueOrBlobKey.left());
            } else {
                return new TaskDeploymentDescriptor.Offloaded<>(serializedValueOrBlobKey.right());
            }
        }

        /**
         * Determine whether shuffle descriptors should be offloaded to blob server.
         *
         * @param shuffleDescriptorsToSerialize shuffle descriptors to serialize
         * @param numConsumers how many consumers this serialized shuffle descriptor should be sent
         * @return whether shuffle descriptors should be offloaded to blob server
         */
        private boolean shouldOffload(
                ShuffleDescriptorAndIndex[] shuffleDescriptorsToSerialize, int numConsumers) {
            return shuffleDescriptorsToSerialize.length * numConsumers
                    >= offloadShuffleDescriptorsThreshold;
        }
    }
}
