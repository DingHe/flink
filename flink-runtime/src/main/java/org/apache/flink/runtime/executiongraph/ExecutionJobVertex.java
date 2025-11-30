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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.source.coordinator.SourceCoordinator;
import org.apache.flink.types.Either;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An {@code ExecutionJobVertex} is part of the {@link ExecutionGraph}, and the peer to the {@link
 * JobVertex}.
 *
 * <p>The {@code ExecutionJobVertex} corresponds to a parallelized operation. It contains an {@link
 * ExecutionVertex} for each parallel instance of that operation.
 */
// ExecutionJobVertex 是 Flink 执行图 (ExecutionGraph) 中的一个核心组件，是 逻辑作业图 (JobGraph) 中 JobVertex 的运行时对应物。
// 逻辑到物理的桥梁： 将一个逻辑算子（如 Map、Source、Sink）转换为 Flink 运行时可调度和管理的物理执行结构。
// 并行实例的容器： 它包含该算子的所有并行子任务实例，即 ExecutionVertex 数组。如果一个 Job Vertex 的并行度为 $N$，那么它就包含 $N$ 个 ExecutionVertex。
// Job 级别状态管理： 维护和聚合其所有子任务的运行时状态（如总进度、聚合状态、累计器），并提供对连接信息（输入、输出）和配置信息（并行度、资源）的访问。
// ExecutionJobVertex 代表了 Flink 作业中的一个并行算子阶段。


public class ExecutionJobVertex
        implements AccessExecutionJobVertex, Archiveable<ArchivedExecutionJobVertex> {

    /** Use the same log for all ExecutionGraph classes. */
    private static final Logger LOG = DefaultExecutionGraph.LOG;

    // 用于同步对 ExecutionJobVertex 内部状态（尤其是涉及到 taskInformationOrBlobKey 的并发访问）进行修改的锁对象。
    private final Object stateMonitor = new Object();

    // 所属的 ExecutionGraph 接口。
    // 允许访问其父级 ExecutionGraph 的信息和功能（如作业 ID、Blob 写入器）。
    private final InternalExecutionGraphAccessor graph;
    // 对应的逻辑作业顶点。
    // 指向 JobGraph 中表示该算子的原始配置。
    private final JobVertex jobVertex;
    // 并行子任务实例数组。
    // 包含该逻辑顶点所有并行度上的 $\text{ExecutionVertex}$ 实例。只有在初始化后才非空。
    @Nullable private ExecutionVertex[] taskVertices;
    // 产生的中间结果。
    // 包含该顶点所有输出边对应的中间结果集。
    @Nullable private IntermediateResult[] producedDataSets;
    // 任务的所有输入。
    // 包含该顶点从其前驱节点接收数据的所有中间结果集。
    @Nullable private List<IntermediateResult> inputs;
    // 并行度信息存储。
    // 存储配置的并行度、最大并行度等信息。
    private final VertexParallelismInformation parallelismInfo;
     // 槽位共享组。
     // 定义了哪些 Job Vertex 可以共享 TaskManager 中的 Slot 资源。
    private final SlotSharingGroup slotSharingGroup;
    // 协同定位组。
    // 如果任务需要严格地部署在同一个 TaskManager 上，此属性定义了该分组。
    @Nullable private final CoLocationGroup coLocationGroup;
    // 输入分片。
    // 对于 Source 算子，存储该任务需要处理的数据输入分片列表。
    @Nullable private InputSplit[] inputSplits;
     // 资源配置。
     // 描述该 Job Vertex 的子任务所需的资源（CPU、内存等）。
    private final ResourceProfile resourceProfile;
     // 已完成子任务计数器。
     // 记录当前已处于 FINISHED 状态的 ExecutionVertex 数量。
    private int numExecutionVertexFinished;

    /**
     * Either store a serialized task information, which is for all sub tasks the same, or the
     * permanent blob key of the offloaded task information BLOB containing the serialized task
     * information.
     */
    // 任务信息或其 Blob 键。
    // 存储序列化后的任务配置信息 $\text{TaskInformation}$，如果信息过大，则存储其在 Blob 存储中的永久键。
    private Either<SerializedValue<TaskInformation>, PermanentBlobKey> taskInformationOrBlobKey =
            null;
    // 算子协调器集合。
    // 包含该顶点中所有算子相关的 $\text{OperatorCoordinatorHolder}$ 实例，用于管理 Source 协调或状态处理。
    private final Collection<OperatorCoordinatorHolder> operatorCoordinators;
    // 输入分片分配器。
    // 对于 Source 算子，负责将 inputSplits 分配给各个并行子任务。
    @Nullable private InputSplitAssigner splitAssigner;

    @VisibleForTesting
    public ExecutionJobVertex(
            InternalExecutionGraphAccessor graph,
            JobVertex jobVertex,
            VertexParallelismInformation parallelismInfo,
            CoordinatorStore coordinatorStore,
            JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws JobException {

        if (graph == null || jobVertex == null) {
            throw new NullPointerException();
        }

        this.graph = graph;
        this.jobVertex = jobVertex;

        this.parallelismInfo = parallelismInfo;

        // verify that our parallelism is not higher than the maximum parallelism
        if (this.parallelismInfo.getParallelism() > this.parallelismInfo.getMaxParallelism()) {
            throw new JobException(
                    String.format(
                            "Vertex %s's parallelism (%s) is higher than the max parallelism (%s). Please lower the parallelism or increase the max parallelism.",
                            jobVertex.getName(),
                            this.parallelismInfo.getParallelism(),
                            this.parallelismInfo.getMaxParallelism()));
        }

        this.resourceProfile =
                ResourceProfile.fromResourceSpec(jobVertex.getMinResources(), MemorySize.ZERO);

        // take the sharing group
        this.slotSharingGroup = checkNotNull(jobVertex.getSlotSharingGroup());
        this.coLocationGroup = jobVertex.getCoLocationGroup();

        final List<SerializedValue<OperatorCoordinator.Provider>> coordinatorProviders =
                getJobVertex().getOperatorCoordinators();
        if (coordinatorProviders.isEmpty()) {
            this.operatorCoordinators = Collections.emptyList();
        } else {
            final ArrayList<OperatorCoordinatorHolder> coordinators =
                    new ArrayList<>(coordinatorProviders.size());
            try {
                for (final SerializedValue<OperatorCoordinator.Provider> provider :
                        coordinatorProviders) {
                    coordinators.add(
                            createOperatorCoordinatorHolder(
                                    provider,
                                    graph.getUserClassLoader(),
                                    coordinatorStore,
                                    jobManagerJobMetricGroup));
                }
            } catch (Exception | LinkageError e) {
                IOUtils.closeAllQuietly(coordinators);
                throw new JobException(
                        "Cannot instantiate the coordinator for operator " + getName(), e);
            }
            this.operatorCoordinators = Collections.unmodifiableList(coordinators);
        }
    }

    // 作用是执行物理初始化，将一个逻辑作业顶点 (JobVertex) 转化为可运行的并行执行结构，并为调度和执行做好准备。
    // 创建了所有的并行子任务实例（ExecutionVertex）、定义了输出数据集，并为 Source 任务准备了输入数据。

    protected void initialize(
            int executionHistorySizeLimit,
            Time timeout,
            long createTimestamp,
            SubtaskAttemptNumberStore initialAttemptCounts)
            throws JobException {

        // 前置条件检查 1。
        // 确保该 JobVertex 的并行度已确定且大于零。如果并行度为零或未确定，则抛出异常。
        checkState(parallelismInfo.getParallelism() > 0);
        // 前置条件检查 2。
        // 确保该 JobVertex 尚未初始化（即 taskVertices 仍为 $\text{null}$），防止重复初始化。
        checkState(!isInitialized());
        // 初始化 ExecutionVertex 数组。
        this.taskVertices = new ExecutionVertex[parallelismInfo.getParallelism()];
        // 初始化输入列表
        this.inputs = new ArrayList<>(jobVertex.getInputs().size());

        // create the intermediate results
        // 初始化输出数据集数组
        this.producedDataSets =
                new IntermediateResult[jobVertex.getNumberOfProducedIntermediateDataSets()];
        // 遍历逻辑输出。
        // 遍历 $\text{JobVertex}$ 配置中定义的所有逻辑输出数据集 (IntermediateDataSet)
        for (int i = 0; i < jobVertex.getProducedDataSets().size(); i++) {
            final IntermediateDataSet result = jobVertex.getProducedDataSets().get(i);

            this.producedDataSets[i] =
                    new IntermediateResult(
                            result,
                            this,
                            this.parallelismInfo.getParallelism(),
                            result.getResultType());
        }

        // create all task vertices
        // 创建 ExecutionVertex 实例
        for (int i = 0; i < this.parallelismInfo.getParallelism(); i++) {
            ExecutionVertex vertex =
                    createExecutionVertex(
                            this,
                            i,
                            producedDataSets,
                            timeout,
                            createTimestamp,
                            executionHistorySizeLimit,
                            initialAttemptCounts.getAttemptCount(i));

            this.taskVertices[i] = vertex;
        }

        // sanity check for the double referencing between intermediate result partitions and
        // execution vertices

        // 遍历所有新创建的IntermediateResult，
        // 检查其分配的分区数量是否恰好等于该 JobVertex 的并行度。这是确保数据流拓扑完整性的重要检查。
        for (IntermediateResult ir : this.producedDataSets) {
            if (ir.getNumberOfAssignedPartitions() != this.parallelismInfo.getParallelism()) {
                throw new RuntimeException(
                        "The intermediate result's partitions were not correctly assigned.");
            }
        }

        // set up the input splits, if the vertex has any
        try {
            @SuppressWarnings("unchecked")
            // 获取 JobVertex 上配置的 InputSplitSource（通常是 Source 任务才有）
            InputSplitSource<InputSplit> splitSource =
                    (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();

            if (splitSource != null) {
                // 设置类加载器。
                // 为了安全地调用用户代码（splitSource 可能在用户 JAR 包中），需要临时将当前线程的上下文类加载器设置为用户的类加载器。
                Thread currentThread = Thread.currentThread();
                ClassLoader oldContextClassLoader = currentThread.getContextClassLoader();
                currentThread.setContextClassLoader(graph.getUserClassLoader());
                try {
                    inputSplits =
                            splitSource.createInputSplits(this.parallelismInfo.getParallelism());

                    if (inputSplits != null) {
                        splitAssigner = splitSource.getInputSplitAssigner(inputSplits);
                    }
                } finally {
                    currentThread.setContextClassLoader(oldContextClassLoader);
                }
            } else {
                inputSplits = null;
            }
        } catch (Throwable t) {
            throw new JobException(
                    "Creating the input splits caused an error: " + t.getMessage(), t);
        }
    }

    protected ExecutionVertex createExecutionVertex(
            ExecutionJobVertex jobVertex,
            int subTaskIndex,
            IntermediateResult[] producedDataSets,
            Time timeout,
            long createTimestamp,
            int executionHistorySizeLimit,
            int initialAttemptCount) {
        return new ExecutionVertex(
                jobVertex,
                subTaskIndex,
                producedDataSets,
                timeout,
                createTimestamp,
                executionHistorySizeLimit,
                initialAttemptCount);
    }

    protected OperatorCoordinatorHolder createOperatorCoordinatorHolder(
            SerializedValue<OperatorCoordinator.Provider> provider,
            ClassLoader classLoader,
            CoordinatorStore coordinatorStore,
            JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws Exception {
        return OperatorCoordinatorHolder.create(
                provider,
                this,
                classLoader,
                coordinatorStore,
                false,
                getTaskInformation(),
                jobManagerJobMetricGroup);
    }
    //  检查初始化状态。
    //  返回 taskVertices 是否已创建（即 initialize方法是否已成功执行）
    public boolean isInitialized() {
        return taskVertices != null;
    }
    //判断并行度是否已确定
    public boolean isParallelismDecided() {
        return parallelismInfo.getParallelism() > 0;
    }

    /**
     * Returns a list containing the ID pairs of all operators contained in this execution job
     * vertex.
     *
     * @return list containing the ID pairs of all contained operators
     */
    public List<OperatorIDPair> getOperatorIDs() {
        return jobVertex.getOperatorIDs();
    }

    public void setMaxParallelism(int maxParallelism) {
        parallelismInfo.setMaxParallelism(maxParallelism);
    }

    public InternalExecutionGraphAccessor getGraph() {
        return graph;
    }

    public void setParallelism(int parallelism) {
        parallelismInfo.setParallelism(parallelism);
    }

    public JobVertex getJobVertex() {
        return jobVertex;
    }

    @Override
    public String getName() {
        return getJobVertex().getName();
    }

    @Override
    public int getParallelism() {
        return parallelismInfo.getParallelism();
    }

    @Override
    public int getMaxParallelism() {
        return parallelismInfo.getMaxParallelism();
    }

    @Override
    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    public boolean canRescaleMaxParallelism(int desiredMaxParallelism) {
        return parallelismInfo.canRescaleMaxParallelism(desiredMaxParallelism);
    }

    public JobID getJobId() {
        return graph.getJobID();
    }

    @Override
    public JobVertexID getJobVertexId() {
        return jobVertex.getID();
    }

    @Override
    public ExecutionVertex[] getTaskVertices() {
        if (taskVertices == null) {
            // The REST/web may try to get execution vertices of an uninitialized job vertex. Using
            // DEBUG log level to avoid flooding logs.
            LOG.debug(
                    "Trying to get execution vertices of an uninitialized job vertex "
                            + getJobVertexId());
            return new ExecutionVertex[0];
        }
        return taskVertices;
    }

    public IntermediateResult[] getProducedDataSets() {
        checkState(isInitialized());
        return producedDataSets;
    }

    public InputSplitAssigner getSplitAssigner() {
        checkState(isInitialized());
        return splitAssigner;
    }

    @Override
    public SlotSharingGroup getSlotSharingGroup() {
        return slotSharingGroup;
    }

    @Nullable
    public CoLocationGroup getCoLocationGroup() {
        return coLocationGroup;
    }

    public List<IntermediateResult> getInputs() {
        checkState(isInitialized());
        return inputs;
    }

    public Collection<OperatorCoordinatorHolder> getOperatorCoordinators() {
        checkState(isInitialized());
        return operatorCoordinators;
    }

    public List<SourceCoordinator<?, ?>> getSourceCoordinators() {
        List<SourceCoordinator<?, ?>> sourceCoordinators = new ArrayList<>();
        for (OperatorCoordinatorHolder oph : operatorCoordinators) {
            if (oph.coordinator() instanceof RecreateOnResetOperatorCoordinator) {
                RecreateOnResetOperatorCoordinator opc =
                        (RecreateOnResetOperatorCoordinator) oph.coordinator();
                try {
                    if (opc.getInternalCoordinator() instanceof SourceCoordinator) {
                        sourceCoordinators.add(
                                (SourceCoordinator<?, ?>) opc.getInternalCoordinator());
                    }
                } catch (Throwable e) {
                    throw new RuntimeException(
                            "Unexpected error occurred when get sourceCoordinators.", e);
                }
            }
        }
        return sourceCoordinators;
    }

    int getNumExecutionVertexFinished() {
        return numExecutionVertexFinished;
    }

    public Either<SerializedValue<TaskInformation>, PermanentBlobKey> getTaskInformationOrBlobKey()
            throws IOException {
        // only one thread should offload the task information, so let's also let only one thread
        // serialize the task information!
        synchronized (stateMonitor) {
            if (taskInformationOrBlobKey == null) {
                final BlobWriter blobWriter = graph.getBlobWriter();
                final TaskInformation taskInformation = getTaskInformation();
                taskInformationOrBlobKey =
                        BlobWriter.serializeAndTryOffload(taskInformation, getJobId(), blobWriter);
            }

            return taskInformationOrBlobKey;
        }
    }

    public TaskInformation getTaskInformation() {
        return new TaskInformation(
                jobVertex.getID(),
                jobVertex.getName(),
                parallelismInfo.getParallelism(),
                parallelismInfo.getMaxParallelism(),
                jobVertex.getInvokableClassName(),
                jobVertex.getConfiguration());
    }

    @Override
    public ExecutionState getAggregateState() {
        int[] num = new int[ExecutionState.values().length];
        for (ExecutionVertex vertex : getTaskVertices()) {
            num[vertex.getExecutionState().ordinal()]++;
        }

        return getAggregateJobVertexState(num, this.parallelismInfo.getParallelism());
    }

    // ---------------------------------------------------------------------------------------------

    public void connectToPredecessors(
            Map<IntermediateDataSetID, IntermediateResult> intermediateDataSets)
            throws JobException {
        checkState(isInitialized());

        List<JobEdge> inputs = jobVertex.getInputs();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    String.format(
                            "Connecting ExecutionJobVertex %s (%s) to %d predecessors.",
                            jobVertex.getID(), jobVertex.getName(), inputs.size()));
        }

        for (int num = 0; num < inputs.size(); num++) {
            JobEdge edge = inputs.get(num);

            if (LOG.isDebugEnabled()) {
                if (edge.getSource() == null) {
                    LOG.debug(
                            String.format(
                                    "Connecting input %d of vertex %s (%s) to intermediate result referenced via ID %s.",
                                    num,
                                    jobVertex.getID(),
                                    jobVertex.getName(),
                                    edge.getSourceId()));
                } else {
                    LOG.debug(
                            String.format(
                                    "Connecting input %d of vertex %s (%s) to intermediate result referenced via predecessor %s (%s).",
                                    num,
                                    jobVertex.getID(),
                                    jobVertex.getName(),
                                    edge.getSource().getProducer().getID(),
                                    edge.getSource().getProducer().getName()));
                }
            }

            // fetch the intermediate result via ID. if it does not exist, then it either has not
            // been created, or the order
            // in which this method is called for the job vertices is not a topological order
            IntermediateResult ires = intermediateDataSets.get(edge.getSourceId());
            if (ires == null) {
                throw new JobException(
                        "Cannot connect this job graph to the previous graph. No previous intermediate result found for ID "
                                + edge.getSourceId());
            }

            this.inputs.add(ires);

            EdgeManagerBuildUtil.connectVertexToResult(this, ires);
        }
    }

    // ---------------------------------------------------------------------------------------------
    //  Actions
    // ---------------------------------------------------------------------------------------------

    /** Cancels all currently running vertex executions. */
    public void cancel() {
        for (ExecutionVertex ev : getTaskVertices()) {
            ev.cancel();
        }
    }

    /**
     * Cancels all currently running vertex executions.
     *
     * @return A future that is complete once all tasks have canceled.
     */
    public CompletableFuture<Void> cancelWithFuture() {
        return FutureUtils.waitForAll(mapExecutionVertices(ExecutionVertex::cancel));
    }

    public CompletableFuture<Void> suspend() {
        return FutureUtils.waitForAll(mapExecutionVertices(ExecutionVertex::suspend));
    }

    @Nonnull
    private Collection<CompletableFuture<?>> mapExecutionVertices(
            final Function<ExecutionVertex, CompletableFuture<?>> mapFunction) {
        return Arrays.stream(getTaskVertices()).map(mapFunction).collect(Collectors.toList());
    }

    public void fail(Throwable t) {
        for (ExecutionVertex ev : getTaskVertices()) {
            ev.fail(t);
        }
    }

    void executionVertexFinished() {
        checkState(isInitialized());
        numExecutionVertexFinished++;
        if (numExecutionVertexFinished == parallelismInfo.getParallelism()) {
            getGraph().jobVertexFinished();
        }
    }

    void executionVertexUnFinished() {
        checkState(isInitialized());
        if (numExecutionVertexFinished == parallelismInfo.getParallelism()) {
            getGraph().jobVertexUnFinished();
        }
        numExecutionVertexFinished--;
    }

    public boolean isFinished() {
        return isParallelismDecided()
                && numExecutionVertexFinished == parallelismInfo.getParallelism();
    }

    // --------------------------------------------------------------------------------------------
    //  Accumulators / Metrics
    // --------------------------------------------------------------------------------------------

    public StringifiedAccumulatorResult[] getAggregatedUserAccumulatorsStringified() {
        Map<String, OptionalFailure<Accumulator<?, ?>>> userAccumulators = new HashMap<>();

        for (ExecutionVertex vertex : getTaskVertices()) {
            Map<String, Accumulator<?, ?>> next =
                    vertex.getCurrentExecutionAttempt().getUserAccumulators();
            if (next != null) {
                AccumulatorHelper.mergeInto(userAccumulators, next);
            }
        }

        return StringifiedAccumulatorResult.stringifyAccumulatorResults(userAccumulators);
    }

    // --------------------------------------------------------------------------------------------
    //  Archiving
    // --------------------------------------------------------------------------------------------

    @Override
    public ArchivedExecutionJobVertex archive() {
        return new ArchivedExecutionJobVertex(this);
    }

    // ------------------------------------------------------------------------
    //  Static Utilities
    // ------------------------------------------------------------------------

    /**
     * A utility function that computes an "aggregated" state for the vertex.
     *
     * <p>This state is not used anywhere in the coordination, but can be used for display in
     * dashboards to as a summary for how the particular parallel operation represented by this
     * ExecutionJobVertex is currently behaving.
     *
     * <p>For example, if at least one parallel task is failed, the aggregate state is failed. If
     * not, and at least one parallel task is cancelling (or cancelled), the aggregate state is
     * cancelling (or cancelled). If all tasks are finished, the aggregate state is finished, and so
     * on.
     *
     * @param verticesPerState The number of vertices in each state (indexed by the ordinal of the
     *     ExecutionState values).
     * @param parallelism The parallelism of the ExecutionJobVertex
     * @return The aggregate state of this ExecutionJobVertex.
     */
    public static ExecutionState getAggregateJobVertexState(
            int[] verticesPerState, int parallelism) {
        if (verticesPerState == null || verticesPerState.length != ExecutionState.values().length) {
            throw new IllegalArgumentException(
                    "Must provide an array as large as there are execution states.");
        }

        if (verticesPerState[ExecutionState.FAILED.ordinal()] > 0) {
            return ExecutionState.FAILED;
        }
        if (verticesPerState[ExecutionState.CANCELING.ordinal()] > 0) {
            return ExecutionState.CANCELING;
        } else if (verticesPerState[ExecutionState.CANCELED.ordinal()] > 0) {
            return ExecutionState.CANCELED;
        } else if (verticesPerState[ExecutionState.INITIALIZING.ordinal()] > 0) {
            return ExecutionState.INITIALIZING;
        } else if (verticesPerState[ExecutionState.RUNNING.ordinal()] > 0) {
            return ExecutionState.RUNNING;
        } else if (verticesPerState[ExecutionState.FINISHED.ordinal()] > 0) {
            return verticesPerState[ExecutionState.FINISHED.ordinal()] == parallelism
                    ? ExecutionState.FINISHED
                    : ExecutionState.RUNNING;
        } else {
            // all else collapses under created
            return ExecutionState.CREATED;
        }
    }
   //创建执行图顶点的工厂
    /** Factory to create {@link ExecutionJobVertex}. */
    public static class Factory {
        ExecutionJobVertex createExecutionJobVertex(
                InternalExecutionGraphAccessor graph,
                JobVertex jobVertex,
                VertexParallelismInformation parallelismInfo,
                CoordinatorStore coordinatorStore,
                JobManagerJobMetricGroup jobManagerJobMetricGroup)
                throws JobException {
            return new ExecutionJobVertex(
                    graph, jobVertex, parallelismInfo, coordinatorStore, jobManagerJobMetricGroup);
        }
    }
}
