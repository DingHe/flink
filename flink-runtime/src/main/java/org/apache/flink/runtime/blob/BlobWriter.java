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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.types.Either;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/** BlobWriter is used to upload data to the BLOB store. */
// Flink 的 BLOB 存储服务（Binary Large Object Store）中负责写入数据的核心组件。
// BlobWriter 接口定义了将数据上传到 Flink 集群的 BLOB Server（二进制大对象服务器）所必须具备的功能。
// 数据上传（putPermanent）： 接收 JobMaster 侧的二进制数据（通常是大型配置、用户代码、大序列化对象等），并将其安全、持久地上传到 BLOB Server。
// 生成键值： 为上传的数据生成一个唯一的、永久的标识符 PermanentBlobKey。
// 支持卸载（offload）： 提供静态辅助方法，用于判断一个序列化对象的大小是否超过阈值，并决定是直接传输（作为序列化对象）还是上传到 BLOB Server（作为 PermanentBlobKey 引用）。这减少了 JobMaster 与 TaskManager 之间 RPC 消息的大小。
public interface BlobWriter {

    Logger LOG = LoggerFactory.getLogger(BlobWriter.class);

    /**
     * Uploads the data of the given byte array for the given job to the BLOB server and makes it a
     * permanent BLOB.
     *
     * @param jobId the ID of the job the BLOB belongs to
     * @param value the buffer to upload
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while writing it to a local file, or
     *     uploading it to the HA store
     */
    // 上传永久 BLOB（字节数组）
    // 将给定的字节数组数据上传到 BLOB Server，并将其标记为永久 BLOB（Permanent BLOB）。
    // 返回一个唯一的 PermanentBlobKey 用于识别该数据。jobId 用于将数据与特定作业关联。
    PermanentBlobKey putPermanent(JobID jobId, byte[] value) throws IOException;

    /**
     * Uploads the data from the given input stream for the given job to the BLOB server and makes
     * it a permanent BLOB.
     *
     * @param jobId ID of the job this blob belongs to
     * @param inputStream the input stream to read the data from
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while reading the data from the input
     *     stream, writing it to a local file, or uploading it to the HA store
     */
    // 上传永久 BLOB（输入流）
    // 提供了一个重载方法，允许从输入流中读取数据并上传为永久 BLOB。这有助于处理非常大的数据而无需先完全加载到内存中。
    PermanentBlobKey putPermanent(JobID jobId, InputStream inputStream) throws IOException;

    /**
     * Delete the uploaded data with the given {@link JobID} and {@link PermanentBlobKey}.
     *
     * @param jobId ID of the job this blob belongs to
     * @param permanentBlobKey the key of this blob
     */
    // 删除永久 BLOB。
    // 根据给定的 JobID 和 PermanentBlobKey，从 BLOB Server 上删除对应的永久 BLOB 数据。
    // 返回删除操作是否成功。
    boolean deletePermanent(JobID jobId, PermanentBlobKey permanentBlobKey);

    /**
     * Returns the min size before data will be offloaded to the BLOB store.
     *
     * @return minimum offloading size
     */
    // 获取最小卸载大小。
    // 返回一个整数值，表示数据大小必须超过此阈值才会被考虑卸载到 BLOB Server。
    // 这是用于优化数据传输的配置参数。
    int getMinOffloadingSize();

    /**
     * Serializes the given value and offloads it to the BlobServer if its size exceeds the minimum
     * offloading size of the BlobServer.
     *
     * @param value to serialize
     * @param jobId to which the value belongs.
     * @param blobWriter to use to offload the serialized value
     * @param <T> type of the value to serialize
     * @return Either the serialized value or the stored blob key
     * @throws IOException if the data cannot be serialized
     */
    // 序列化并尝试卸载。
    // 这是用户最常调用的入口方法。
    static <T> Either<SerializedValue<T>, PermanentBlobKey> serializeAndTryOffload(
            T value, JobID jobId, BlobWriter blobWriter) throws IOException {
        Preconditions.checkNotNull(value);

        final SerializedValue<T> serializedValue = new SerializedValue<>(value);

        return tryOffload(serializedValue, jobId, blobWriter);
    }

    static <T> Either<SerializedValue<T>, PermanentBlobKey> tryOffload(
            SerializedValue<T> serializedValue, JobID jobId, BlobWriter blobWriter) {
        Preconditions.checkNotNull(serializedValue);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(blobWriter);

        if (serializedValue.getByteArray().length < blobWriter.getMinOffloadingSize()) {
            return Either.Left(serializedValue);
        } else {
            return offloadWithException(serializedValue, jobId, blobWriter);
        }
    }

    static <T> Either<SerializedValue<T>, PermanentBlobKey> offloadWithException(
            SerializedValue<T> serializedValue, JobID jobId, BlobWriter blobWriter) {
        Preconditions.checkNotNull(serializedValue);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(blobWriter);
        try {
            final PermanentBlobKey permanentBlobKey =
                    blobWriter.putPermanent(jobId, serializedValue.getByteArray());
            return Either.Right(permanentBlobKey);
        } catch (IOException e) {
            LOG.warn("Failed to offload value for job {} to BLOB store.", jobId, e);
            return Either.Left(serializedValue);
        }
    }
}
