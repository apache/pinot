/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.connector.spark.v3.datasource

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.pinot.connector.spark.common.PinotDataSourceWriteOptions
import org.apache.pinot.spi.data.Schema
import org.apache.pinot.spi.ingestion.batch.spec.Constants
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, Write, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.NonFatal

class PinotWrite(
                  logicalWriteInfo: LogicalWriteInfo
                ) extends Write with BatchWrite {
  private val logger: Logger = LoggerFactory.getLogger(classOf[PinotWrite])
  private[pinot] val writeOptions: PinotDataSourceWriteOptions = PinotDataSourceWriteOptions.from(logicalWriteInfo.options())
  private[pinot] val writeSchema: StructType = logicalWriteInfo.schema()
  private[pinot] val pinotSchema: Schema = SparkToPinotTypeTranslator.translate(
    writeSchema, writeOptions.tableName, writeOptions.timeColumnName, writeOptions.timeFormat, writeOptions.timeGranularity)

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = {
    // capture the values to allow lambda serialization
    val _writeOptions = writeOptions
    val _writeSchema = writeSchema
    val _pinotSchema = pinotSchema

    (partitionId: Int, taskId: Long) => {
      new PinotDataWriter(
        partitionId,
        taskId,
        _writeOptions,
        _writeSchema,
        _pinotSchema)
    }
  }

  override def toBatch: BatchWrite = this

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    messages.foreach(m => logger.info("Spark write committed: {}", m))
  }

  /**
   * Called by Spark on the driver when the write job fails. Best-effort: delete the segment tar
   * files at {@code writeOptions.savePath} that correspond to the already-succeeded tasks in
   * {@code messages}, so a retry does not duplicate them. We do not throw: the driver already
   * has a failure to surface, and a cleanup miss is recoverable by the user.
   *
   * This is the runtime-level counterpart to the contract-level guard that
   * {@link PinotWriteBuilder#overwrite} / {@code truncate} provide against silent overwrite —
   * together they ensure partial-failure retries don't silently produce duplicate segments in
   * the downstream controller push step.
   */
  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    val savePath = writeOptions.savePath
    if (savePath == null || savePath.isEmpty) {
      messages.foreach(m => logger.warn("Spark write aborted and no savePath is set, cannot clean up: {}", m))
      return
    }
    val fsOpt = try {
      Some(FileSystem.get(new URI(savePath), new Configuration()))
    } catch {
      case NonFatal(t) =>
        logger.warn("Spark write aborted; could not obtain Hadoop FileSystem for savePath={} " +
          "— leftover segment tars may remain and be duplicated on retry.", savePath, t)
        None
    }
    messages.foreach {
      // Per Spark's BatchWrite.abort contract, the messages array may contain nulls for tasks
      // that failed before producing a commit message; nothing to clean up for those.
      case null =>
      case m: SuccessWriterCommitMessage =>
        val tarName = s"${m.segmentName}${Constants.TAR_GZ_FILE_EXT}"
        val destPath = new Path(s"$savePath/$tarName")
        fsOpt match {
          case Some(fs) =>
            try {
              if (fs.delete(destPath, /* recursive */ false)) {
                logger.warn("Spark write aborted, cleaned up leftover segment tar at {}", destPath)
              } else {
                // FileSystem.delete returns false when the path doesn't exist. That's not a
                // success signal — it usually means a previous abort already cleaned up, the
                // upload itself never happened, or an out-of-band process moved the file.
                logger.info("Spark write aborted, no leftover segment tar found at {}", destPath)
              }
            } catch {
              case NonFatal(t) =>
                logger.warn("Spark write aborted; failed to clean up leftover segment tar at {}. " +
                  "Manual cleanup may be required before retry to avoid duplicate segments.",
                  destPath, t)
            }
          case None =>
            logger.warn("Spark write aborted, leftover segment tar may remain at {}", destPath)
        }
      case other =>
        logger.warn("Spark write aborted, unknown commit message type: {}", other)
    }
  }
}
