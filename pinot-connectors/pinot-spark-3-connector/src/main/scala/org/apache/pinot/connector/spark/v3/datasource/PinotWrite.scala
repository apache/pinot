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

import org.apache.pinot.connector.spark.common.PinotDataSourceWriteOptions
import org.apache.pinot.spi.data.Schema
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, Write, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class PinotWrite(
                  logicalWriteInfo: LogicalWriteInfo
                ) extends Write with BatchWrite {
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
    messages.foreach(println)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    messages.foreach(println)
  }
}
