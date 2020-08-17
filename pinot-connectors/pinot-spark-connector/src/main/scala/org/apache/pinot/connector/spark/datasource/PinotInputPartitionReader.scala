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
package org.apache.pinot.connector.spark.datasource

import org.apache.pinot.connector.spark.connector.{PinotServerDataFetcher, PinotSplit, PinotUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType

/**
 * Actual data reader on spark worker side.
 * Represents a spark partition, and is receive data from specified pinot server-segment list.
 */
class PinotInputPartitionReader(
    schema: StructType,
    partitionId: Int,
    pinotSplit: PinotSplit,
    dataSourceOptions: PinotDataSourceReadOptions)
  extends InputPartitionReader[InternalRow] {
  private val responseIterator: Iterator[InternalRow] = fetchDataAndConvertToInternalRows()
  private[this] var currentRow: InternalRow = _

  override def next(): Boolean = {
    if (!responseIterator.hasNext) {
      return false
    }
    currentRow = responseIterator.next()
    true
  }

  override def get(): InternalRow = {
    currentRow
  }

  override def close(): Unit = {}

  private def fetchDataAndConvertToInternalRows(): Iterator[InternalRow] = {
    PinotServerDataFetcher(partitionId, pinotSplit, dataSourceOptions)
      .fetchData()
      .flatMap(PinotUtils.pinotDataTableToInternalRows(_, schema))
      .toIterator
  }
}
