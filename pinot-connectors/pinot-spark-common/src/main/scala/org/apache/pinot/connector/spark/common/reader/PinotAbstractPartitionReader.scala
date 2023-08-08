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
package org.apache.pinot.connector.spark.common.reader

import org.apache.pinot.common.datatable.DataTable
import org.apache.pinot.connector.spark.common.PinotDataSourceReadOptions
import org.apache.pinot.connector.spark.common.partition.PinotSplit

import java.io.Closeable

/**
 * Abstract partition reader is designed to be shared between two concrete reader implementations
 * for Spark2 and Spark3 connectors.
 *
 * @tparam RowType
 */
trait PinotAbstractPartitionReader[RowType] {
  def _partitionId: Int
  def _pinotSplit: PinotSplit
  def _dataSourceOptions: PinotDataSourceReadOptions
  def _translator: (DataTable) => Seq[RowType]

  private val (responseIterator: Iterator[RowType], source: Closeable) = getIteratorAndSource()
  private[this] var currentRow: RowType = _

  def next(): Boolean = {
    if (!responseIterator.hasNext) {
      return false
    }
    currentRow = responseIterator.next()
    true
  }

  def get(): RowType = {
    currentRow
  }

  def close(): Unit = {
    source.close()
  }

  private def getIteratorAndSource(): (Iterator[RowType], Closeable) = {
    if (_dataSourceOptions.useGrpcServer) {
      val dataFetcher = PinotGrpcServerDataFetcher(_pinotSplit)
      val iterable = dataFetcher.fetchData()
        .flatMap(_translator)
      (iterable, dataFetcher)
    } else {
      (PinotServerDataFetcher(_partitionId, _pinotSplit, _dataSourceOptions)
        .fetchData()
        .flatMap(_translator)
        .toIterator,
        () => {})
    }
  }
}
