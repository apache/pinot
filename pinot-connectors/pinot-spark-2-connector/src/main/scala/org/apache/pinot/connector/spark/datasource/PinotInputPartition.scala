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

import org.apache.pinot.common.datatable.DataTable
import org.apache.pinot.connector.spark.common.PinotDataSourceReadOptions
import org.apache.pinot.connector.spark.common.partition.PinotSplit
import org.apache.pinot.connector.spark.common.reader.PinotAbstractPartitionReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

class PinotInputPartition(
    schema: StructType,
    partitionId: Int,
    pinotSplit: PinotSplit,
    dataSourceOptions: PinotDataSourceReadOptions)
  extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new InputPartitionReader[InternalRow] with PinotAbstractPartitionReader[InternalRow]  {
      override def _partitionId: Int = partitionId
      override def _pinotSplit: PinotSplit = pinotSplit
      override def _dataSourceOptions: PinotDataSourceReadOptions = dataSourceOptions
      override def _translator: DataTable => Seq[InternalRow] = TypeConverter.pinotDataTableToInternalRows(_, schema)
    }
  }
}
