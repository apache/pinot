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

import org.apache.pinot.common.datatable.DataTable
import org.apache.pinot.connector.spark.common.partition.{PinotSplit, PinotSplitter}
import org.apache.pinot.connector.spark.common.query.ScanQuery
import org.apache.pinot.connector.spark.common.reader.PinotAbstractPartitionReader
import org.apache.pinot.connector.spark.common.{InstanceInfo, PinotClusterClient, PinotDataSourceReadOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReader,
  PartitionReaderFactory,
  Scan
}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.Map

/**
 * PinotScan implements Spark's 'Scan', which is a logical representation of a scan operation,
 * and also adds the 'Batch' mixin to support batch reads.
 *
 * @param query   An instance of ScanQuery which encapsulates SQL queries to be executed
 * @param schema
 * @param readParameters
 */
class PinotScan(
    query: ScanQuery,
    schema: StructType,
    readParameters: PinotDataSourceReadOptions)
  extends Scan with Batch {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val routingTable = PinotClusterClient.getRoutingTable(readParameters.broker, readParameters.authorization, query)

    val instanceInfo : Map[String, InstanceInfo] = Map()
    val instanceInfoReader = (instance:String) => { // cached reader to reduce network round trips
      instanceInfo.getOrElseUpdate(
        instance,
        PinotClusterClient.getInstanceInfo(readParameters.controller, readParameters.authorization, instance)
      )
    }

    PinotSplitter
      .generatePinotSplits(query, routingTable, instanceInfoReader, readParameters)
      .zipWithIndex
      .map {
        case (pinotSplit, partitionId) =>
          PinotInputPartition(readSchema(), partitionId, pinotSplit, readParameters)
            .asInstanceOf[InputPartition]
      }
      .toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // necessary to make PartitionReaderFactory serializable
    val _schema = this.schema

    (partition: InputPartition) => {
      partition match {
        case p: PinotInputPartition =>
          new PartitionReader[InternalRow] with PinotAbstractPartitionReader[InternalRow] {
            override def _partitionId: Int = p.partitionId
            override def _pinotSplit: PinotSplit = p.pinotSplit
            override def _dataSourceOptions: PinotDataSourceReadOptions = p.dataSourceOptions
            override def _dataExtractor: DataTable => Seq[InternalRow] =
              DataExtractor.pinotDataTableToInternalRows(_, _schema, p.dataSourceOptions.failOnInvalidSegments)
          }
        case _ =>
          throw new Exception("Unknown InputPartition type. Expecting PinotInputPartition")
      }
    }
  }
}
