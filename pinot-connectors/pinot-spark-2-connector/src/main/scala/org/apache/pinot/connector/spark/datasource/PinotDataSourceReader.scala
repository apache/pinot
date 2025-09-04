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

import java.util.{List => JList}

import org.apache.pinot.connector.spark.common.{InstanceInfo, PinotClusterClient, PinotDataSourceReadOptions}
import org.apache.pinot.connector.spark.common.query.ScanQueryGenerator
import org.apache.pinot.connector.spark.common.partition.PinotSplitter
import org.apache.pinot.connector.spark.datasource.query.FilterPushDown
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{
  DataSourceReader,
  InputPartition,
  SupportsPushDownFilters,
  SupportsPushDownRequiredColumns
}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable.Map

/**
 * Spark-Pinot datasource reader to read metadata and create partition splits.
 */
class PinotDataSourceReader(options: DataSourceOptions, userSchema: Option[StructType] = None)
  extends DataSourceReader
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private val readParameters = PinotDataSourceReadOptions.from(options.asMap())
  private var acceptedFilters: Array[Filter] = Array.empty
  private var currentSchema: StructType = _

  override def readSchema(): StructType = {
    if (currentSchema == null) {
      currentSchema = userSchema.getOrElse {
        val pinotTableSchema =
          PinotClusterClient.getTableSchema(
            readParameters.controller,
            readParameters.tableName,
            readParameters.authorization
          )
        DataExtractor.pinotSchemaToSparkSchema(pinotTableSchema)
      }
    }
    currentSchema
  }

  override def planInputPartitions(): JList[InputPartition[InternalRow]] = {
    val schema = readSchema()
    // Time boundary is used when table is hybrid to ensure that the overlap
    // between realtime and offline segment data is queried exactly once
    val timeBoundaryInfo =
      if (readParameters.tableType.isDefined) {
        None
      } else {
        PinotClusterClient.getTimeBoundaryInfo(readParameters.broker, readParameters.tableName, readParameters.authorization)
      }

    val whereCondition = FilterPushDown.compileFiltersToSqlWhereClause(this.acceptedFilters)
    val scanQuery = ScanQueryGenerator.generate(
      readParameters.tableName,
      readParameters.tableType,
      timeBoundaryInfo,
      schema.fieldNames,
      whereCondition,
      readParameters.queryOptions
    )

    val routingTable = PinotClusterClient.getRoutingTable(readParameters.broker, readParameters.authorization, scanQuery)

    val instanceInfo : Map[String, InstanceInfo] = Map()
    val instanceInfoReader = (instance:String) => { // cached reader to reduce network round trips
      instanceInfo.getOrElseUpdate(
        instance,
        PinotClusterClient.getInstanceInfo(readParameters.controller, readParameters.authorization, instance)
      )
    }

    PinotSplitter
      .generatePinotSplits(scanQuery, routingTable, instanceInfoReader, readParameters)
      .zipWithIndex
      .map {
        case (pinotSplit, partitionId) =>
          new PinotInputPartition(schema, partitionId, pinotSplit, readParameters)
            .asInstanceOf[InputPartition[InternalRow]]
      }
      .asJava
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.currentSchema = requiredSchema
  }

  override def pushedFilters(): Array[Filter] = {
    this.acceptedFilters
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (readParameters.usePushDownFilters) {
      val (acceptedFilters, postScanFilters) = FilterPushDown.acceptFilters(filters)
      this.acceptedFilters = acceptedFilters
      postScanFilters
    } else {
      filters
    }
  }
}
