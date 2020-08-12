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

import org.apache.pinot.connector.spark.connector._
import org.apache.pinot.connector.spark.connector.query.SQLSelectionQueryGenerator
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

/**
 * Spark-Pinot datasource reader to read metadata and create partition splits.
 */
class PinotDataSourceReader(options: DataSourceOptions, userSchema: Option[StructType] = None)
  extends DataSourceReader
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private val readParameters = PinotDataSourceReadOptions.from(options)
  private var acceptedFilters: Array[Filter] = Array.empty
  private var currentSchema: StructType = _

  override def readSchema(): StructType = {
    if (currentSchema == null) {
      currentSchema = userSchema.getOrElse {
        val pinotTableSchema =
          PinotClusterClient.getTableSchema(readParameters.controller, readParameters.tableName)
        PinotUtils.pinotSchemaToSparkSchema(pinotTableSchema)
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
        PinotClusterClient.getTimeBoundaryInfo(readParameters.broker, readParameters.tableName)
      }

    val whereCondition = FilterPushDown.compileFiltersToSqlWhereClause(this.acceptedFilters)
    val generatedSQLs = SQLSelectionQueryGenerator.generate(
      readParameters.tableName,
      readParameters.tableType,
      timeBoundaryInfo,
      schema.fieldNames,
      whereCondition
    )

    val routingTable = PinotClusterClient.getRoutingTable(readParameters.broker, generatedSQLs)

    PinotSplitter
      .generatePinotSplits(generatedSQLs, routingTable, readParameters.segmentsPerSplit)
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
