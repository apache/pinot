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

import org.apache.pinot.connector.spark.common.query.ScanQueryGenerator
import org.apache.pinot.connector.spark.common.{PinotClusterClient, PinotDataSourceReadOptions}
import org.apache.pinot.connector.spark.v3.datasource.query.FilterPushDown
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * PinotScanBuilder: Implementation of Spark DataSourceV2 API's ScanBuilder interface.
 * This is the main class in which various push down functionality is implemented.
 * We currently support pushing down Filters (where clause) and RequiredColumns (selection).
 *
 * @param readParameters PinotDataSourceReadOptions instance for the read
 */
class PinotScanBuilder(readParameters: PinotDataSourceReadOptions)
  extends ScanBuilder with SupportsPushDownFilters with SupportsPushDownRequiredColumns {

  private var acceptedFilters: Array[Filter] = Array.empty
  private var currentSchema: StructType = _

  override def build(): Scan = {
    // Time boundary is used when table is hybrid to ensure that the overlap
    // between realtime and offline segment data is queried exactly once
    val timeBoundaryInfo =
      if (readParameters.tableType.isDefined) {
        None
      } else {
        PinotClusterClient.getTimeBoundaryInfo(readParameters.broker, readParameters.tableName)
      }

    val whereCondition = FilterPushDown.compileFiltersToSqlWhereClause(this.acceptedFilters)
    val scanQuery = ScanQueryGenerator.generate(
      readParameters.tableName,
      readParameters.tableType,
      timeBoundaryInfo,
      currentSchema.fieldNames,
      whereCondition,
      readParameters.queryOptions
    )

    new PinotScan(scanQuery, currentSchema, readParameters)
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

  override def pushedFilters(): Array[Filter] = {
    this.acceptedFilters
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.currentSchema = requiredSchema
  }

}
