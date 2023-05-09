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
package org.apache.pinot.connector.spark.common.query

import org.apache.pinot.connector.spark.common.TimeBoundaryInfo
import org.apache.pinot.spi.config.table.TableType

/**
 * Generate realtime and offline SQL queries for specified table with given columns and filters.
 */
private[pinot] class ScanQueryGenerator(
    tableName: String,
    tableType: Option[TableType],
    timeBoundaryInfo: Option[TimeBoundaryInfo],
    columns: Array[String],
    whereClause: Option[String],
    queryOptions: Set[String]) {
  private val columnsExpression = columnsAsExpression()

  def generateSQLs(): ScanQuery = {
    val offlineSelectQuery = buildSelectQuery(TableType.OFFLINE)
    val realtimeSelectQuery = buildSelectQuery(TableType.REALTIME)
    ScanQuery(
      tableName,
      tableType,
      offlineSelectQuery,
      realtimeSelectQuery
    )
  }

  /** Get all columns if selecting columns empty(eg: resultDataFrame.count()) */
  private def columnsAsExpression(): String = {
    if (columns.isEmpty) "*" else columns.map(escapeCol).mkString(",")
  }

  private def escapeCol(col: String): String = {
      if (col.contains("\"")) col else s""""$col""""
  }

  /** Build realtime or offline SQL selection query. */
  private def buildSelectQuery(tableType: TableType): String = {
    def getTimeBoundaryFilter(tbi: TimeBoundaryInfo): String = {
      if (tableType == TableType.OFFLINE) tbi.getOfflinePredicate
      else tbi.getRealtimePredicate
    }

    val tableNameWithType = s"${tableName}_${tableType.toString}"
    val queryBuilder = new StringBuilder()

    // add Query Options and SELECT clause
    queryOptions.foreach(opt => queryBuilder.append(s"SET $opt;"))
    queryBuilder.append(s"SELECT $columnsExpression FROM $tableNameWithType")

    // add where clause if exists
    whereClause.foreach(c => queryBuilder.append(s" WHERE $c"))

    // add time boundary filter if exists
    timeBoundaryInfo.foreach { tbi =>
      val timeBoundaryFilter = getTimeBoundaryFilter(tbi)
      if (whereClause.isEmpty) queryBuilder.append(s" WHERE $timeBoundaryFilter")
      else queryBuilder.append(s" AND $timeBoundaryFilter")
    }

    // query will be converted to Pinot 'BrokerRequest' with SQL compiler
    // pinot set limit to 10 automatically
    // to prevent this add limit to query
    queryBuilder.append(s" LIMIT ${Int.MaxValue}")

    queryBuilder.toString
  }

}

private[pinot] object ScanQueryGenerator {
  def generate(
      tableName: String,
      tableType: Option[TableType],
      timeBoundaryInfo: Option[TimeBoundaryInfo],
      columns: Array[String],
      whereClause: Option[String],
      queryOptions: Set[String]): ScanQuery = {
    new ScanQueryGenerator(tableName, tableType, timeBoundaryInfo, columns, whereClause, queryOptions)
      .generateSQLs()
  }
}
