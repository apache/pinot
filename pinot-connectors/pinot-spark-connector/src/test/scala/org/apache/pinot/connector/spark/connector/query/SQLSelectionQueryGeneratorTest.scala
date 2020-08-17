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
package org.apache.pinot.connector.spark.connector.query

import org.apache.pinot.connector.spark.BaseTest
import org.apache.pinot.connector.spark.connector.TimeBoundaryInfo
import org.apache.pinot.spi.config.table.TableType

/**
 * Test SQL query generation from spark push down filters, selection columns etc.
 */
class SQLSelectionQueryGeneratorTest extends BaseTest {
  private val columns = Array("c1, c2")
  private val tableName = "tbl"
  private val tableType = Some(TableType.OFFLINE)
  private val whereClause = Some("c1 = 5 OR c2 = 'hello'")
  private val limit = s"LIMIT ${Int.MaxValue}"

  test("Queries should be created with given filters") {
    val pinotQueries =
      SQLSelectionQueryGenerator.generate(tableName, tableType, None, columns, whereClause)
    val expectedRealtimeQuery =
      s"SELECT c1, c2 FROM ${tableName}_REALTIME WHERE ${whereClause.get} $limit"
    val expectedOfflineQuery =
      s"SELECT c1, c2 FROM ${tableName}_OFFLINE WHERE ${whereClause.get} $limit"

    pinotQueries.realtimeSelectQuery shouldEqual expectedRealtimeQuery
    pinotQueries.offlineSelectQuery shouldEqual expectedOfflineQuery
  }

  test("Time boundary info should be added to existing where clause") {
    val timeBoundaryInfo = TimeBoundaryInfo("timeCol", "12345")
    val pinotQueries = SQLSelectionQueryGenerator
      .generate(tableName, tableType, Some(timeBoundaryInfo), columns, whereClause)

    val realtimeWhereClause = s"${whereClause.get} AND timeCol >= 12345"
    val offlineWhereClause = s"${whereClause.get} AND timeCol < 12345"
    val expectedRealtimeQuery =
      s"SELECT c1, c2 FROM ${tableName}_REALTIME WHERE $realtimeWhereClause $limit"
    val expectedOfflineQuery =
      s"SELECT c1, c2 FROM ${tableName}_OFFLINE WHERE $offlineWhereClause $limit"

    pinotQueries.realtimeSelectQuery shouldEqual expectedRealtimeQuery
    pinotQueries.offlineSelectQuery shouldEqual expectedOfflineQuery
  }

  test("Time boundary info should be added to where clause") {
    val timeBoundaryInfo = TimeBoundaryInfo("timeCol", "12345")
    val pinotQueries = SQLSelectionQueryGenerator
      .generate(tableName, tableType, Some(timeBoundaryInfo), columns, None)

    val realtimeWhereClause = s"timeCol >= 12345"
    val offlineWhereClause = s"timeCol < 12345"
    val expectedRealtimeQuery =
      s"SELECT c1, c2 FROM ${tableName}_REALTIME WHERE $realtimeWhereClause $limit"
    val expectedOfflineQuery =
      s"SELECT c1, c2 FROM ${tableName}_OFFLINE WHERE $offlineWhereClause $limit"

    pinotQueries.realtimeSelectQuery shouldEqual expectedRealtimeQuery
    pinotQueries.offlineSelectQuery shouldEqual expectedOfflineQuery
  }

  test("Selection query should be created with '*' column expressions without filters") {
    val pinotQueries = SQLSelectionQueryGenerator
      .generate(tableName, tableType, None, Array.empty, None)

    val expectedRealtimeQuery =
      s"SELECT * FROM ${tableName}_REALTIME $limit"
    val expectedOfflineQuery =
      s"SELECT * FROM ${tableName}_OFFLINE $limit"

    pinotQueries.realtimeSelectQuery shouldEqual expectedRealtimeQuery
    pinotQueries.offlineSelectQuery shouldEqual expectedOfflineQuery
  }

}
