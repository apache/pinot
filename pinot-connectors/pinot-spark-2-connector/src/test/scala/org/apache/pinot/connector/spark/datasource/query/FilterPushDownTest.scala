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
package org.apache.pinot.connector.spark.datasource.query

import java.sql.{Date, Timestamp}

import org.apache.pinot.connector.spark.datasource.BaseTest
import org.apache.spark.sql.sources._

/**
 * Test filter conversions => Spark filter to SQL where clause
 */
class FilterPushDownTest extends BaseTest {

  private val filters: Array[Filter] = Array(
    EqualTo("attr1", 1),
    In("attr2", Array("1", "2", "'5'")),
    LessThan("attr3", 1),
    LessThanOrEqual("attr4", 3),
    GreaterThan("attr5", 10),
    GreaterThanOrEqual("attr6", 15),
    Not(EqualTo("attr7", "1")),
    And(LessThan("attr8", 10), LessThanOrEqual("attr9", 3)),
    Or(EqualTo("attr10", "hello"), GreaterThanOrEqual("attr11", 13)),
    StringContains("attr12", "pinot"),
    In("attr13", Array(10, 20)),
    EqualNullSafe("attr20", "123"),
    IsNull("attr14"),
    IsNotNull("attr15"),
    StringStartsWith("attr16", "pinot1"),
    StringEndsWith("attr17", "pinot2"),
    EqualTo("attr18", Timestamp.valueOf("2020-01-01 00:00:15")),
    LessThan("attr19", Date.valueOf("2020-01-01")),
    EqualTo("attr21", Seq(1, 2)),
    EqualTo("attr22", 10.5d)
  )

  test("Unsupported filters should be filtered") {
    val (accepted, postScan) = FilterPushDown.acceptFilters(filters)

    accepted should contain theSameElementsAs filters
    postScan should contain theSameElementsAs Seq.empty
  }

  test("SQL query should be created from spark filters") {
    val whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters)
    val expectedOutput =
      s"""("attr1" = 1) AND ("attr2" IN ('1', '2', '''5''')) AND ("attr3" < 1) AND ("attr4" <= 3) AND ("attr5" > 10) AND """ +
        s"""("attr6" >= 15) AND (NOT ("attr7" = '1')) AND (("attr8" < 10) AND ("attr9" <= 3)) AND """ +
        s"""(("attr10" = 'hello') OR ("attr11" >= 13)) AND ("attr12" LIKE '%pinot%') AND ("attr13" IN (10, 20)) AND """ +
        s"""(NOT ("attr20" != '123' OR "attr20" IS NULL OR '123' IS NULL) OR ("attr20" IS NULL AND '123' IS NULL)) AND """ +
        s"""("attr14" IS NULL) AND ("attr15" IS NOT NULL) AND ("attr16" LIKE 'pinot1%') AND ("attr17" LIKE '%pinot2') AND """ +
        s"""("attr18" = '2020-01-01 00:00:15.0') AND ("attr19" < '2020-01-01') AND ("attr21" = List(1, 2)) AND """ +
        s"""("attr22" = 10.5)"""

    whereClause.get shouldEqual expectedOutput
  }

  test("Shouldn't escape column names which are already escaped") {
    val whereClause = FilterPushDown.compileFiltersToSqlWhereClause(
      Array(EqualTo("\"some\".\"nested\".\"column\"", 1)))
    val expectedOutput = "(\"some\".\"nested\".\"column\" = 1)"

    whereClause.get shouldEqual expectedOutput
  }
}
