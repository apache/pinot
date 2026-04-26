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
package org.apache.pinot.connector.spark.v4.datasource.query

import java.sql.{Date, Timestamp}

import org.apache.pinot.connector.spark.v4.datasource.BaseTest
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
        s"""(("attr10" = 'hello') OR ("attr11" >= 13)) AND ("attr12" LIKE '%pinot%' ESCAPE '\\') AND ("attr13" IN (10, 20)) AND """ +
        s"""(NOT ("attr20" != '123' OR "attr20" IS NULL OR '123' IS NULL) OR ("attr20" IS NULL AND '123' IS NULL)) AND """ +
        s"""("attr14" IS NULL) AND ("attr15" IS NOT NULL) AND ("attr16" LIKE 'pinot1%' ESCAPE '\\') AND ("attr17" LIKE '%pinot2' ESCAPE '\\') AND """ +
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

  test("Compound filter with unsupported child should be rejected by acceptFilters") {
    // The connector does not recognize AlwaysTrue as a pushable leaf, so wrapping it in
    // And/Or/Not should fall through to postScan. Before this fix the compound was accepted
    // even though compileFilter silently returned None, which caused Pinot to return
    // unfiltered rows.
    val unsupportedLeaf = AlwaysTrue
    val orWithUnsupported = Or(EqualTo("a", 1), unsupportedLeaf)
    val andWithUnsupported = And(EqualTo("a", 1), unsupportedLeaf)
    val notWithUnsupported = Not(unsupportedLeaf)

    val (accepted, postScan) =
      FilterPushDown.acceptFilters(Array(orWithUnsupported, andWithUnsupported, notWithUnsupported))
    accepted shouldBe empty
    postScan should contain theSameElementsAs
      Seq(orWithUnsupported, andWithUnsupported, notWithUnsupported)
  }

  test("Compound filter whose children are all supported should still be accepted") {
    val andOk = And(EqualTo("a", 1), LessThan("b", 10))
    val orOk = Or(EqualTo("a", 1), GreaterThanOrEqual("b", 10))
    val notOk = Not(EqualTo("a", 1))

    val (accepted, postScan) = FilterPushDown.acceptFilters(Array(andOk, orOk, notOk))
    accepted should contain theSameElementsAs Seq(andOk, orOk, notOk)
    postScan shouldBe empty
  }

  test("LIKE pushdowns should escape SQL and LIKE wildcard characters in the value") {
    // Literal %, _, and single-quote in the user-supplied string would change the Pinot
    // predicate meaning (or break the SQL) if they leaked through unescaped.
    val filters = Array[Filter](
      StringStartsWith("name", "50%_off'sale"),
      StringEndsWith("name", "10%_off'sale"),
      StringContains("name", "%_'")
    )
    val whereClause = FilterPushDown.compileFiltersToSqlWhereClause(filters)
    whereClause.get shouldEqual
      """("name" LIKE '50\%\_off''sale%' ESCAPE '\') AND """ +
        """("name" LIKE '%10\%\_off''sale' ESCAPE '\') AND """ +
        """("name" LIKE '%\%\_''%' ESCAPE '\')"""
  }

  test("LIKE pushdowns with backslash in the value should fall back to post-scan") {
    // Pinot's likeToRegexpLike does not round-trip `\\` correctly (it emits a regex that
    // matches two backslashes instead of one), so the connector rejects pushdown for these
    // and lets Spark evaluate them on the driver side. Once the runtime conversion is fixed,
    // this test should be revisited to push these down again.
    val filters = Array[Filter](
      StringStartsWith("name", "a\\b"),
      StringEndsWith("name", "x\\y"),
      StringContains("name", "p\\q")
    )
    val (accepted, postScan) = FilterPushDown.acceptFilters(filters)
    accepted shouldBe empty
    postScan should contain theSameElementsAs filters
  }

  test("EqualNullSafe with a null value falls back to post-scan") {
    // Pushing this down would render the literal SQL `attr != null` (Pinot's `compileValue`
    // null branch falls through to `value.toString`), which Pinot would parse syntactically
    // rather than as a NULL test. Spark must evaluate post-scan so three-valued logic is
    // honored.
    val f = EqualNullSafe("name", null)
    val (accepted, postScan) = FilterPushDown.acceptFilters(Array(f))
    accepted shouldBe empty
    postScan should contain only f
  }

  test("IN with a null array element falls back to post-scan") {
    // Same problem as EqualNullSafe(_, null) but for IN: a null entry would render as
    // `IN (1, null, 3)` and break Spark's null semantics. An all-non-null IN is fine.
    val withNull = In("attr", Array[Any]("a", null, "b"))
    val withoutNull = In("attr", Array[Any]("a", "b", "c"))
    val (accepted, postScan) =
      FilterPushDown.acceptFilters(Array[Filter](withNull, withoutNull))
    accepted should contain only withoutNull
    postScan should contain only withNull
  }

  test("escapeAttr properly quotes a column name containing a stray double-quote") {
    val whereClause = FilterPushDown.compileFiltersToSqlWhereClause(
      Array(EqualTo("weird\"col", 1)))
    // Inner `"` doubled, outer wrapping quotes preserved → single well-formed quoted
    // identifier. Without the fix, the result was `weird"col = 1` (raw, broken SQL).
    whereClause.get shouldEqual "(\"weird\"\"col\" = 1)"
  }
}
