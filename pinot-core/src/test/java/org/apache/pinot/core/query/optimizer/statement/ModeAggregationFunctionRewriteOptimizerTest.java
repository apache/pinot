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
package org.apache.pinot.core.query.optimizer.statement;

import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Verifies MODE rewriting through the single-stage optimizer, including expressions and post-aggregation clauses.
public class ModeAggregationFunctionRewriteOptimizerTest {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .addSingleValueDimension("stringCol", DataType.STRING)
      .addSingleValueDimension("intCol", DataType.INT)
      .addSingleValueDimension("longCol", DataType.LONG)
      .addSingleValueDimension("floatCol", DataType.FLOAT)
      .addSingleValueDimension("doubleCol", DataType.DOUBLE)
      .addMultiValueDimension("mvStringCol", DataType.STRING)
      .addDateTime("timestampCol", DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
      .build();

  @DataProvider
  public Object[][] modeExpressions() {
    return new Object[][]{
        {"MODE(stringCol)", "modeString(stringCol)"},
        {"MODE(timestampCol, 'MAX')", "modeTimestamp(timestampCol, 'MAX')"},
        {"MODE(CONCAT(stringCol, 'suffix'))", "modeString(CONCAT(stringCol, 'suffix'))"},
        {"MODE(JSONEXTRACTSCALAR(stringCol, '$.user', 'STRING', ''))",
            "modeString(JSONEXTRACTSCALAR(stringCol, '$.user', 'STRING', ''))"},
        {"MODE(CAST(intCol AS STRING))", "modeString(CAST(intCol AS STRING))"},
        {"MODE(CAST(stringCol AS TIMESTAMP))", "modeTimestamp(CAST(stringCol AS TIMESTAMP))"},
        {"MODE(CASE WHEN intCol > 0 THEN stringCol ELSE 'other' END)",
            "modeString(CASE WHEN intCol > 0 THEN stringCol ELSE 'other' END)"},
        {"MODE('literal')", "modeString('literal')"},
        {"fromTimestamp(MODE(timestampCol))", "fromTimestamp(modeTimestamp(timestampCol))"}
    };
  }

  @Test(dataProvider = "modeExpressions")
  public void testModeExpressions(String original, String rewritten) {
    for (boolean nullHandlingEnabled : new boolean[]{false, true}) {
      String prefix =
          "SET autoRewriteAggregationType=true; SET enableNullHandling=" + nullHandlingEnabled + "; SELECT ";
      TestHelper.assertEqualsQuery(prefix + original + " AS commonValue FROM testTable",
          prefix + rewritten + " AS commonValue FROM testTable", SCHEMA);
    }
  }

  @Test
  public void testModeInHavingAndOrderBy() {
    TestHelper.assertEqualsQuery(
        "SET autoRewriteAggregationType=true; "
            + "SELECT intCol, MODE(stringCol) AS commonValue FROM testTable GROUP BY intCol "
            + "HAVING MODE(CASE WHEN stringCol = '' THEN NULL ELSE stringCol END) = 'value' "
            + "ORDER BY MODE(timestampCol) DESC",
        "SET autoRewriteAggregationType=true; "
            + "SELECT intCol, modeString(stringCol) AS commonValue FROM testTable GROUP BY intCol "
            + "HAVING modeString(CASE WHEN stringCol = '' THEN NULL ELSE stringCol END) = 'value' "
            + "ORDER BY modeTimestamp(timestampCol) DESC", SCHEMA);
  }

  @Test
  public void testNumericModeAndOptInRewritesRemainUnchanged() {
    assertUnchanged("SELECT MODE(intCol), MODE(longCol), MODE(floatCol), MODE(doubleCol), "
        + "MODE(CAST(stringCol AS LONG)), MODE(fromDateTime(stringCol, 'yyyy-MM-dd HH:mm:ss')), "
        + "MIN(stringCol), MAX(longCol), SUM(intCol) FROM testTable", SCHEMA);
    assertUnchanged("SET autoRewriteAggregationType=false; SELECT MODE(stringCol), MODE(timestampCol) FROM testTable",
        SCHEMA);
    assertUnchanged("SELECT MODE(stringCol), MODE(timestampCol) FROM testTable", SCHEMA);
  }

  @Test
  public void testServerDependentModeDoesNotInitializeOnBroker() {
    assertUnchanged("SET autoRewriteAggregationType=true; "
        + "SELECT MODE(LOOKUP('baseballTeams', 'teamInteger', 'teamID', stringCol)) FROM testTable", SCHEMA);
  }

  @Test
  public void testMissingSchemaAndColumns() {
    assertUnchanged("SET autoRewriteAggregationType=true; SELECT MODE(stringCol) FROM testTable", null);
    assertUnchanged("SET autoRewriteAggregationType=true; SELECT MODE(unknownCol) FROM testTable", SCHEMA);
    assertUnchanged("SET autoRewriteAggregationType=true; SELECT MODE(CONCAT(unknownCol, 'suffix')) FROM testTable",
        SCHEMA);
    assertUnchanged("SET autoRewriteAggregationType=true; SELECT MODE(mvStringCol) FROM testTable", SCHEMA);
  }

  private static void assertUnchanged(String sql, Schema schema) {
    PinotQuery original = CalciteSqlParser.compileToPinotQuery(sql);
    PinotQuery optimized = original.deepCopy();
    OPTIMIZER.optimize(optimized, schema);
    assertEquals(optimized, original);
  }
}
