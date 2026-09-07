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
        {"MODE(stringCol)", "MODE(stringCol, 'MIN', 'STRING')"},
        {"MODE(timestampCol, 'MAX')", "MODE(timestampCol, 'MAX', 'TIMESTAMP')"},
        {"MODE(CONCAT(stringCol, 'suffix'))", "MODE(CONCAT(stringCol, 'suffix'), 'MIN', 'STRING')"},
        {"MODE(JSONEXTRACTSCALAR(stringCol, '$.user', 'STRING', ''))",
            "MODE(JSONEXTRACTSCALAR(stringCol, '$.user', 'STRING', ''), 'MIN', 'STRING')"},
        {"MODE(CAST(intCol AS STRING))", "MODE(CAST(intCol AS STRING), 'MIN', 'STRING')"},
        {"MODE(CAST(stringCol AS TIMESTAMP))", "MODE(CAST(stringCol AS TIMESTAMP), 'MIN', 'TIMESTAMP')"},
        {"MODE(CASE WHEN intCol > 0 THEN stringCol ELSE 'other' END)",
            "MODE(CASE WHEN intCol > 0 THEN stringCol ELSE 'other' END, 'MIN', 'STRING')"},
        {"MODE('literal')", "MODE('literal', 'MIN', 'STRING')"},
        {"fromTimestamp(MODE(timestampCol))", "fromTimestamp(MODE(timestampCol, 'MIN', 'TIMESTAMP'))"}
    };
  }

  @Test(dataProvider = "modeExpressions")
  public void testModeExpressions(String original, String rewritten) {
    for (boolean nullHandlingEnabled : new boolean[]{false, true}) {
      String prefix =
          "SET enableTypedMode=true; SET enableNullHandling=" + nullHandlingEnabled + "; SELECT ";
      TestHelper.assertEqualsQuery(prefix + original + " AS commonValue FROM testTable",
          prefix + rewritten + " AS commonValue FROM testTable", SCHEMA);
    }
  }

  @Test
  public void testModeInHavingAndOrderBy() {
    TestHelper.assertEqualsQuery(
        "SET enableTypedMode=true; "
            + "SELECT intCol, MODE(stringCol) AS commonValue FROM testTable GROUP BY intCol "
            + "HAVING MODE(CASE WHEN stringCol = '' THEN NULL ELSE stringCol END) = 'value' "
            + "ORDER BY MODE(timestampCol) DESC",
        "SET enableTypedMode=true; "
            + "SELECT intCol, MODE(stringCol, 'MIN', 'STRING') AS commonValue FROM testTable GROUP BY intCol "
            + "HAVING MODE(CASE WHEN stringCol = '' THEN NULL ELSE stringCol END, 'MIN', 'STRING') = 'value' "
            + "ORDER BY MODE(timestampCol, 'MIN', 'TIMESTAMP') DESC", SCHEMA);
  }

  @Test
  public void testNumericModeAndOptInRewritesRemainUnchanged() {
    assertUnchanged("SET enableTypedMode=true; SELECT MODE(intCol), MODE(longCol), MODE(floatCol), MODE(doubleCol), "
        + "MODE(CAST(stringCol AS LONG)), MODE(fromDateTime(stringCol, 'yyyy-MM-dd HH:mm:ss')), "
        + "MIN(stringCol), MAX(longCol), SUM(intCol) FROM testTable", SCHEMA);
    assertUnchanged("SET enableTypedMode=false; SELECT MODE(stringCol), MODE(timestampCol) FROM testTable",
        SCHEMA);
    assertUnchanged("SELECT MODE(stringCol), MODE(timestampCol) FROM testTable", SCHEMA);
  }

  @Test
  public void testExistingRewriteOptionPreservesLegacyMode() {
    assertUnchanged("SET autoRewriteAggregationType=true; "
        + "SELECT MODE(timestampCol), MODE(timestampCol, 'AVG'), MODE(stringCol) FROM testTable", SCHEMA);
    assertUnchanged("SET autoRewriteAggregationType=true; SET enableTypedMode=false; "
        + "SELECT MODE(timestampCol), MODE(timestampCol, 'AVG'), MODE(stringCol) FROM testTable", SCHEMA);
    TestHelper.assertEqualsQuery("SET autoRewriteAggregationType=true; SET enableTypedMode=true; "
            + "SELECT MODE(timestampCol), MODE(stringCol), MIN(stringCol) FROM testTable",
        "SET autoRewriteAggregationType=true; SET enableTypedMode=true; "
            + "SELECT MODE(timestampCol, 'MIN', 'TIMESTAMP'), MODE(stringCol, 'MIN', 'STRING'), "
            + "MINSTRING(stringCol) FROM testTable", SCHEMA);
  }

  @Test
  public void testExplicitTypesAndRepeatedOptimization() {
    assertUnchanged("SET enableTypedMode=true; SELECT MODE(stringCol, 'MAX', 'STRING'), "
        + "MODE(timestampCol, 'MIN', 'TIMESTAMP') FROM testTable", SCHEMA);
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SET enableTypedMode=true; SELECT MODE(stringCol), MODE(timestampCol, 'MAX') FROM testTable");
    OPTIMIZER.optimize(query, SCHEMA);
    PinotQuery once = query.deepCopy();
    OPTIMIZER.optimize(query, SCHEMA);
    assertEquals(query, once);
  }

  @Test
  public void testServerDependentModeDoesNotInitializeOnBroker() {
    assertUnchanged("SET enableTypedMode=true; "
        + "SELECT MODE(LOOKUP('baseballTeams', 'teamInteger', 'teamID', stringCol)) FROM testTable", SCHEMA);
  }

  @Test
  public void testMissingSchemaAndColumns() {
    assertUnchanged("SET enableTypedMode=true; SELECT MODE(stringCol) FROM testTable", null);
    assertUnchanged("SET enableTypedMode=true; SELECT MODE(unknownCol) FROM testTable", SCHEMA);
    assertUnchanged("SET enableTypedMode=true; SELECT MODE(CONCAT(unknownCol, 'suffix')) FROM testTable",
        SCHEMA);
    assertUnchanged("SET enableTypedMode=true; SELECT MODE(mvStringCol) FROM testTable", SCHEMA);
  }

  private static void assertUnchanged(String sql, Schema schema) {
    PinotQuery original = CalciteSqlParser.compileToPinotQuery(sql);
    PinotQuery optimized = original.deepCopy();
    OPTIMIZER.optimize(optimized, schema);
    assertEquals(optimized, original);
  }
}
