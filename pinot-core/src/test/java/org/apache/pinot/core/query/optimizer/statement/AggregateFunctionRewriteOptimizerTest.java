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


/// Covers inferred aggregate arguments and unchanged legacy calls.
public class AggregateFunctionRewriteOptimizerTest {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .addSingleValueDimension("stringCol", DataType.STRING)
      .addSingleValueDimension("longCol", DataType.LONG)
      .addMultiValueDimension("mvStringCol", DataType.STRING)
      .addDateTime("timestampCol", DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
      .build();

  @DataProvider
  public Object[][] modeExpressions() {
    return new Object[][]{
        {"MODE(stringCol)", "MODE(stringCol, 'MIN', 'STRING')"},
        {"MODE(timestampCol, 'MAX')", "MODE(timestampCol, 'MAX', 'TIMESTAMP')"},
        {"MODE(CONCAT(stringCol, 'suffix'))", "MODE(CONCAT(stringCol, 'suffix'), 'MIN', 'STRING')"},
        {"MODE(CASE WHEN stringCol = '' THEN NULL ELSE JSONEXTRACTSCALAR(stringCol, '$.user', 'STRING', '') END)",
            "MODE(CASE WHEN stringCol = '' THEN NULL ELSE JSONEXTRACTSCALAR(stringCol, '$.user', 'STRING', '') END, "
                + "'MIN', 'STRING')"},
        {"MODE(CAST(stringCol AS TIMESTAMP))", "MODE(CAST(stringCol AS TIMESTAMP), 'MIN', 'TIMESTAMP')"},
        {"fromTimestamp(MODE(timestampCol))", "fromTimestamp(MODE(timestampCol, 'MIN', 'TIMESTAMP'))"}
    };
  }

  @Test(dataProvider = "modeExpressions")
  public void testModeExpressions(String original, String rewritten) {
    String prefix = "SELECT ";
    TestHelper.assertEqualsQuery(prefix + original + " FROM testTable", prefix + rewritten + " FROM testTable", SCHEMA);
  }

  @Test
  public void testLegacyCallsAndOtherRewritesAreUnchanged() {
    assertUnchanged("SELECT MODE(longCol, 'AVG'), MIN(stringCol), SUM(longCol), "
        + "MODE(stringCol, 'MAX', 'STRING'), MODE(timestampCol, 'MIN', 'TIMESTAMP') FROM testTable", SCHEMA);
    TestHelper.assertEqualsQuery(
        "SET autoRewriteAggregationType=true; SELECT MODE(stringCol), MIN(stringCol), MAX(longCol), SUM(longCol), "
            + "COUNT(*) FROM testTable",
        "SET autoRewriteAggregationType=true; SELECT MODE(stringCol, 'MIN', 'STRING'), MINSTRING(stringCol), "
            + "MAXLONG(longCol), SUMLONG(longCol), COUNT(*) FROM testTable", SCHEMA);
  }

  @Test
  public void testUnknownTypesDoNotInitializeServerTransforms() {
    assertUnchanged("SELECT MODE(unknownCol), MODE(mvStringCol), MODE(NULLIF(longCol, 0)), "
        + "MODE(LOOKUP('baseballTeams', 'teamInteger', 'teamID', stringCol)) FROM testTable", SCHEMA);
    assertUnchanged("SELECT MODE(stringCol) FROM testTable", null);
  }

  private static void assertUnchanged(String sql, Schema schema) {
    PinotQuery original = CalciteSqlParser.compileToPinotQuery(sql);
    PinotQuery optimized = original.deepCopy();
    OPTIMIZER.optimize(optimized, schema);
    assertEquals(optimized, original);
  }
}
