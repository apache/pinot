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
package org.apache.pinot.query.validate;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class SqlNodeCopierTest extends QueryEnvironmentTestBase {

  @DataProvider
  public Object[][] queries() {
    return new Object[][]{
        {"SELECT col1, COUNT(*) FROM a WHERE col2 IN ('x', 'y') AND col3 > 1 GROUP BY col1 HAVING COUNT(*) > 2 "
            + "ORDER BY col1 LIMIT 10"},
        {"SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */ ARRAY_TO_MV(mcol1), COUNT(*) FROM e "
            + "GROUP BY ARRAY_TO_MV(mcol1)"},
        {"SELECT /*+ joinOptions(join_strategy='lookup') */ a.col1, CASE WHEN b.col3 > 1 THEN 'x' ELSE 'y' END "
            + "FROM a JOIN b ON a.col1 = b.col1 WHERE a.col2 = 'z'"},
        {"WITH t AS (SELECT col1, col3 FROM a) SELECT col1, SUM(col3) OVER (PARTITION BY col1 ORDER BY col3) FROM t"},
        {"SELECT * FROM a WHERE col1 IN (SELECT col1 FROM b WHERE col3 = 1) AND CAST(col3 AS BIGINT) > 2"},
        {"SELECT ARRAY_TO_MV(e.mcol1), COUNT(*) FROM e CROSS JOIN UNNEST(e.mcol2) AS u(elem) "
            + "GROUP BY ARRAY_TO_MV(e.mcol1)"},
        {"SELECT col1 FROM a UNION ALL SELECT col1 FROM b"},
        {"SELECT ts_timestamp + INTERVAL '1' DAY FROM a"}
    };
  }

  /// The copy prints as the original, shares no node with it, and stays unchanged and valid after the original is
  /// validated.
  @Test(dataProvider = "queries")
  public void testCopyCanBeValidatedAfterTheOriginal(String query) {
    SqlNodeAndOptions original = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    String parsed = original.getSqlNode().toString();
    SqlNode copy = SqlNodeCopier.copy(original.getSqlNode());
    assertEquals(copy.toString(), parsed);
    Set<SqlNode> originalNodes = nodes(original.getSqlNode());
    for (SqlNode node : nodes(copy)) {
      assertFalse(originalNodes.contains(node), "Shared node: " + node);
    }

    try (QueryEnvironment.CompiledQuery ignored = _queryEnvironment.compile(query, original)) {
      assertEquals(copy.toString(), parsed);
    }
    SqlNodeAndOptions copied = new SqlNodeAndOptions(copy, original.getSqlType(), original.getOptions());
    try (QueryEnvironment.CompiledQuery ignored = _queryEnvironment.compile(query, copied)) {
      // The copy validates and converts as the original does
    }
  }

  private static Set<SqlNode> nodes(SqlNode root) {
    Set<SqlNode> nodes = Collections.newSetFromMap(new IdentityHashMap<>());
    collect(root, nodes);
    return nodes;
  }

  private static void collect(SqlNode node, Set<SqlNode> nodes) {
    if (node == null) {
      return;
    }
    nodes.add(node);
    if (node instanceof SqlCall) {
      for (SqlNode operand : ((SqlCall) node).getOperandList()) {
        collect(operand, nodes);
      }
    } else if (node instanceof SqlNodeList) {
      for (SqlNode element : (SqlNodeList) node) {
        collect(element, nodes);
      }
    }
  }
}
