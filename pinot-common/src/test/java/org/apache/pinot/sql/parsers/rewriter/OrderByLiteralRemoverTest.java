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
package org.apache.pinot.sql.parsers.rewriter;

import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Tests literal removal through the full rewrite pipeline. Test cases have no shared mutable state.
public class OrderByLiteralRemoverTest {
  @DataProvider
  public Object[][] queries() {
    return new Object[][]{
        {"SELECT a FROM t", "SELECT a FROM t"},
        {"SELECT a FROM t ORDER BY a DESC NULLS FIRST", "SELECT a FROM t ORDER BY a DESC NULLS FIRST"},
        {"SELECT a FROM t ORDER BY 'x'", "SELECT a FROM t"},
        {"SELECT a FROM t ORDER BY NULL DESC NULLS FIRST, true, 1.5", "SELECT a FROM t"},
        {"SELECT a FROM t ORDER BY now() DESC", "SELECT a FROM t"},
        {"SELECT a FROM t ORDER BY concat('a', 'b')", "SELECT a FROM t"},
        {"SELECT a, 'x' FROM t ORDER BY 2 DESC", "SELECT a, 'x' FROM t"},
        {"SELECT a, 42 AS constant FROM t ORDER BY 2", "SELECT a, 42 AS constant FROM t"},
        {"SELECT a, 'x' AS constant FROM t ORDER BY constant", "SELECT a, 'x' AS constant FROM t"},
        {"SELECT a, now() AS ts FROM t ORDER BY 2 DESC, ts", "SELECT a, now() AS ts FROM t"},
        {"SELECT a FROM t ORDER BY 'x' DESC, a ASC NULLS FIRST, 'y' ASC",
            "SELECT a FROM t ORDER BY a ASC NULLS FIRST"},
        {"SELECT a, b FROM t ORDER BY 2 DESC, 'x', 1 ASC", "SELECT a, b FROM t ORDER BY b DESC, a ASC"},
        {"SELECT a AS x FROM t ORDER BY 'constant', x DESC", "SELECT a AS x FROM t ORDER BY a DESC"},
        {"SELECT a FROM t ORDER BY 'x', add(a, 1) DESC NULLS LAST",
            "SELECT a FROM t ORDER BY add(a, 1) DESC NULLS LAST"},
        {"SELECT DISTINCT a FROM t ORDER BY 'x', a", "SELECT DISTINCT a FROM t ORDER BY a"},
        {"SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY 'x' DESC, COUNT(*) DESC",
            "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY COUNT(*) DESC"},
        {"SELECT a FROM t GROUP BY a ORDER BY 'x', a", "SELECT a FROM t GROUP BY a ORDER BY a"}
    };
  }

  @Test(dataProvider = "queries")
  public void testRemoveLiterals(String query, String expectedQuery) {
    PinotQuery actual = CalciteSqlParser.compileToPinotQuery(query + " LIMIT 5 OFFSET 3");
    PinotQuery expected = CalciteSqlParser.compileToPinotQuery(expectedQuery + " LIMIT 5 OFFSET 3");
    assertEquals(actual.getOrderByList(), expected.getOrderByList());
    assertEquals(actual.getLimit(), 5);
    assertEquals(actual.getOffset(), 3);
  }

  @Test
  public void testInvalidOrdinalsAreNotIgnored() {
    for (int ordinal : new int[]{0, -1, 2}) {
      assertThrows(SqlCompilationException.class,
          () -> CalciteSqlParser.compileToPinotQuery("SELECT a FROM t ORDER BY " + ordinal));
    }
  }
}
