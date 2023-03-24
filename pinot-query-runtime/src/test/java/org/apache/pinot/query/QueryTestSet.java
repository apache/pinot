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
package org.apache.pinot.query;

import org.testng.annotations.DataProvider;


/**
 * all legacy test query sets.
 *
 * @deprecated do not add to this test set. this class will be broken down and clean up.
 * add your test to appropriate files in {@link org.apache.pinot.query.runtime.queries} instead.
 */
public class QueryTestSet {

  @DataProvider(name = "testSql")
  public Object[][] provideTestSql() {
    return new Object[][]{
        // Order BY LIMIT
        new Object[]{"SELECT * FROM b ORDER BY col1, col2 DESC LIMIT 3"},
        new Object[]{"SELECT * FROM a ORDER BY col1, ts LIMIT 10"},
        new Object[]{"SELECT * FROM a ORDER BY col1 LIMIT 20"},
        new Object[]{"SELECT * FROM a ORDER BY col1, ts LIMIT 1, 2"},
        new Object[]{"SELECT * FROM a ORDER BY col1, ts LIMIT 2 OFFSET 1"},

        // No match filter
        new Object[]{"SELECT * FROM b WHERE col3 < 0.5"},

        // Hybrid table
        new Object[]{"SELECT * FROM d"},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // thus the final JOIN result will be 15 x 1 = 15.
        // Next join with table C which has (5 on server1 and 10 on server2), since data is identical. each of the row
        // of the A JOIN B will have identical value of col3 as table C.col3 has. Since the values are cycling between
        // (1, 42, 1, 42, 1). we will have 9 1s, and 6 42s, total result count will be 9 * 9 + 6 * 6 = 117
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col1 JOIN c ON a.col3 = c.col3"},
        // Reverse the order of join condition and join table order.
        new Object[]{"SELECT * FROM a JOIN b ON b.col1 = a.col1 JOIN c ON c.col3 = a.col3"},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // thus the final JOIN result will be 15 x 1 = 15.
        new Object[]{"SELECT * FROM a JOIN b on a.col1 = b.col1"},

        // Query with function in JOIN keys, table A and B are both (1, 42, 1, 42, 1), with table A cycling 3 times.
        // Because:
        //   - MOD(a.col3, 2) will have 6 (42)s equal to 0 and 9 (1)s equals to 1
        //   - MOD(b.col3, 3) will have 2 (42)s equal to 0 and 3 (1)s equals to 1;
        // final results are 6 * 2 + 9 * 3 = 39 rows
        new Object[]{"SELECT a.col1, a.col3, b.col3 FROM a JOIN b ON MOD(a.col3, 2) = MOD(b.col3, 3)"},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // thus the final JOIN result will be 15 x 1 = 15.
        new Object[]{"SELECT * FROM a JOIN b on a.col1 = b.col1 AND a.col2 = b.col2"},
        // Reverse the order of join condition and join table order.
        new Object[]{"SELECT * FROM a JOIN b on b.col1 = a.col1 AND b.col2 = a.col2"},

        // LEFT JOIN
        new Object[]{"SELECT * FROM a LEFT JOIN b on a.col1 = b.col2"},

        new Object[]{"SELECT a.col1, SUM(CASE WHEN b.col3 IS NULL THEN 0 ELSE b.col3 END) "
            + " FROM a LEFT JOIN b on a.col1 = b.col2 GROUP BY a.col1"},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // but only 1 out of 5 rows from table A will be selected out; and all in table B will be selected.
        // thus the final JOIN result will be 1 x 3 x 1 = 3.
        new Object[]{"SELECT a.col1, a.ts, b.col2, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND a.col2 = 'alice' AND b.col3 >= 0"},

        // Join query with IN and Not-IN clause. Table A's side of join will return 9 rows and Table B's side will
        // return 2 rows. Join will be only on col1=bar and since A will return 3 rows with that value and B will return
        // 1 row, the final output will have 3 rows.
        new Object[]{"SELECT a.col1, b.col2 FROM a JOIN b ON a.col1 = b.col1 "
            + " WHERE a.col1 IN ('foo', 'bar', 'alice') AND b.col2 NOT IN ('foo', 'alice')"},

        // Same query as above but written using OR/AND instead of IN.
        new Object[]{"SELECT a.col1, b.col2 FROM a JOIN b ON a.col1 = b.col1 "
            + " WHERE (a.col1 = 'foo' OR a.col1 = 'bar' OR a.col1 = 'alice') AND b.col2 != 'foo'"
            + " AND b.col2 != 'alice'"},

        // Same as above but with single argument IN clauses. Left side of the join returns 3 rows, and the right side
        // returns 5 rows. Only key where join succeeds is col1=foo, and since table B has only 1 row with that value,
        // the number of rows should be 3.
        new Object[]{"SELECT a.col1, b.col2 FROM a JOIN b ON a.col1 = b.col1 "
            + " WHERE a.col1 IN ('foo') AND b.col2 NOT IN ('')"},

        // Range conditions with continuous and non-continuous range.
        // Calcite default compilation will convert multiple `=` and `<>` into range IN and NOT IN search predicates.
        new Object[]{"SELECT a.col1, SUM(CASE WHEN a.col2 = 'foo' OR a.col2 = 'alice' THEN 1 ELSE 0 END) AS match_sum, "
            + " SUM(CASE WHEN a.col2 <> 'foo' AND a.col2 <> 'alice' THEN 1 ELSE 0 END) as unmatch_sum "
            + " FROM a WHERE a.ts >= 1600000000 GROUP BY a.col1"},

        new Object[]{"SELECT a.col1, b.col2 FROM a JOIN b ON a.col1 = b.col1 "
            + " WHERE a.col3 IN (1, 2, 3) OR (a.col3 > 10 AND a.col3 < 50)"},

        new Object[]{"SELECT col1, SUM(col3) FROM a WHERE a.col3 BETWEEN 23 AND 36 "
            + " GROUP BY col1 HAVING SUM(col3) > 10.0 AND MIN(col3) <> 123 AND MAX(col3) BETWEEN 10 AND 20"},

        new Object[]{"SELECT col1, SUM(col3) FROM a WHERE (col3 > 0 AND col3 < 45) AND (col3 > 15 AND col3 < 50) "
            + " GROUP BY col1 HAVING (SUM(col3) > 10 AND SUM(col3) < 20) AND (SUM(col3) > 30 AND SUM(col3) < 40)"},

        // Projection pushdown
        new Object[]{"SELECT a.col1, a.col3 + a.col3 FROM a WHERE a.col3 >= 0 AND a.col2 = 'alice'"},

        // Inequality JOIN & partial filter pushdown
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2 WHERE a.col3 >= 0 AND a.col3 > b.col3"},
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2 WHERE a.col3 >= 0 AND "
            + "((a.col1 <> 'foo' AND b.col2 <> 'bar') or (a.col1 <> 'bar' AND b.col2 <> 'foo'))"},

        new Object[]{"SELECT * FROM a, b WHERE a.col1 > b.col2 AND a.col3 > b.col3"},

        // Aggregation with group by
        new Object[]{"SELECT a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 GROUP BY a.col1"},

        // Aggregation with multiple group key
        new Object[]{"SELECT a.col2, a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 GROUP BY a.col1, a.col2"},

        // Aggregation without GROUP BY
        new Object[]{"SELECT SUM(col3) FROM a WHERE a.col3 >= 0 AND a.col2 = 'alice'"},

        // Aggregation with GROUP BY on a count star reference
        new Object[]{"SELECT a.col1, COUNT(*) FROM a WHERE a.col3 >= 0 GROUP BY a.col1"},

        // project in intermediate stage
        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // col1 on both are "foo", "bar", "alice", "bob", "charlie"
        // col2 on both are "foo", "bar", "alice", "foo", "bar",
        //   filtered at :    ^                      ^
        // thus the final JOIN result will have 6 rows: 3 "foo" <-> "foo"; and 3 "bob" <-> "bob"
        new Object[]{"SELECT a.col1, a.col2, a.ts, b.col1, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND a.col2 = 'foo' AND b.col3 >= 0"},

        // Making transform after JOIN, number of rows should be the same as JOIN result.
        new Object[]{"SELECT a.col1, a.ts, a.col3 - b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND b.col3 >= 0"},

        // Making transform after GROUP-BY, number of rows should be the same as GROUP-BY result.
        new Object[]{"SELECT a.col1, a.col2, SUM(a.col3) - MIN(a.col3) FROM a"
            + " WHERE a.col3 >= 0 GROUP BY a.col1, a.col2"},

        // GROUP BY after JOIN
        //   - optimizable transport for GROUP BY key after JOIN, using SINGLETON exchange
        //     only 3 GROUP BY key exist because b.col2 cycles between "foo", "bar", "alice".
        new Object[]{"SELECT a.col1, SUM(b.col3), COUNT(*), SUM(2) FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 GROUP BY a.col1"},
        //   - non-optimizable transport for GROUP BY key after JOIN, using HASH exchange
        //     only 2 GROUP BY key exist for b.col3.
        new Object[]{"SELECT b.col3, SUM(a.col3) FROM a JOIN b"
            + " on a.col1 = b.col1 AND a.col2 = b.col2 GROUP BY b.col3"},

        // Sub-query
        new Object[]{"SELECT b.col1, b.col3, i.maxVal FROM b JOIN "
            + "  (SELECT a.col2 AS joinKey, MAX(a.col3) AS maxVal FROM a GROUP BY a.col2) AS i "
            + "  ON b.col1 = i.joinKey"},

        // Sub-query with predicate clause to SEMI JOIN.
        new Object[]{"SELECT * FROM b WHERE b.col1 IN (SELECT a.col2 FROM a)"},
        new Object[]{"SELECT b.col1, b.col2, SUM(b.col3) * 100 / COUNT(b.col3) FROM b WHERE b.col1 IN "
            + " (SELECT a.col2 FROM a WHERE a.col2 != 'foo') GROUP BY b.col1, b.col2"},
        new Object[]{"SELECT SUM(b.col3) FROM b WHERE b.col3 > (SELECT AVG(a.col3) FROM a WHERE a.col2 != 'bar')"},

        // Aggregate query with HAVING clause, "foo" and "bar" occurred 6/2 times each and "alice" occurred 3/1 times
        // numbers are cycle in (1, 42, 1, 42, 1), and (foo, bar, alice, foo, bar)
        // - COUNT(*) < 5 matches "alice" (3 times)
        // - COUNT(*) > 5 matches "foo" and "bar" (6 times); so both will be selected out SUM(a.col3) = (1 + 42) * 3
        // - last condition doesn't match anything.
        // total to 3 rows.
        new Object[]{"SELECT a.col2, COUNT(*), MAX(a.col3), MIN(a.col3), SUM(a.col3) FROM a GROUP BY a.col2 "
            + "HAVING COUNT(*) < 5 OR (COUNT(*) > 5 AND SUM(a.col3) >= 10)"
            + "OR (MIN(a.col3) != 20 AND SUM(a.col3) = 100)"},
        new Object[]{"SELECT COUNT(*) AS Count, MAX(a.col3) AS \"max\" FROM a GROUP BY a.col2 "
            + "HAVING Count > 1 AND \"max\" < 50"},

        // Order-by
        new Object[]{"SELECT a.col1, a.col3, b.col3 FROM a JOIN b ON a.col1 = b.col1 ORDER BY a.col3, b.col3 DESC"},
        new Object[]{"SELECT MAX(a.col3) FROM a GROUP BY a.col2 ORDER BY MAX(a.col3) - MIN(a.col3)"},

        // Test CAST
        //   - implicit CAST
        new Object[]{"SELECT a.col1, a.col2, AVG(a.col3) FROM a GROUP BY a.col1, a.col2"},
        new Object[]{"SELECT a.col1 FROM a WHERE a.col3 >= 0.5 AND a.col3 < 0.7 OR a.col3 = 42.0"},
        new Object[]{"SELECT a.col1, SUM(a.col3) FROM a GROUP BY a.col1 "
            + " HAVING MIN(a.col3) > 0.5 AND MIN(a.col3) <> 0.7 OR MIN(a.col3) > 30"},
        //   - explicit CAST
        new Object[]{"SELECT a.col1, CAST(SUM(a.col3) AS BIGINT) FROM a GROUP BY a.col1"},

        // Test DISTINCT
        //   - distinct value done via GROUP BY with empty expr aggregation list.
        new Object[]{"SELECT a.col2, a.col3 FROM a JOIN b ON a.col1 = b.col1 "
            + " WHERE b.col3 > 0 GROUP BY a.col2, a.col3"},
        new Object[]{"SELECT col3 FROM a GROUP BY col3, col1"},
        new Object[]{"SELECT col1 FROM a GROUP BY col3, col1"},
        new Object[]{"SELECT AVG(col3) FROM (SELECT col1, col3 FROM a WHERE col3 > 1 GROUP BY col1, col3)"},

        // Test optimized constant literal.
        new Object[]{"SELECT col1 FROM a WHERE col3 > 0 AND col3 < -5"},
        // TODO: fix agg without group by return zero-row instead of default agg results
        // new Object[]{"SELECT COALESCE(SUM(col3), 0) FROM a WHERE col1 = 'foo' AND col1 = 'bar'"},
        new Object[]{"SELECT SUM(CAST(col3 AS INTEGER)) FROM a HAVING MIN(col3) BETWEEN 1 AND 0"},
        new Object[]{"SELECT col1, COUNT(col3) FROM a GROUP BY col1 HAVING SUM(col3) > 40 AND SUM(col3) < 30"},
        new Object[]{"SELECT col1, COUNT(col3) FROM b GROUP BY col1 HAVING SUM(col3) >= 42.5"},

        // Test SQL functions
        // TODO split these SQL functions into separate test files to share between planner and runtime
        // LIKE function
        new Object[]{"SELECT col1 FROM a WHERE col2 LIKE '%o%'"},
        new Object[]{"SELECT a.col1, b.col1 FROM a JOIN b ON a.col3 = b.col3 WHERE a.col2 LIKE b.col1"},
        new Object[]{"SELECT a.col1 LIKE b.col1 FROM a JOIN b ON a.col3 = b.col3"},

        // COALESCE function
        new Object[]{"SELECT a.col1, COALESCE(b.col3, 0) FROM a LEFT JOIN b ON a.col1 = b.col2"},
        new Object[]{"SELECT a.col1, COALESCE(a.col3, 0) FROM a WHERE COALESCE(a.col2, 'bar') = 'bar'"},

        // CASE WHEN function
        new Object[]{"SELECT MAX(CAST((CASE WHEN col3 > 0 THEN 1 WHEN col3 > 10 then 2 ELSE 0 END) AS INTEGER)) "
            + " FROM a"},
        new Object[]{"SELECT col2, CASE WHEN SUM(col3) > 0 THEN 1 WHEN SUM(col3) > 10 then 2 WHEN SUM(col3) > 100 "
            + " THEN 3 ELSE 0 END FROM a GROUP BY col2"}
    };
  }
}
