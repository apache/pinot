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
package org.apache.pinot.common.utils.request;

import org.apache.pinot.spi.trace.QueryFingerprint;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class QueryFingerprintUtilsTest {

  @Test
  public void testNullQuery() {
    QueryFingerprint fingerprint = QueryFingerprintUtils.generateFingerprint(null);
    Assert.assertNull(fingerprint, "Null query should return null fingerprint");
  }

  /**
   * Data provider for single-stage query shapes.
   * Returns: [queryString, expectedFingerprintPattern]
   * Note: Calcite uses ANSI SQL standard syntax (FETCH NEXT instead of LIMIT, BETWEEN ASYMMETRIC)
   */
  @DataProvider(name = "singleStageQueryShapes")
  public Object[][] provideSingleStageQueryShapes() {
    return new Object[][]{
        // simple filters
        {"SELECT col1 FROM table1 WHERE col2 = 100",
            "SELECT `col1` FROM `table1` WHERE `col2` = ?"},
        {"SELECT col1 FROM table1 WHERE col2 > 100 AND col3 < 200",
            "SELECT `col1` FROM `table1` WHERE `col2` > ? AND `col3` < ?"},
        {"SELECT * FROM table1 WHERE col1 IN (1, 2, 3)",
            "SELECT * FROM `table1` WHERE `col1` IN (?)"},
        {"SELECT * FROM table1 WHERE col1 NOT IN (1, 2, 3)",
            "SELECT * FROM `table1` WHERE `col1` NOT IN (?)"},
        {"SELECT * FROM table1 WHERE col1 BETWEEN 10 AND 100",
            "SELECT * FROM `table1` WHERE `col1` BETWEEN ASYMMETRIC ? AND ?"},
        // aggregations
        {"SELECT COUNT(*) FROM table1 WHERE col2 > 100",
            "SELECT COUNT(*) FROM `table1` WHERE `col2` > ?"},
        {"SELECT col1, COUNT(*) FROM table1 WHERE col2 > 100 GROUP BY col1",
            "SELECT `col1`, COUNT(*) FROM `table1` WHERE `col2` > ? GROUP BY `col1`"},
        // order by
        {"SELECT col1, col2 FROM table1 ORDER BY col1 LIMIT 10",
            "SELECT `col1`, `col2` FROM `table1` ORDER BY `col1` FETCH NEXT ? ROWS ONLY"},
        // DISTINCT
        {"SELECT DISTINCT col1, col2 FROM table1 WHERE col3 > 100",
            "SELECT DISTINCT `col1`, `col2` FROM `table1` WHERE `col3` > ?"},
        // Multiple aggregations
        {"SELECT col1, COUNT(*), SUM(col2), AVG(col3), MIN(col4), MAX(col5) "
            + "FROM table1 WHERE col6 > 100 GROUP BY col1",
            "SELECT `col1`, COUNT(*), SUM(`col2`), AVG(`col3`), MIN(`col4`), MAX(`col5`) "
                + "FROM `table1` WHERE `col6` > ? GROUP BY `col1`"},
        // Window function
        {"SELECT col1, ROW_NUMBER() OVER (PARTITION BY col2 ORDER BY col3) as rn FROM table1",
            "SELECT `col1`, ROW_NUMBER() OVER (PARTITION BY `col2` ORDER BY `col3`) AS `rn` "
                + "FROM `table1`"},
        // CASE expression
        {"SELECT CASE WHEN col1 > 100 THEN 'high' WHEN col1 > 50 THEN 'medium' ELSE 'low' END "
            + "FROM table1",
            "SELECT CASE WHEN `col1` > ? THEN ? WHEN `col1` > ? THEN ? ELSE ? END FROM `table1`"},
        // HAVING clause
        {"SELECT col1, COUNT(*) FROM table1 WHERE col2 > 100 GROUP BY col1 HAVING COUNT(*) > 10",
            "SELECT `col1`, COUNT(*) FROM `table1` WHERE `col2` > ? "
                + "GROUP BY `col1` HAVING COUNT(*) > ?"},
        // IS NOT NULL
        {"SELECT col1 FROM table1 WHERE col2 IS NOT NULL AND col3 > 100",
            "SELECT `col1` FROM `table1` WHERE `col2` IS NOT NULL AND `col3` > ?"},
        // LIKE operator
        {"SELECT col1 FROM table1 WHERE col2 LIKE '%test%' AND col3 = 100",
            "SELECT `col1` FROM `table1` WHERE `col2` LIKE ? AND `col3` = ?"},
        // COALESCE function
        {"SELECT COALESCE(col1, col2, 0) FROM table1 WHERE col3 > 100",
            "SELECT COALESCE(`col1`, `col2`, ?) FROM `table1` WHERE `col3` > ?"},
        // CAST operation
        {"SELECT CAST(col1 AS VARCHAR) FROM table1 WHERE CAST(col2 AS INTEGER) = 100",
            "SELECT CAST(`col1` AS VARCHAR) FROM `table1` WHERE CAST(`col2` AS INTEGER) = ?"},
    };
  }

  /**
   * Data provider for multi-stage query shapes.
   * Returns: [queryString, expectedFingerprintPattern]
   */
  @DataProvider(name = "multiStageQueryShapes")
  public Object[][] provideMultiStageQueryShapes() {
    return new Object[][]{
        // LEFT JOIN with filter
        {"SELECT * FROM table1 t1 LEFT JOIN table2 t2 ON t1.id = t2.id WHERE t1.col1 > 100",
            "SELECT * FROM `table1` AS `t1` LEFT JOIN `table2` AS `t2` ON `t1`.`id` = `t2`.`id` "
                + "WHERE `t1`.`col1` > ?"},
        // Subquery in WHERE clause
        {"SELECT col1 FROM table1 WHERE col2 IN (SELECT col2 FROM table2 WHERE col3 = 100)",
            "SELECT `col1` FROM `table1` WHERE `col2` IN "
                + "(SELECT `col2` FROM `table2` WHERE `col3` = ?)"},
        // CTE (WITH clause)
        {"WITH cte AS (SELECT col1 FROM table1 WHERE col2 = 100) SELECT * FROM cte WHERE col1 > 50",
            "WITH `cte` AS (SELECT `col1` FROM `table1` WHERE `col2` = ?) "
                + "SELECT * FROM `cte` WHERE `col1` > ?"},
        // UNION
        {"SELECT col1 FROM table1 UNION SELECT col1 FROM table2",
            "SELECT `col1` FROM `table1` UNION SELECT `col1` FROM `table2`"},
        // RIGHT JOIN
        {"SELECT * FROM table1 t1 RIGHT JOIN table2 t2 ON t1.id = t2.id WHERE t2.col1 > 100",
            "SELECT * FROM `table1` AS `t1` RIGHT JOIN `table2` AS `t2` ON `t1`.`id` = `t2`.`id` "
                + "WHERE `t2`.`col1` > ?"},
        // FULL OUTER JOIN
        {"SELECT * FROM table1 t1 FULL OUTER JOIN table2 t2 ON t1.id = t2.id",
            "SELECT * FROM `table1` AS `t1` FULL JOIN `table2` AS `t2` ON `t1`.`id` = `t2`.`id`"},
        // CROSS JOIN
        {"SELECT * FROM table1 t1 CROSS JOIN table2 t2 WHERE t1.col1 > 100",
            "SELECT * FROM `table1` AS `t1` CROSS JOIN `table2` AS `t2` WHERE `t1`.`col1` > ?"},
        // Nested subqueries
        {"SELECT col1 FROM table1 WHERE col2 IN "
            + "(SELECT col2 FROM table2 WHERE col3 IN "
            + "(SELECT col3 FROM table3 WHERE col4 = 100))",
            "SELECT `col1` FROM `table1` WHERE `col2` IN "
                + "(SELECT `col2` FROM `table2` WHERE `col3` IN "
                + "(SELECT `col3` FROM `table3` WHERE `col4` = ?))"},
        // INTERSECT
        {"SELECT col1 FROM table1 WHERE col2 = 100 INTERSECT SELECT col1 FROM table2 WHERE col3 = 200",
            "SELECT `col1` FROM `table1` WHERE `col2` = ? "
                + "INTERSECT SELECT `col1` FROM `table2` WHERE `col3` = ?"},
        // EXCEPT
        {"SELECT col1 FROM table1 WHERE col2 = 100 EXCEPT SELECT col1 FROM table2 WHERE col3 = 200",
            "SELECT `col1` FROM `table1` WHERE `col2` = ? "
                + "EXCEPT SELECT `col1` FROM `table2` WHERE `col3` = ?"},
        // UNION ALL
        {"SELECT col1 FROM table1 WHERE col2 = 100 UNION ALL SELECT col1 FROM table2 WHERE col3 = 200",
            "SELECT `col1` FROM `table1` WHERE `col2` = ? "
                + "UNION ALL SELECT `col1` FROM `table2` WHERE `col3` = ?"},
        // Multiple CTEs with JOIN
        {"WITH cte1 AS (SELECT col1 FROM table1 WHERE col2 = 100), "
            + "cte2 AS (SELECT col3 FROM table2 WHERE col4 = 200) "
            + "SELECT cte1.col1, cte2.col3 FROM cte1 JOIN cte2 ON cte1.col1 = cte2.col3",
            "WITH `cte1` AS (SELECT `col1` FROM `table1` WHERE `col2` = ?), "
                + "`cte2` AS (SELECT `col3` FROM `table2` WHERE `col4` = ?) "
                + "SELECT `cte1`.`col1`, `cte2`.`col3` FROM `cte1` INNER JOIN `cte2` "
                + "ON `cte1`.`col1` = `cte2`.`col3`"},
        // Self-join
        {"SELECT t1.col1, t2.col2 FROM table1 t1 JOIN table1 t2 ON t1.id = t2.parent_id "
            + "WHERE t1.col3 > 100",
            "SELECT `t1`.`col1`, `t2`.`col2` FROM `table1` AS `t1` INNER JOIN `table1` AS `t2` "
                + "ON `t1`.`id` = `t2`.`parent_id` WHERE `t1`.`col3` > ?"},
        // Complex join conditions
        {"SELECT t1.col1, t2.col2 FROM table1 t1 LEFT JOIN table2 t2 "
            + "ON t1.id = t2.id AND t1.col3 > 100 AND t2.col4 < 200",
            "SELECT `t1`.`col1`, `t2`.`col2` FROM `table1` AS `t1` LEFT JOIN `table2` AS `t2` "
                + "ON `t1`.`id` = `t2`.`id` AND `t1`.`col3` > ? AND `t2`.`col4` < ?"},
    };
  }

  @Test(dataProvider = "singleStageQueryShapes")
  public void testSingleStageQueryShapes(String queryString, String expectedFingerprint) {
    try {
      // Parse query twice to verify consistency
      SqlNodeAndOptions sqlNodeAndOptions1 = CalciteSqlParser.compileToSqlNodeAndOptions(queryString);
      SqlNodeAndOptions sqlNodeAndOptions2 = CalciteSqlParser.compileToSqlNodeAndOptions(queryString);

      // Generate fingerprints
      QueryFingerprint fingerprint1 = QueryFingerprintUtils.generateFingerprint(sqlNodeAndOptions1);
      QueryFingerprint fingerprint2 = QueryFingerprintUtils.generateFingerprint(sqlNodeAndOptions2);

      // 1. Verify queryHash is consistent
      Assert.assertNotNull(fingerprint1, "Fingerprint should not be null");
      Assert.assertNotNull(fingerprint2, "Fingerprint should not be null");
      Assert.assertEquals(fingerprint1.getQueryHash(), fingerprint2.getQueryHash(),
          "QueryHash should be consistent for same query");

      // 2. Verify fingerprint matches expected
      String actualFingerprint = fingerprint1.getFingerprint();
      Assert.assertNotNull(actualFingerprint, "Fingerprint should not be null");

      Assert.assertEquals(actualFingerprint, expectedFingerprint,
          String.format("Fingerprint mismatch\nExpected: %s\nActual: %s", expectedFingerprint, actualFingerprint));
    } catch (Exception e) {
      Assert.fail("Failed to process query: " + queryString + "\nError: " + e.getMessage());
    }
  }

  @Test(dataProvider = "multiStageQueryShapes")
  public void testMultiStageQueryShapes(String queryString, String expectedFingerprint) {
    try {
      // Parse query twice to verify consistency
      SqlNodeAndOptions sqlNodeAndOptions1 = CalciteSqlParser.compileToSqlNodeAndOptions(queryString);
      SqlNodeAndOptions sqlNodeAndOptions2 = CalciteSqlParser.compileToSqlNodeAndOptions(queryString);

      // Generate fingerprints
      QueryFingerprint fingerprint1 = QueryFingerprintUtils.generateFingerprint(sqlNodeAndOptions1);
      QueryFingerprint fingerprint2 = QueryFingerprintUtils.generateFingerprint(sqlNodeAndOptions2);

      // 1. Verify queryHash is consistent
      Assert.assertNotNull(fingerprint1, "Fingerprint should not be null");
      Assert.assertNotNull(fingerprint2, "Fingerprint should not be null");
      Assert.assertEquals(fingerprint1.getQueryHash(), fingerprint2.getQueryHash(),
          "QueryHash should be consistent for same query");

      // 2. Verify fingerprint matches expected
      String actualFingerprint = fingerprint1.getFingerprint();
      Assert.assertNotNull(actualFingerprint, "Fingerprint should not be null");

      Assert.assertEquals(actualFingerprint, expectedFingerprint,
          String.format("Fingerprint mismatch\nExpected: %s\nActual: %s", expectedFingerprint, actualFingerprint));
    } catch (Exception e) {
      Assert.fail("Failed to process query: " + queryString + "\nError: " + e.getMessage());
    }
  }

  @Test
  public void testQueryShapeNormalization() {
    // Queries with different literals should produce the same fingerprint
    String[] queries = {
        "SELECT * FROM table1 WHERE col1 = 100",
        "SELECT * FROM table1 WHERE col1 = 200",
        "SELECT * FROM table1 WHERE col1 = 999"
    };

    QueryFingerprint[] fingerprints = new QueryFingerprint[queries.length];
    for (int i = 0; i < queries.length; i++) {
      SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(queries[i]);
      fingerprints[i] = QueryFingerprintUtils.generateFingerprint(sqlNodeAndOptions);
    }

    // All should have the same hash (literals normalized)
    for (int i = 1; i < fingerprints.length; i++) {
      Assert.assertEquals(fingerprints[i].getQueryHash(), fingerprints[0].getQueryHash(),
          "Queries with same shape but different literals should have same hash");
      Assert.assertEquals(fingerprints[i].getFingerprint(), fingerprints[0].getFingerprint(),
          "Queries with same shape but different literals should have same fingerprint");
    }
  }

  @Test
  public void testFingerprintTrimsWhitespace() {
    SqlNodeAndOptions sqlNodeAndOptions1 = CalciteSqlParser.compileToSqlNodeAndOptions("SELECT col1 FROM table1");
    SqlNodeAndOptions sqlNodeAndOptions2 = CalciteSqlParser.compileToSqlNodeAndOptions("SELECT col1 FROM table1 ");
    QueryFingerprint queryFingerprint1 = QueryFingerprintUtils.generateFingerprint(sqlNodeAndOptions1);
    QueryFingerprint queryFingerprint2 = QueryFingerprintUtils.generateFingerprint(sqlNodeAndOptions2);

    Assert.assertNotNull(queryFingerprint1);
    Assert.assertNotNull(queryFingerprint2);
    Assert.assertEquals(queryFingerprint1.getFingerprint(), queryFingerprint2.getFingerprint());
    Assert.assertEquals(queryFingerprint1.getQueryHash(), queryFingerprint2.getQueryHash());
  }

  @Test
  public void testMultilineFormatNormalization() {
    // Queries with newlines should produce the same fingerprint as single-line queries
    String query1 = "SELECT col1\nFROM table1\nWHERE col2 = 100";
    String query2 = "SELECT col1 FROM table1 WHERE col2 = 100";

    SqlNodeAndOptions sqlNodeAndOptions1 = CalciteSqlParser.compileToSqlNodeAndOptions(query1);
    SqlNodeAndOptions sqlNodeAndOptions2 = CalciteSqlParser.compileToSqlNodeAndOptions(query2);

    QueryFingerprint fingerprint1 = QueryFingerprintUtils.generateFingerprint(sqlNodeAndOptions1);
    QueryFingerprint fingerprint2 = QueryFingerprintUtils.generateFingerprint(sqlNodeAndOptions2);

    Assert.assertNotNull(fingerprint1);
    Assert.assertNotNull(fingerprint2);
    Assert.assertEquals(fingerprint1.getQueryHash(), fingerprint2.getQueryHash(),
        "Multi-line and single-line queries should have same hash");
    Assert.assertEquals(fingerprint1.getFingerprint(), fingerprint2.getFingerprint(),
        "Multi-line and single-line queries should have same fingerprint");
    Assert.assertFalse(fingerprint1.getFingerprint().contains("\n"),
        "Fingerprint should not contain newlines");
  }

  @Test
  public void testComplexMultiStageQuery() {
    // Test a complex multi-stage query with JOINs, subqueries, and aggregations
    String query = "SELECT t1.col1, COUNT(*), AVG(t2.col2) "
        + "FROM table1 t1 "
        + "LEFT JOIN table2 t2 ON t1.id = t2.id "
        + "WHERE t1.col3 IN (100, 200, 300) "
        + "AND t2.col4 BETWEEN 50 AND 100 "
        + "GROUP BY t1.col1 "
        + "HAVING COUNT(*) > 10 "
        + "ORDER BY AVG(t2.col2) DESC "
        + "LIMIT 100";

    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    QueryFingerprint fingerprint = QueryFingerprintUtils.generateFingerprint(sqlNodeAndOptions);

    Assert.assertNotNull(fingerprint);
    Assert.assertNotNull(fingerprint.getQueryHash());
    Assert.assertNotNull(fingerprint.getFingerprint());

    // Verify literals are replaced
    String normalizedQuery = fingerprint.getFingerprint();
    Assert.assertTrue(normalizedQuery.contains("?"), "Should contain dynamic parameters");
    Assert.assertFalse(normalizedQuery.contains("100") && !normalizedQuery.contains("?"),
        "Should not contain literal 100");
    Assert.assertFalse(normalizedQuery.contains("200"), "Should not contain literal 200");
    Assert.assertFalse(normalizedQuery.contains("300"), "Should not contain literal 300");
  }
}
