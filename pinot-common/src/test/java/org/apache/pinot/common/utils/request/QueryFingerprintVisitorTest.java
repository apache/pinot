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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class QueryFingerprintVisitorTest {

  /**
   * Helper method to parse SQL and apply the visitor
   */
  private String generateFingerprint(String sql) throws Exception {
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    SqlNode sqlNode = sqlNodeAndOptions.getSqlNode();

    QueryFingerprintVisitor visitor = new QueryFingerprintVisitor();
    SqlNode normalizedNode = sqlNode.accept(visitor);

    // same as QueryFingerprintUtils.generateFingerprint
    String fingerprint = normalizedNode.toSqlString(c -> c.withDialect(AnsiSqlDialect.DEFAULT)).getSql();
    if (fingerprint != null) {
      fingerprint = fingerprint.replace("\n", " ").replaceAll("\\s+", " ").trim();
    }
    return fingerprint;
  }

  @Test
  public void testBasicLiterals() throws Exception {
    // Numeric literals
    String sql1 = "SELECT col1 FROM table1 WHERE col2 = 100";
    String expected1 = "SELECT `col1` FROM `table1` WHERE `col2` = ?";
    String actual1 = generateFingerprint(sql1);
    assertEquals(actual1, expected1, "Numeric literal should be replaced with ?");

    // String literals
    String sql2 = "SELECT col1 FROM table1 WHERE col2 = 'test'";
    String expected2 = "SELECT `col1` FROM `table1` WHERE `col2` = ?";
    String actual2 = generateFingerprint(sql2);
    assertEquals(actual2, expected2, "String literal should be replaced with ?");

    // Boolean literals
    String sql3 = "SELECT col1 FROM table1 WHERE active = TRUE AND deleted = FALSE";
    String expected3 = "SELECT `col1` FROM `table1` WHERE `active` = ? AND `deleted` = ?";
    String actual3 = generateFingerprint(sql3);
    assertEquals(actual3, expected3, "Boolean literal should be replaced with ?");
  }

  @Test
  public void testPreservedSqlKeywordsAndNulls() throws Exception {
    // Symbolic keywords: DISTINCT, ASC, DESC, etc
    String sql1 = "SELECT DISTINCT col1 FROM table1 ORDER BY col1 DESC LIMIT 100 OFFSET 50";
    String expected1 = "SELECT DISTINCT `col1` FROM `table1` ORDER BY `col1` DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";
    String actual1 = generateFingerprint(sql1);
    assertEquals(actual1, expected1, "DISTINCT keyword should be preserved");

    // NULL
    String sql2 = "SELECT col1 FROM table1 WHERE col2 IS NULL";
    String expected2 = "SELECT `col1` FROM `table1` WHERE `col2` IS NULL";
    String actual2 = generateFingerprint(sql2);
    assertEquals(actual2, expected2, "NULL literal should be preserved");
  }

  @Test
  public void testNotInClauseWithMultipleDataLiterals() throws Exception {
    String sql1 = "SELECT col1 FROM table1 WHERE col2 NOT IN (1)";
    String sql2 = "SELECT col1 FROM table1 WHERE col2 NOT IN (1, 2, 3)";
    String sql3 = "SELECT col1 FROM table1 WHERE col2 NOT IN (5, 10, 15, 20, 25, 30)";

    String fingerprint1 = generateFingerprint(sql1);
    String fingerprint2 = generateFingerprint(sql2);
    String fingerprint3 = generateFingerprint(sql3);

    assertEquals(fingerprint1, fingerprint2,
        "Queries with different NOT IN list sizes should have the same fingerprint");
    assertEquals(fingerprint2, fingerprint3,
        "Queries with different NOT IN list sizes should have the same fingerprint");
  }

  @Test
  public void testInClauseWithMultipleDataLiterals() throws Exception {
    String sql1 = "SELECT col1 FROM table1 WHERE col2 IN (1)";
    String sql2 = "SELECT col1 FROM table1 WHERE col2 IN (1, 2, 3)";
    String sql3 = "SELECT col1 FROM table1 WHERE col2 IN (5, 10, 15, 20, 25, 30)";

    String fingerprint1 = generateFingerprint(sql1);
    String fingerprint2 = generateFingerprint(sql2);
    String fingerprint3 = generateFingerprint(sql3);

    assertEquals(fingerprint1, fingerprint2, "Queries with different IN list sizes should have the same fingerprint");
    assertEquals(fingerprint2, fingerprint3, "Queries with different IN list sizes should have the same fingerprint");
  }

  @Test
  public void testInClauseWithSubquery() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 IN (SELECT col2 FROM table2 WHERE col3 = 100)";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` IN (SELECT `col2` FROM `table2` WHERE `col3` = ?)";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "IN clause with subquery should visit the subquery normally, not squash");
  }

  @Test
  public void testInClauseWithNullValue() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 IN (100, 200, NULL)";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` IN (?, ?, NULL)";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "IN clause with NULL should preserve NULL and replace other literals");
  }

  @Test
  public void testInClauseWithBooleanValues() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 IN (TRUE, FALSE)";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` IN (?)";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "IN clause with boolean values should be replaced with a single ?");
  }

  @Test
  public void testInClauseWithExpressions() throws Exception {
    // Expressions like col1 + 1 should be visited normally
    String sql = "SELECT col1 FROM table1 WHERE col2 IN (col3 + 1, 2)";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` IN (`col3` + ?, ?)";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "IN clause with expressions should visit each value normally");
  }

  @Test
  public void testInClauseWithFunctionCalls() throws Exception {
    // Function calls should be visited normally
    String sql = "SELECT col1 FROM table1 WHERE col2 IN (UPPER('hello'), LOWER('WORLD'))";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` IN (UPPER(?), LOWER(?))";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "IN clause with function calls should visit each function normally");
  }

  @Test
  public void testInClauseWithMixedExpressionsAndLiterals() throws Exception {
    // Mix of expressions and literals should visit each one
    String sql = "SELECT col1 FROM table1 WHERE col2 IN (10, col3 * 2, 20)";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` IN (?, `col3` * ?, ?)";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "IN clause with mixed expressions and literals should visit each value");
  }

  @Test
  public void testBetweenClause() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 BETWEEN 100 AND 200";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` BETWEEN ASYMMETRIC ? AND ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "BETWEEN literals should be replaced with ?");
  }

  @Test
  public void testLimitOffset() throws Exception {
    String sql = "SELECT col1 FROM table1 ORDER BY col1 LIMIT 100 OFFSET 50";
    String expected = "SELECT `col1` FROM `table1` ORDER BY `col1` OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "LIMIT/OFFSET should be replaced with ?");
  }

  @Test
  public void testCaseExpression() throws Exception {
    String sql = "SELECT CASE WHEN col1 > 100 THEN 'high' WHEN col1 > 50 THEN 'medium' ELSE 'low' END FROM table1";
    String expected = "SELECT CASE WHEN `col1` > ? THEN ? WHEN `col1` > ? THEN ? ELSE ? END FROM `table1`";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "CASE expression literals should be replaced with ?");
  }

  @Test
  public void testSubquery() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 IN (SELECT col2 FROM table2 WHERE col3 = 100)";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` IN (SELECT `col2` FROM `table2` WHERE `col3` = ?)";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Subquery literals should be replaced with ?");
  }

  @Test
  public void testJoinWithAliases() throws Exception {
    String sql = "SELECT t1.col1, t2.col2 FROM table1 t1 "
        + "LEFT JOIN table2 t2 ON t1.id = t2.id "
        + "WHERE t1.col3 > 100";
    String expected = "SELECT `t1`.`col1`, `t2`.`col2` FROM `table1` AS `t1` "
        + "LEFT JOIN `table2` AS `t2` ON `t1`.`id` = `t2`.`id` WHERE `t1`.`col3` > ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "JOIN query literals should be replaced with ?");
  }

  @Test
  public void testWithClause() throws Exception {
    String sql = "WITH cte AS (SELECT col1, col2 FROM table1 WHERE col3 = 100) "
        + "SELECT col1 FROM cte WHERE col2 = 200";
    String expected = "WITH `cte` AS (SELECT `col1`, `col2` FROM `table1` WHERE `col3` = ?) "
        + "SELECT `col1` FROM `cte` WHERE `col2` = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "CTE literals should be replaced with ?");
  }

  @Test
  public void testUnion() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 = 100 "
        + "UNION "
        + "SELECT col1 FROM table2 WHERE col3 = 200";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` = ? "
        + "UNION "
        + "SELECT `col1` FROM `table2` WHERE `col3` = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "UNION query literals should be replaced with ?");
  }

  @Test
  public void testUnionAll() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 = 100 "
        + "UNION ALL "
        + "SELECT col1 FROM table2 WHERE col3 = 200";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` = ? "
        + "UNION ALL "
        + "SELECT `col1` FROM `table2` WHERE `col3` = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "UNION ALL should be preserved and literals replaced with ?");
  }

  @Test
  public void testIntersect() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 = 100 "
        + "INTERSECT "
        + "SELECT col1 FROM table2 WHERE col3 = 200";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` = ? "
        + "INTERSECT "
        + "SELECT `col1` FROM `table2` WHERE `col3` = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "INTERSECT should be preserved and literals replaced with ?");
  }

  @Test
  public void testExcept() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 = 100 "
        + "EXCEPT "
        + "SELECT col1 FROM table2 WHERE col3 = 200";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` = ? "
        + "EXCEPT "
        + "SELECT `col1` FROM `table2` WHERE `col3` = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "EXCEPT should be preserved and literals replaced with ?");
  }

  @Test
  public void testMinus() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 = 100 "
        + "MINUS "
        + "SELECT col1 FROM table2 WHERE col3 = 200";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` = ? "
        + "EXCEPT "  // Note: MINUS is converted to EXCEPT by Calcite
        + "SELECT `col1` FROM `table2` WHERE `col3` = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "MINUS should be converted to EXCEPT and literals replaced with ?");
  }

  @Test
  public void testExplain() throws Exception {
    String sql = "EXPLAIN PLAN FOR SELECT col1 FROM table1 WHERE col2 = 100";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "EXPLAIN PLAN FOR should be ignored and query should be normalized");
  }

  @Test
  public void testAggregateFunctions() throws Exception {
    String sql = "SELECT COUNT(*), SUM(col1), AVG(col2) FROM table1 WHERE col3 > 100 GROUP BY col4";
    String expected = "SELECT COUNT(*), SUM(`col1`), AVG(`col2`) FROM `table1` WHERE `col3` > ? GROUP BY `col4`";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Aggregate functions should be preserved and literals replaced with ?");
  }

  @Test
  public void testComplexQuery() throws Exception {
    String sql = "SELECT t1.col1, COUNT(*), AVG(t2.col2) "
        + "FROM table1 t1 "
        + "LEFT JOIN table2 t2 ON t1.id = t2.id "
        + "WHERE t1.col3 IN (100, 200, 300) "
        + "AND t2.col4 BETWEEN 50 AND 100 "
        + "GROUP BY t1.col1 "
        + "HAVING COUNT(*) > 10 "
        + "ORDER BY AVG(t2.col2) DESC "
        + "LIMIT 100";
    String expected = "SELECT `t1`.`col1`, COUNT(*), AVG(`t2`.`col2`) "
        + "FROM `table1` AS `t1` "
        + "LEFT JOIN `table2` AS `t2` ON `t1`.`id` = `t2`.`id` "
        + "WHERE `t1`.`col3` IN (?) "
        + "AND `t2`.`col4` BETWEEN ASYMMETRIC ? AND ? "
        + "GROUP BY `t1`.`col1` "
        + "HAVING COUNT(*) > ? "
        + "ORDER BY AVG(`t2`.`col2`) DESC "
        + "FETCH NEXT ? ROWS ONLY";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Complex query should preserve structure, and replace all other literals with ?");
  }

  @Test
  public void testWindowFunction() throws Exception {
    String sql = "SELECT col1, ROW_NUMBER() OVER (PARTITION BY col2 ORDER BY col3) as rn "
        + "FROM table1 WHERE col4 > 100";
    String expected = "SELECT `col1`, ROW_NUMBER() OVER (PARTITION BY `col2` ORDER BY `col3`) AS `rn` "
        + "FROM `table1` WHERE `col4` > ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Window function should be preserved and literals replaced with ?");
  }

  @Test
  public void testWindowFunctionWithFrame() throws Exception {
    String sql = "SELECT col1, "
        + "AVG(amount) OVER (ORDER BY date_col ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) as moving_avg "
        + "FROM sales WHERE store = 100";
    String expected = "SELECT `col1`, "
        + "AVG(`amount`) OVER (ORDER BY `date_col` ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS `moving_avg` "
        + "FROM `sales` WHERE `store` = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Window frame literals are preserved (window spec not visited)");
  }

  @Test
  public void testWindowFunctionWithLiteralInOrderBy() throws Exception {
    String sql = "SELECT ROW_NUMBER() OVER (ORDER BY col1 + 100) as rn FROM table1 WHERE col2 = 50";
    String expected = "SELECT ROW_NUMBER() OVER (ORDER BY `col1` + 100) AS `rn` FROM `table1` WHERE `col2` = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Literals in window specification are preserved");
  }

  @Test
  public void testWindowFunctionWithLiteralInAggregate() throws Exception {
    String sql = "SELECT SUM(amount + 10) OVER (PARTITION BY category) FROM sales WHERE date_col = '2024-01-01'";
    String expected = "SELECT SUM(`amount` + ?) OVER (PARTITION BY `category`) FROM `sales` WHERE `date_col` = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Literals in the aggregate function ARE replaced (operand 0 is visited)");
  }

  @Test
  public void testMultilineFormatNormalization() throws Exception {
    String sql = "SELECT col1\nFROM table1\nWHERE col2 = 100";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Multi-line query should be normalized to single line with literals replaced");
    assertFalse(actual.contains("\n"), "Should not contain newlines after normalization");
  }

  @Test
  public void testSpacesNormalization() throws Exception {
    String sql = "  SELECT col1 FROM table1 WHERE col2 = 100     \n";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Spaces and newlines should be normalized");
  }

  @Test
  public void testNullSafety() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 IS NULL";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` IS NULL";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "IS NULL should be preserved");

    String sql2 = "SELECT col1 FROM table1 WHERE col2 IS NOT NULL";
    String expected2 = "SELECT `col1` FROM `table1` WHERE `col2` IS NOT NULL";
    String actual2 = generateFingerprint(sql2);
    assertEquals(actual2, expected2, "IS NOT NULL should be preserved");
  }

  @Test
  public void testRightJoin() throws Exception {
    String sql = "SELECT t1.col1, t2.col2 FROM table1 t1 RIGHT JOIN table2 t2 ON t1.id = t2.id WHERE t2.col3 > 100";
    String expected = "SELECT `t1`.`col1`, `t2`.`col2` FROM `table1` AS `t1` "
        + "RIGHT JOIN `table2` AS `t2` ON `t1`.`id` = `t2`.`id` WHERE `t2`.`col3` > ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "RIGHT JOIN should be preserved and literals replaced with ?");
  }

  @Test
  public void testNestedSubqueries() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 IN "
        + "(SELECT col2 FROM table2 WHERE col3 IN "
        + "(SELECT col3 FROM table3 WHERE col4 = 100))";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` IN "
        + "(SELECT `col2` FROM `table2` WHERE `col3` IN "
        + "(SELECT `col3` FROM `table3` WHERE `col4` = ?))";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Nested subquery literals should be replaced with ?");
  }


  @Test
  public void testDistinctKeyword() throws Exception {
    String sql = "SELECT DISTINCT col1, col2 FROM table1 WHERE col3 > 100";
    String expected = "SELECT DISTINCT `col1`, `col2` FROM `table1` WHERE `col3` > ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "DISTINCT keyword should be preserved");
  }

  @Test
  public void testMultipleAggregateFunctions() throws Exception {
    String sql = "SELECT COUNT(*), SUM(col1), AVG(col2), MIN(col3), MAX(col4), STDDEV(col5) "
        + "FROM table1 WHERE col6 > 100 GROUP BY col7";
    String expected = "SELECT COUNT(*), SUM(`col1`), AVG(`col2`), MIN(`col3`), MAX(`col4`), STDDEV(`col5`) "
        + "FROM `table1` WHERE `col6` > ? GROUP BY `col7`";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Multiple aggregate functions should be preserved and literals replaced with ?");
  }

  @Test
  public void testLikeOperator() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 LIKE '%test%'";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` LIKE ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "LIKE operator should be preserved and pattern replaced with ?");
  }

  @Test
  public void testCoalesceFunction() throws Exception {
    String sql = "SELECT COALESCE(col1, col2, 0) FROM table1 WHERE col3 > 100";
    String expected = "SELECT COALESCE(`col1`, `col2`, ?) FROM `table1` WHERE `col3` > ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "COALESCE should be preserved and literals replaced with ?");
  }

  @Test
  public void testCastOperation() throws Exception {
    String sql = "SELECT CAST(col1 AS VARCHAR) FROM table1 WHERE CAST(col2 AS INTEGER) = 100";
    String expected = "SELECT CAST(`col1` AS VARCHAR) FROM `table1` WHERE CAST(`col2` AS INTEGER) = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "CAST operations should be preserved and literals replaced with ?");
  }

  @Test
  public void testGroupByWithHaving() throws Exception {
    String sql = "SELECT col1, COUNT(*) FROM table1 WHERE col2 > 100 "
        + "GROUP BY col1 HAVING COUNT(*) > 10 AND SUM(col3) < 1000";
    String expected = "SELECT `col1`, COUNT(*) FROM `table1` WHERE `col2` > ? "
        + "GROUP BY `col1` HAVING COUNT(*) > ? AND SUM(`col3`) < ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "GROUP BY and HAVING should be preserved and literals replaced with ?");
  }

  @Test
  public void testComplexCaseExpression() throws Exception {
    String sql = "SELECT col1, "
        + "CASE "
        + "  WHEN col2 > 100 AND col3 < 50 THEN 'high' "
        + "  WHEN col2 > 50 THEN 'medium' "
        + "  WHEN col2 IS NULL THEN 'unknown' "
        + "  ELSE 'low' "
        + "END as category "
        + "FROM table1";
    String expected = "SELECT `col1`, "
        + "CASE "
        + "WHEN `col2` > ? AND `col3` < ? THEN ? "
        + "WHEN `col2` > ? THEN ? "
        + "WHEN `col2` IS NULL THEN ? "
        + "ELSE ? "
        + "END AS `category` "
        + "FROM `table1`";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Complex CASE expression should be preserved and literals replaced with ?");
  }

  @Test
  public void testStringConcatenation() throws Exception {
    String sql = "SELECT col1 || ' - ' || col2 FROM table1 WHERE col3 = 'test'";
    String expected = "SELECT `col1` || ? || `col2` FROM `table1` WHERE `col3` = ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "String concatenation literals should be replaced with ?");
  }

  @Test
  public void testArrayConstructor() throws Exception {
    String sql = "SELECT col1 FROM table1 WHERE col2 IN (100, 200, 300, 400, 500)";
    String expected = "SELECT `col1` FROM `table1` WHERE `col2` IN (?)";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "IN clause with multiple values should be squashed to a single ?");
  }

  @Test
  public void testComplexJoinConditions() throws Exception {
    String sql = "SELECT t1.col1, t2.col2 FROM table1 t1 "
        + "LEFT JOIN table2 t2 ON t1.id = t2.id AND t1.col3 > 100 AND t2.col4 < 200";
    String expected = "SELECT `t1`.`col1`, `t2`.`col2` FROM `table1` AS `t1` "
        + "LEFT JOIN `table2` AS `t2` ON `t1`.`id` = `t2`.`id` AND `t1`.`col3` > ? AND `t2`.`col4` < ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Complex join conditions should have literals replaced with ?");
  }

  @Test
  public void testPartitionByMultipleColumns() throws Exception {
    String sql = "SELECT col1, ROW_NUMBER() OVER (PARTITION BY col2, col3, col4 ORDER BY col5) as rn "
        + "FROM table1 WHERE col6 > 100";
    String expected = "SELECT `col1`, ROW_NUMBER() OVER (PARTITION BY `col2`, `col3`, `col4` ORDER BY `col5`) AS `rn` "
        + "FROM `table1` WHERE `col6` > ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected,
        "PARTITION BY with multiple columns should be preserved and literals replaced with ?");
  }

  @Test
  public void testFilteredAggregation() throws Exception {
    String sql = "SELECT COUNT(*) FILTER (WHERE status = 'active'), "
        + "SUM(amount) FILTER (WHERE category = 'sale') "
        + "FROM orders WHERE order_date > '2024-01-01'";
    String expected = "SELECT COUNT(*) FILTER (WHERE `status` = ?), "
        + "SUM(`amount`) FILTER (WHERE `category` = ?) FROM `orders` WHERE `order_date` > ?";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Filtered aggregation literals should be replaced with ?");
  }

  @Test
  public void testMultipleFilteredAggregations() throws Exception {
    String sql = "SELECT "
        + "COUNT(*) FILTER (WHERE amount > 100) as high_count, "
        + "COUNT(*) FILTER (WHERE amount <= 100) as low_count, "
        + "AVG(amount) FILTER (WHERE status = 'completed') as avg_completed "
        + "FROM transactions";
    String expected = "SELECT COUNT(*) FILTER (WHERE `amount` > ?) AS `high_count`, "
        + "COUNT(*) FILTER (WHERE `amount` <= ?) AS `low_count`, "
        + "AVG(`amount`) FILTER (WHERE `status` = ?) AS `avg_completed` FROM `transactions`";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Multiple filtered aggregations should be handled correctly");
  }

  @Test
  public void testFactDimensionJoin() throws Exception {
    String sql = "SELECT d.product_name, SUM(f.sales_amount) "
        + "FROM sales_fact f "
        + "INNER JOIN product_dim d ON f.product_id = d.product_id "
        + "WHERE f.sale_date >= '2024-01-01' AND d.category = 'Electronics' "
        + "GROUP BY d.product_name";
    String expected = "SELECT `d`.`product_name`, SUM(`f`.`sales_amount`) "
        + "FROM `sales_fact` AS `f` "
        + "INNER JOIN `product_dim` AS `d` ON `f`.`product_id` = `d`.`product_id` "
        + "WHERE `f`.`sale_date` >= ? AND `d`.`category` = ? "
        + "GROUP BY `d`.`product_name`";
    String actual = generateFingerprint(sql);
    assertEquals(actual, expected, "Fact-dimension join literals should be replaced with ?");
  }
}
