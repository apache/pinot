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
package org.apache.pinot.client;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for the TableNameExtractor class.
 */
public class TableNameExtractorTest {

  @Test
  public void testResolveTableNameWithSingleQuery() {
    // Test that single queries work correctly
    String singleQuery = "SELECT * FROM myTable WHERE id > 100";

    String[] tableNames = TableNameExtractor.resolveTableName(singleQuery);

    assertNotNull(tableNames, "Table names should not be null");
    assertEquals(tableNames.length, 1, "Should resolve exactly one table");
    assertEquals(tableNames[0], "myTable", "Should resolve the correct table name");
  }

  @Test
  public void testResolveTableNameWithSingleStatementAlias() {
    String singleStatementQuery = "SELECT stats.* FROM airlineStats stats LIMIT 10";
    String[] tableNames = TableNameExtractor.resolveTableName(singleStatementQuery);

    assertNotNull(tableNames);
    assertEquals(tableNames.length, 1);
    assertEquals(tableNames[0], "airlineStats");
  }

  @Test
  public void testResolveTableNameWithMultiStatementQuery() {
    // Test the fix for issue #11823: CalciteSQLParser error with multi-statement queries
    String multiStatementQuery = "SET useMultistageEngine=true;\nSELECT stats.* FROM airlineStats stats LIMIT 10";

    // This should not throw a ClassCastException anymore
    String[] tableNames = TableNameExtractor.resolveTableName(multiStatementQuery);

    // Should successfully resolve the table name
    assertNotNull(tableNames, "Table names should not be null");
    assertEquals(tableNames.length, 1, "Should resolve exactly one table");
    assertEquals(tableNames[0], "airlineStats", "Should resolve the correct table name");
  }

  @Test
  public void testResolveTableNameWithMultipleSetStatements() {
    // Test with multiple SET statements
    String multiSetQuery = "SET useMultistageEngine=true;\nSET timeoutMs=10000;\nSELECT * FROM testTable";

    String[] tableNames = TableNameExtractor.resolveTableName(multiSetQuery);

    assertNotNull(tableNames, "Table names should not be null");
    assertEquals(tableNames.length, 1, "Should resolve exactly one table");
    assertEquals(tableNames[0], "testTable", "Should resolve the correct table name");
  }

  @Test
  public void testResolveTableNameWithMultipleSetStatementsAndJoin() {
    String multiStatementQuery = "SET useMultistageEngine=true;\nSET maxRowsInJoin=1000;\n"
        + "SELECT stats.* FROM airlineStats stats LIMIT 10";
    String[] tableNames = TableNameExtractor.resolveTableName(multiStatementQuery);

    assertNotNull(tableNames, "Table names should be resolved for queries with multiple SET statements");
    assertEquals(tableNames.length, 1);
    assertEquals(tableNames[0], "airlineStats");
  }

  @Test
  public void testResolveTableNameWithJoin() {
    // Test with JOIN queries
    String joinQuery = "SELECT * FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id";

    String[] tableNames = TableNameExtractor.resolveTableName(joinQuery);

    assertNotNull(tableNames, "Table names should not be null");
    assertEquals(tableNames.length, 2, "Should resolve two tables");
    assertTrue(Arrays.asList(tableNames).contains("table1"), "Should contain table1");
    assertTrue(Arrays.asList(tableNames).contains("table2"), "Should contain table2");
  }

  @Test
  public void testResolveTableNameWithJoinQueryAndSetStatements() {
    String joinQuery = "SET useMultistageEngine=true;\n"
        + "SELECT a.col1, b.col2 FROM tableA a JOIN tableB b ON a.id = b.id";
    String[] tableNames = TableNameExtractor.resolveTableName(joinQuery);

    assertNotNull(tableNames, "Table names should be resolved for join queries with SET statements");
    assertEquals(tableNames.length, 2);

    Set<String> expectedTableNames = new HashSet<>(Arrays.asList("tableA", "tableB"));
    Set<String> actualTableNames = new HashSet<>(Arrays.asList(tableNames));
    assertEquals(actualTableNames, expectedTableNames);
  }

  @Test
  public void testResolveTableNameWithExplicitAlias() {
    // Test with explicit AS alias
    String aliasQuery = "SELECT u.name FROM users AS u WHERE u.active = true";

    String[] tableNames = TableNameExtractor.resolveTableName(aliasQuery);

    assertNotNull(tableNames, "Table names should not be null");
    assertEquals(tableNames.length, 1, "Should resolve exactly one table");
    assertEquals(tableNames[0], "users", "Should resolve the actual table name, not the alias");
  }

  @Test
  public void testResolveTableNameWithImplicitAlias() {
    // Test with implicit alias (no AS keyword)
    String implicitAliasQuery = "SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id";

    String[] tableNames = TableNameExtractor.resolveTableName(implicitAliasQuery);

    assertNotNull(tableNames, "Table names should not be null");
    assertEquals(tableNames.length, 2, "Should resolve two tables");
    assertTrue(Arrays.asList(tableNames).contains("orders"), "Should contain orders table");
    assertTrue(Arrays.asList(tableNames).contains("users"), "Should contain users table");
  }

  @Test
  public void testResolveTableNameWithCTE() {
    // Test with Common Table Expression (CTE)
    String cteQuery = "WITH active_users AS (SELECT * FROM users WHERE active = true) "
        + "SELECT au.name FROM active_users au JOIN orders o ON au.id = o.user_id";

    String[] tableNames = TableNameExtractor.resolveTableName(cteQuery);

    assertNotNull(tableNames, "Table names should not be null");
    assertEquals(tableNames.length, 2, "Should resolve two tables");
    assertTrue(Arrays.asList(tableNames).contains("users"), "Should contain users table from CTE");
    assertTrue(Arrays.asList(tableNames).contains("orders"), "Should contain orders table");
  }

  @Test
  public void testResolveTableNameWithNestedCTE() {
    // Test with nested CTEs
    String nestedCteQuery = "WITH user_orders AS ("
        + "  SELECT u.id, u.name, o.order_date "
        + "  FROM users u JOIN orders o ON u.id = o.user_id"
        + "), recent_orders AS ("
        + "  SELECT * FROM user_orders WHERE order_date > '2023-01-01'"
        + ") "
        + "SELECT ro.name FROM recent_orders ro JOIN products p ON ro.id = p.user_id";

    String[] tableNames = TableNameExtractor.resolveTableName(nestedCteQuery);

    assertNotNull(tableNames, "Table names should not be null");
    assertEquals(tableNames.length, 3, "Should resolve three tables");
    assertTrue(Arrays.asList(tableNames).contains("users"), "Should contain users table");
    assertTrue(Arrays.asList(tableNames).contains("orders"), "Should contain orders table");
    assertTrue(Arrays.asList(tableNames).contains("products"), "Should contain products table");
  }

  @Test
  public void testResolveTableNameWithSubqueryAlias() {
    // Test with subquery alias
    String subqueryQuery = "SELECT t.name FROM (SELECT * FROM users WHERE active = true) AS t "
        + "JOIN orders o ON t.id = o.user_id";

    String[] tableNames = TableNameExtractor.resolveTableName(subqueryQuery);

    assertNotNull(tableNames, "Table names should not be null");
    assertEquals(tableNames.length, 2, "Should resolve two tables");
    assertTrue(Arrays.asList(tableNames).contains("users"), "Should contain users table from subquery");
    assertTrue(Arrays.asList(tableNames).contains("orders"), "Should contain orders table");
  }

  @Test
  public void testResolveTableNameWithComplexJoinAndAliases() {
    // Test with multiple JOINs and various alias styles
    String complexQuery = "SELECT u.name, o.total, p.title "
        + "FROM users AS u "
        + "INNER JOIN orders o ON u.id = o.user_id "
        + "LEFT JOIN order_items oi ON o.id = oi.order_id "
        + "RIGHT JOIN products AS p ON oi.product_id = p.id "
        + "WHERE u.active = true";

    String[] tableNames = TableNameExtractor.resolveTableName(complexQuery);

    assertNotNull(tableNames, "Table names should not be null");
    assertEquals(tableNames.length, 4, "Should resolve four tables");
    assertTrue(Arrays.asList(tableNames).contains("users"), "Should contain users table");
    assertTrue(Arrays.asList(tableNames).contains("orders"), "Should contain orders table");
    assertTrue(Arrays.asList(tableNames).contains("order_items"), "Should contain order_items table");
    assertTrue(Arrays.asList(tableNames).contains("products"), "Should contain products table");
  }

  @Test
  public void testResolveTableNameWithJoinConditionSubquery() {
    // Test with subquery in join condition
    String joinSubqueryQuery = "SELECT u.name, o.total "
        + "FROM users u "
        + "JOIN orders o ON u.id = o.user_id "
        + "AND o.id IN (SELECT order_id FROM order_items WHERE quantity > 5)";

    String[] tableNames = TableNameExtractor.resolveTableName(joinSubqueryQuery);

    assertNotNull(tableNames, "Table names should not be null");
    assertEquals(tableNames.length, 3, "Should resolve three tables");
    assertTrue(Arrays.asList(tableNames).contains("users"), "Should contain users table");
    assertTrue(Arrays.asList(tableNames).contains("orders"), "Should contain orders table");
    assertTrue(Arrays.asList(tableNames).contains("order_items"),
        "Should contain order_items table from subquery");
  }

  @Test
  public void testResolveTableNameWithOrderBy() {
    // Test with ORDER BY clause
    String orderByQuery = "SELECT * FROM users ORDER BY name";
    String[] tableNames = TableNameExtractor.resolveTableName(orderByQuery);

    assertNotNull(tableNames, "Table names should not be null");
    assertEquals(tableNames.length, 1, "Should resolve exactly one table");
    assertEquals(tableNames[0], "users", "Should resolve the correct table name");
  }

  @Test
  public void testResolveTableNameWithOrderBySubquery() {
    // Test with subquery in ORDER BY clause (rare but possible)
    String orderBySubqueryQuery = "SELECT * FROM users u ORDER BY "
        + "(SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id)";
    String[] tableNames = TableNameExtractor.resolveTableName(orderBySubqueryQuery);

    assertNotNull(tableNames, "Table names should not be null");
    assertEquals(tableNames.length, 2, "Should resolve two tables");
    assertTrue(Arrays.asList(tableNames).contains("users"), "Should contain users table");
    assertTrue(Arrays.asList(tableNames).contains("orders"), "Should contain orders table");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testResolveTableNameWithInvalidQuery() {
    String[] tableNames = TableNameExtractor.resolveTableName("INVALID SQL QUERY");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testResolveTableNameWithOnlySetStatements() {
    TableNameExtractor.resolveTableName("SET useMultistageEngine=true;");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testResolveTableNameWithNullQuery() {
    TableNameExtractor.resolveTableName(null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testResolveTableNameWithEmptyQuery() {
    TableNameExtractor.resolveTableName("");
  }

  /**
   * Data provider for SQL queries and their expected table names.
   * This makes it easy to add new test cases by simply adding entries to this array.
   *
   * @return Object[][] where each Object[] contains: [testName (String), sqlQuery (String),
   * expectedTableNames (String[] or null)]
   * Each entry in the returned array is an Object[] of length 3, structured as follows:
   * <ul>
   * <li><b>testName</b> (String): A descriptive name for the test case.</li>
   * <li><b>sqlQuery</b> (String): The SQL query to be tested.</li>
   * <li><b>expectedTableNames</b> (String[]): The expected table names to be extracted from the query,
   * or {@code null} if no table names are expected (e.g., for invalid or empty queries).</li>
   * </ul>
   * This makes it easy to add new test cases by simply adding entries to this array.
   */
  @DataProvider(name = "sqlQueries")
  public Object[][] sqlQueriesDataProvider() {
    return new Object[][]{
        // Basic queries
        {
            "Simple SELECT",
            "SELECT * FROM users",
            new String[]{"users"},
            false
        },
        {
            "SELECT with WHERE",
            "SELECT name FROM users WHERE age > 18",
            new String[]{"users"},
            false
        },
        {
            "SELECT with LIMIT",
            "SELECT * FROM products LIMIT 10",
            new String[]{"products"},
            false
        },

        // Aliases
        {
            "Explicit alias",
            "SELECT u.name FROM users u",
            new String[]{"users"},
            false
        },
        {
            "Implicit alias",
            "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id",
            new String[]{"users", "orders"},
            false
        },
        {
            "Multiple aliases",
            "SELECT t1.col1, t2.col2 FROM table1 t1, table2 t2",
            new String[]{"table1", "table2"},
            false
        },

        // JOINs
        {
            "INNER JOIN",
            "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id",
            new String[]{"users", "orders"},
            false
        },
        {
            "LEFT JOIN",
            "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id",
            new String[]{"users", "orders"},
            false
        },
        {
            "RIGHT JOIN",
            "SELECT * FROM users u RIGHT JOIN orders o ON u.id = o.user_id",
            new String[]{"users", "orders"},
            false
        },
        {
            "FULL JOIN",
            "SELECT * FROM users u FULL JOIN orders o ON u.id = o.user_id",
            new String[]{"users", "orders"},
            false
        },
        {
            "Multiple JOINs",
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id",
            new String[]{"users", "orders", "products"},
            false
        },

        // CTEs (Common Table Expressions)
        {
            "Simple CTE",
            "WITH active_users AS (SELECT * FROM users WHERE active = true) "
                + "SELECT * FROM active_users",
            new String[]{"users"},
            false
        },
        {
            "Multiple CTEs",
            "WITH active_users AS (SELECT * FROM users WHERE active = true), "
                + "recent_orders AS (SELECT * FROM orders WHERE created_date > '2024-01-01') "
                + "SELECT au.name, ro.order_id FROM active_users au JOIN recent_orders ro ON au.id = ro.user_id",
            new String[]{"users", "orders"},
            false
        },
        {
            "Nested CTE",
            "WITH user_stats AS (SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id), "
                + "top_users AS (SELECT * FROM user_stats WHERE order_count > 10) "
                + "SELECT u.name, tu.order_count FROM users u JOIN top_users tu ON u.id = tu.user_id",
            new String[]{"orders", "users"},
            false
        },

        // Subqueries
        {
            "Subquery in FROM",
            "SELECT * FROM (SELECT * FROM users WHERE active = true) AS active_users",
            new String[]{"users"},
            false
        },
        {
            "Subquery in JOIN",
            "SELECT u.name FROM users u JOIN (SELECT user_id FROM orders WHERE amount > 100) o ON u.id = o.user_id",
            new String[]{"users", "orders"},
            false
        },
        {
            "Subquery in WHERE",
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)",
            new String[]{"users", "orders"},
            false
        },

        // Multi-statement queries
        {
            "SET + SELECT",
            "SET useMultistageEngine=true; SELECT * FROM users",
            new String[]{"users"},
            false
        },
        {
            "Multiple SETs",
            "SET useMultistageEngine=true; SET timeoutMs=10000; SELECT * FROM products",
            new String[]{"products"},
            false
        },
        {
            "SET + JOIN",
            "SET useMultistageEngine=true; SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id",
            new String[]{"users", "orders"},
            false
        },

        // Complex queries
        {
            "Complex query with all features",
            "SET useMultistageEngine=true; "
                + "WITH user_stats AS (SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id) "
                + "SELECT u.name, us.order_count "
                + "FROM users u "
                + "JOIN user_stats us ON u.id = us.user_id "
                + "JOIN (SELECT user_id FROM products WHERE category = 'electronics') p ON u.id = p.user_id "
                + "WHERE us.order_count > 5 "
                + "ORDER BY us.order_count DESC",
            new String[]{"orders", "users", "products"},
            false
        },

        // Edge cases
        {
            "Table with underscore",
            "SELECT * FROM user_profiles",
            new String[]{"user_profiles"},
            false
        },
        {
            "Table with numbers",
            "SELECT * FROM table_2024",
            new String[]{"table_2024"},
            false
        },
        {
            "Multiple tables same name",
            "SELECT * FROM users u1 JOIN users u2 ON u1.id = u2.referrer_id",
            new String[]{"users"},
            false
        },

        // Queries that should throw exception
        {
            "Only SET statements",
            "SET useMultistageEngine=true; SET timeoutMs=10000;",
            null,
            true
        },
        {
            "Empty query",
            "",
            null,
            true
        },
        {
            "Null query",
            null,
            null,
            true
        },
        {
            "Invalid SQL",
            "INVALID SQL QUERY",
            null,
            true
        },

        // Additional queries from BaseClusterIntegrationTestSet
        // Basic aggregation queries
        {
            "SUM INTEGER",
            "SELECT SUM(ActualElapsedTime) FROM mytable",
            new String[]{"mytable"},
            false
        },
        {
            "SUM FLOAT",
            "SELECT SUM(CAST(ActualElapsedTime AS FLOAT)) FROM mytable",
            new String[]{"mytable"},
            false
        },
        {
            "SUM DOUBLE",
            "SELECT SUM(CAST(ActualElapsedTime AS DOUBLE)) FROM mytable",
            new String[]{"mytable"},
            false
        },
        {
            "COUNT with WHERE",
            "SELECT COUNT(*) FROM mytable WHERE CarrierDelay=15 AND ArrDelay > CarrierDelay LIMIT  1",
            new String[]{"mytable"},
            false
        },
        {
            "MAX MIN",
            "SELECT MAX(Quarter), MAX(FlightNum) FROM mytable LIMIT 8",
            new String[]{"mytable"},
            false
        },

        // Complex SELECT with arithmetic and functions
        {
            "Arithmetic in SELECT",
            "SELECT ArrDelay, CarrierDelay, (ArrDelay - CarrierDelay) AS diff, "
                + "substring(DestStateName, 4, 8) as stateSubStr FROM mytable WHERE CarrierDelay=15 AND "
                + "ArrDelay > CarrierDelay ORDER BY diff, ArrDelay, CarrierDelay LIMIT 100000",
            new String[]{"mytable"},
            false
        },
        {
            "Arithmetic operations",
            "SELECT ArrTime, ArrTime * 10 FROM mytable WHERE DaysSinceEpoch >= 16312",
            new String[]{"mytable"},
            false
        },
        {
            "Complex arithmetic",
            "SELECT ArrTime, ArrTime + ArrTime * 9 - ArrTime * 10 FROM mytable WHERE DaysSinceEpoch >= 16312",
            new String[]{"mytable"},
            false
        },

        // GROUP BY queries
        {
            "GROUP BY with aggregation",
            "SELECT COUNT(*), MAX(ArrTime), MIN(ArrTime), DaysSinceEpoch FROM mytable GROUP BY DaysSinceEpoch",
            new String[]{"mytable"},
            false
        },
        {
            "GROUP BY with ORDER BY",
            "SELECT DaysSinceEpoch, COUNT(*), MAX(ArrTime), MIN(ArrTime) FROM mytable GROUP BY DaysSinceEpoch",
            new String[]{"mytable"},
            false
        },

        // HAVING clauses
        {
            "HAVING clause",
            "SELECT COUNT(*) AS Count, DaysSinceEpoch FROM mytable GROUP BY DaysSinceEpoch HAVING Count > 350",
            new String[]{"mytable"},
            false
        },
        {
            "HAVING with arithmetic",
            "SELECT MAX(ArrDelay) - MAX(AirTime) AS Diff, DaysSinceEpoch FROM mytable "
                + "GROUP BY DaysSinceEpoch HAVING Diff * 2 > 1000 ORDER BY Diff ASC",
            new String[]{"mytable"},
            false
        },

        // LIKE patterns
        {
            "LIKE pattern",
            "SELECT count(*) FROM mytable WHERE OriginState LIKE 'A_'",
            new String[]{"mytable"},
            false
        },
        {
            "LIKE with %",
            "SELECT count(*) FROM mytable WHERE DestCityName LIKE 'C%'",
            new String[]{"mytable"},
            false
        },
        {
            "LIKE with _ and %",
            "SELECT count(*) FROM mytable WHERE DestCityName LIKE '_h%'",
            new String[]{"mytable"},
            false
        },

        // NOT operators
        {
            "NOT BETWEEN",
            "SELECT count(*) FROM mytable WHERE OriginState NOT BETWEEN 'DE' AND 'PA'",
            new String[]{"mytable"},
            false
        },
        {
            "NOT LIKE",
            "SELECT count(*) FROM mytable WHERE OriginState NOT LIKE 'A_'",
            new String[]{"mytable"},
            false
        },
        {
            "NOT with parentheses",
            "SELECT count(*) FROM mytable WHERE NOT (DaysSinceEpoch = 16312 AND Carrier = 'DL')",
            new String[]{"mytable"},
            false
        },

        // CAST operations
        {
            "CAST operations",
            "SELECT SUM(CAST(CAST(ArrTime AS VARCHAR) AS LONG)) FROM mytable "
                + "WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL'",
            new String[]{"mytable"},
            false
        },
        {
            "CAST with ORDER BY",
            "SELECT CAST(CAST(ArrTime AS STRING) AS BIGINT) FROM mytable "
                + "WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL' ORDER BY ArrTime DESC",
            new String[]{"mytable"},
            false
        },

        // DateTime functions
        {
            "DateTimeConvert",
            "SELECT dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS'), COUNT(*) FROM mytable "
                + "GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS') "
                + "ORDER BY COUNT(*), dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS') DESC",
            new String[]{"mytable"},
            false
        },
        {
            "TimeConvert",
            "SELECT timeConvert(DaysSinceEpoch,'DAYS','SECONDS'), COUNT(*) FROM mytable "
                + "GROUP BY timeConvert(DaysSinceEpoch,'DAYS','SECONDS') "
                + "ORDER BY COUNT(*), timeConvert(DaysSinceEpoch,'DAYS','SECONDS') DESC",
            new String[]{"mytable"},
            false
        },

        // CASE WHEN statements
        {
            "CASE WHEN with aggregation",
            "SELECT AirlineID, "
                + "CASE WHEN Sum(ArrDelay) < 0 THEN 0 WHEN SUM(ArrDelay) > 0 THEN SUM(ArrDelay) END AS SumArrDelay "
                + "FROM mytable GROUP BY AirlineID",
            new String[]{"mytable"},
            false
        },
        {
            "CASE WHEN without GROUP BY",
            "SELECT CASE WHEN Sum(ArrDelay) < 0 THEN 0 WHEN SUM(ArrDelay) > 0 THEN SUM(ArrDelay) END AS SumArrDelay "
                + "FROM mytable",
            new String[]{"mytable"},
            false
        },

        // Post-aggregation operations
        {
            "Post-aggregation in ORDER BY",
            "SELECT MAX(ArrTime) FROM mytable GROUP BY DaysSinceEpoch ORDER BY MAX(ArrTime) - MIN(ArrTime)",
            new String[]{"mytable"},
            false
        },
        {
            "Post-aggregation in SELECT",
            "SELECT MAX(ArrDelay) + MAX(AirTime) FROM mytable",
            new String[]{"mytable"},
            false
        },

        // Virtual columns (these should be treated as regular columns for table name extraction)
        {
            "Virtual columns",
            "SELECT $docId, $segmentName, $hostName FROM mytable",
            new String[]{"mytable"},
            false
        },
        {
            "Virtual columns with WHERE",
            "SELECT $docId, $segmentName, $hostName FROM mytable WHERE $docId < 5 LIMIT 50",
            new String[]{"mytable"},
            false
        },
        {
            "Virtual columns with GROUP BY",
            "SELECT max($docId) FROM mytable GROUP BY $segmentName",
            new String[]{"mytable"},
            false
        },

        // Complex WHERE conditions
        {
            "Complex WHERE with multiple conditions",
            "SELECT count(*) FROM mytable WHERE AirlineID > 20355 AND "
                + "OriginState BETWEEN 'PA' AND 'DE' AND DepTime <> 2202 LIMIT 21",
            new String[]{"mytable"},
            false
        },
        {
            "WHERE with arithmetic",
            "SELECT ArrTime, ArrTime + ArrTime * 9 - ArrTime * 10 FROM mytable WHERE ArrTime - 100 > 0",
            new String[]{"mytable"},
            false
        },

        // Subquery patterns (from V2 tests)
        {
            "IN_SUBQUERY",
            "SELECT COUNT(*) FROM mytable WHERE INSUBQUERY(DestAirportID, "
                + "'SELECT IDSET(DestAirportID) FROM mytable WHERE DaysSinceEpoch = 16430') = 1",
            new String[]{"mytable"},
            false
        },
        {
            "NOT IN_SUBQUERY",
            "SELECT COUNT(*) FROM mytable WHERE INSUBQUERY(DestAirportID, "
                + "'SELECT IDSET(DestAirportID) FROM mytable WHERE DaysSinceEpoch = 16430') = 0",
            new String[]{"mytable"},
            false
        },

        // Multi-value column queries
        {
            "Multi-value IN",
            "SELECT DistanceGroup FROM mytable WHERE \"Month\" BETWEEN 1 AND 1 AND "
                + "arrayToMV(DivAirportSeqIDs) IN (1078102, 1142303, 1530402, 1172102, 1291503) OR SecurityDelay IN "
                + "(1, 0, 14, -9999) LIMIT 10",
            new String[]{"mytable"},
            false
        },

        // Options and hints
        {
            "Query with options",
            "SELECT count(*) FROM mytable WHERE OriginState LIKE 'A_' option(orderedPreferredPools=0|1)",
            new String[]{"mytable"},
            false
        },
        {
            "SET with query",
            "SET orderedPreferredPools='0 | 1'; SELECT count(*) FROM mytable WHERE OriginState LIKE 'A_'",
            new String[]{"mytable"},
            false
        }
    };
  }

  /**
   * Test method that uses the DataProvider to test multiple SQL queries.
   * This makes it easy to add new test cases by simply adding entries to the data provider.
   *
   * @param testName The name of the test case for better reporting
   * @param sqlQuery The SQL query to test
   * @param expectedTableNames The expected table names that should be extracted
   */
  @Test(dataProvider = "sqlQueries")
  public void testResolveTableNameWithDataProvider(String testName, String sqlQuery, String[] expectedTableNames,
      boolean throwException) {
    try {
      // Extract table names from the SQL query
      String[] actualTableNames = TableNameExtractor.resolveTableName(sqlQuery);

      if (expectedTableNames == null) {
        // For queries that should return null (invalid, empty, etc.)
        assertNull(actualTableNames, "Query should return null: " + testName);
      } else {
        // For valid queries, check that we got the expected table names
        assertNotNull(actualTableNames, "Table names should not be null for: " + testName);
        assertEquals(actualTableNames.length, expectedTableNames.length,
            "Should extract correct number of tables for: " + testName);

        // Convert arrays to sets for order-independent comparison
        Set<String> actualSet = new HashSet<>(Arrays.asList(actualTableNames));
        Set<String> expectedSet = new HashSet<>(Arrays.asList(expectedTableNames));

        assertEquals(actualSet, expectedSet,
            "Should extract correct table names for: " + testName);
      }
    } catch (Exception e) {
      assertTrue(throwException);
    }
  }
}
