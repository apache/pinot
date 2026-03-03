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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for {@link InClauseSizeValidationVisitor}.
 */
public class InClauseSizeValidationVisitorTest {

  private static final Map<String, Schema> TABLE_SCHEMAS = new HashMap<>();
  private static final Map<String, List<String>> SERVER1_SEGMENTS = Map.of("test_OFFLINE", List.of("seg1"));
  private static final Map<String, List<String>> SERVER2_SEGMENTS = Map.of("test_OFFLINE", List.of("seg2"));

  static {
    TABLE_SCHEMAS.put("test_OFFLINE", new Schema.SchemaBuilder()
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("col2", FieldSpec.DataType.STRING)
        .addMetric("col3", FieldSpec.DataType.INT)
        .setSchemaName("test")
        .build());
  }

  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp() {
    // Set up with a max IN clause limit of 10 for testing
    _queryEnvironment = getQueryEnvironmentWithMaxInClauseElements(10);
  }

  private QueryEnvironment getQueryEnvironmentWithMaxInClauseElements(int maxInClauseElements) {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    for (Map.Entry<String, Schema> entry : TABLE_SCHEMAS.entrySet()) {
      factory.registerTable(entry.getValue(), entry.getKey());
    }
    for (Map.Entry<String, List<String>> entry : SERVER1_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(1, entry.getKey(), segment);
      }
    }
    for (Map.Entry<String, List<String>> entry : SERVER2_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(2, entry.getKey(), segment);
      }
    }
    RoutingManager routingManager = factory.buildRoutingManager(null);
    return new QueryEnvironment(
        QueryEnvironment.configBuilder()
            .requestId(1L)
            .database(CommonConstants.DEFAULT_DATABASE)
            .tableCache(factory.buildTableCache())
            .workerManager(new WorkerManager("Broker_localhost", "localhost", 3, routingManager))
            .defaultMseMaxInClauseElements(maxInClauseElements)
            .build());
  }

  @Test
  public void testSmallInClauseAllowed() {
    // IN clause with 5 elements (less than limit of 10) should pass
    String query = "SELECT * FROM test WHERE col1 IN ('a', 'b', 'c', 'd', 'e')";
    // Should not throw
    assertNotNull(_queryEnvironment.planQuery(query));
  }

  @Test
  public void testInClauseAtLimitAllowed() {
    // IN clause with exactly 10 elements (at limit) should pass
    String values = IntStream.range(0, 10)
        .mapToObj(i -> "'" + i + "'")
        .collect(Collectors.joining(", "));
    String query = "SELECT * FROM test WHERE col1 IN (" + values + ")";
    // Should not throw
    assertNotNull(_queryEnvironment.planQuery(query));
  }

  @Test
  public void testLargeInClauseRejected() {
    // IN clause with 11 elements (exceeds limit of 10) should be rejected
    String values = IntStream.range(0, 11)
        .mapToObj(i -> "'" + i + "'")
        .collect(Collectors.joining(", "));
    String query = "SELECT * FROM test WHERE col1 IN (" + values + ")";

    try {
      _queryEnvironment.planQuery(query);
      fail("Expected QueryException for IN clause exceeding limit");
    } catch (QueryException e) {
      assertTrue(e.getMessage().contains("IN clause contains 11 elements"),
          "Exception message should mention element count");
      assertTrue(e.getMessage().contains("maximum allowed limit of 10"),
          "Exception message should mention the limit");
    }
  }

  @Test
  public void testNotInClauseValidated() {
    // NOT IN clause with 11 elements (exceeds limit of 10) should also be rejected
    String values = IntStream.range(0, 11)
        .mapToObj(i -> "'" + i + "'")
        .collect(Collectors.joining(", "));
    String query = "SELECT * FROM test WHERE col1 NOT IN (" + values + ")";

    try {
      _queryEnvironment.planQuery(query);
      fail("Expected QueryException for NOT IN clause exceeding limit");
    } catch (QueryException e) {
      assertTrue(e.getMessage().contains("IN clause contains 11 elements"),
          "Exception message should mention element count");
    }
  }

  @Test
  public void testMultipleInClausesValidated() {
    // Both IN clauses should be validated
    String values = IntStream.range(0, 11)
        .mapToObj(i -> "'" + i + "'")
        .collect(Collectors.joining(", "));
    String query = "SELECT * FROM test WHERE col1 IN ('a', 'b') AND col2 IN (" + values + ")";

    try {
      _queryEnvironment.planQuery(query);
      fail("Expected QueryException for IN clause exceeding limit");
    } catch (QueryException e) {
      assertTrue(e.getMessage().contains("IN clause contains 11 elements"));
    }
  }

  @Test
  public void testNestedInClauseValidated() {
    // IN clause in subquery should also be validated
    String values = IntStream.range(0, 11)
        .mapToObj(i -> "'" + i + "'")
        .collect(Collectors.joining(", "));
    String query = "SELECT * FROM test WHERE col3 IN (SELECT col3 FROM test WHERE col1 IN (" + values + "))";

    try {
      _queryEnvironment.planQuery(query);
      fail("Expected QueryException for nested IN clause exceeding limit");
    } catch (QueryException e) {
      assertTrue(e.getMessage().contains("IN clause contains 11 elements"));
    }
  }

  @Test
  public void testInClauseWithSubqueryNotCounted() {
    // IN clause with subquery (not a value list) should not be rejected based on element count
    String query = "SELECT * FROM test WHERE col1 IN (SELECT col1 FROM test WHERE col3 > 0)";
    // Should not throw (subquery operand is not a SqlNodeList)
    assertNotNull(_queryEnvironment.planQuery(query));
  }

  @Test
  public void testValidationDisabledWithZeroLimit() {
    // When limit is 0, validation should be disabled
    QueryEnvironment env = getQueryEnvironmentWithMaxInClauseElements(0);
    String values = IntStream.range(0, 100)
        .mapToObj(i -> "'" + i + "'")
        .collect(Collectors.joining(", "));
    String query = "SELECT * FROM test WHERE col1 IN (" + values + ")";
    // Should not throw even with 100 elements
    assertNotNull(env.planQuery(query));
  }

  @Test
  public void testValidationDisabledWithNegativeLimit() {
    // When limit is negative, validation should be disabled
    QueryEnvironment env = getQueryEnvironmentWithMaxInClauseElements(-1);
    String values = IntStream.range(0, 100)
        .mapToObj(i -> "'" + i + "'")
        .collect(Collectors.joining(", "));
    String query = "SELECT * FROM test WHERE col1 IN (" + values + ")";
    // Should not throw even with 100 elements
    assertNotNull(env.planQuery(query));
  }

  @Test
  public void testInClauseWithNumericValues() {
    // IN clause with numeric values
    String values = IntStream.range(0, 11)
        .mapToObj(String::valueOf)
        .collect(Collectors.joining(", "));
    String query = "SELECT * FROM test WHERE col3 IN (" + values + ")";

    try {
      _queryEnvironment.planQuery(query);
      fail("Expected QueryException for IN clause exceeding limit");
    } catch (QueryException e) {
      assertTrue(e.getMessage().contains("IN clause contains 11 elements"));
    }
  }

  @DataProvider(name = "validQueriesProvider")
  public Object[][] validQueriesProvider() {
    return new Object[][] {
        // Empty IN clause (valid SQL, just no results)
        {"SELECT * FROM test WHERE col1 IN ('a')"},
        // Small IN clause
        {"SELECT * FROM test WHERE col1 IN ('a', 'b', 'c')"},
        // IN clause in different positions
        {"SELECT * FROM test WHERE col1 = 'x' OR col2 IN ('a', 'b')"},
        {"SELECT * FROM test WHERE col1 IN ('a', 'b') ORDER BY col3"},
        // Combined with other conditions
        {"SELECT * FROM test WHERE col1 IN ('a', 'b') AND col3 > 0"},
    };
  }

  @Test(dataProvider = "validQueriesProvider")
  public void testValidQueries(String query) {
    // These queries should all pass validation
    assertNotNull(_queryEnvironment.planQuery(query));
  }
}
