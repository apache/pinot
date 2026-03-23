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
package org.apache.pinot.graph.planner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.pinot.graph.spi.GraphSchemaConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for SQL generation from Cypher IR.
 */
public class SqlGeneratorTest {

  private GraphSchemaConfig _schemaConfig;
  private SqlGenerator _sqlGenerator;

  @BeforeMethod
  public void setUp() {
    _schemaConfig = createTestSchema();
    _sqlGenerator = new SqlGenerator(_schemaConfig);
  }

  @Test
  public void testBasicOutgoingTraversal() {
    // MATCH (a:User {id: '123'})-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 100
    Map<String, Object> sourceProps = new LinkedHashMap<>();
    sourceProps.put("id", "123");

    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", sourceProps);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = '123' "
            + "LIMIT 100");
  }

  @Test
  public void testMultipleReturnColumns() {
    Map<String, Object> sourceProps = new LinkedHashMap<>();
    sourceProps.put("id", "123");

    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", sourceProps);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Arrays.asList(new CypherIR.ReturnItem("b", "id"), new CypherIR.ReturnItem("b", "name")));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertEquals(sql,
        "SELECT b.user_id, b.user_name FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = '123'");
  }

  @Test
  public void testNoPropertyFilters() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    // No WHERE clause expected
    assertFalse(sql.contains("WHERE"));
    assertTrue(sql.startsWith("SELECT b.user_id FROM follows_table AS e"));
  }

  @Test
  public void testIncomingTraversal() {
    Map<String, Object> sourceProps = new LinkedHashMap<>();
    sourceProps.put("id", "456");

    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", sourceProps);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.INCOMING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    // For incoming: source in query maps to target in schema, so:
    // JOIN source on e.followee_id = a.user_id (source sees the target key)
    // JOIN target on e.follower_id = b.user_id (target sees the source key)
    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.followee_id = a.user_id "
            + "JOIN users_table AS b ON e.follower_id = b.user_id "
            + "WHERE a.user_id = '456'");
  }

  @Test
  public void testIntegerPropertyFilter() {
    Map<String, Object> sourceProps = new LinkedHashMap<>();
    sourceProps.put("age", 25L);

    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", sourceProps);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    // 'age' is not in the property map, so it falls back to column name 'age'
    assertTrue(sql.contains("WHERE a.age = 25"));
  }

  @Test
  public void testStringEscaping() {
    Map<String, Object> sourceProps = new LinkedHashMap<>();
    sourceProps.put("name", "O'Brien");

    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", sourceProps);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("O''Brien"));
  }

  @Test
  public void testReturnAliasWithoutProperty() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", null)));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    // When no property is specified, should return primary key
    assertTrue(sql.contains("SELECT b.user_id"));
  }

  @Test
  public void testDistinctSelect() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")), true);
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    assertEquals(sql,
        "SELECT DISTINCT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "LIMIT 100");
  }

  @Test
  public void testBooleanPropertyFilter() {
    Map<String, Object> sourceProps = new LinkedHashMap<>();
    sourceProps.put("active", Boolean.TRUE);

    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", sourceProps);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("WHERE a.active = true"));
  }

  @Test
  public void testBooleanFalsePropertyFilter() {
    Map<String, Object> sourceProps = new LinkedHashMap<>();
    sourceProps.put("active", Boolean.FALSE);

    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", sourceProps);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("WHERE a.active = false"));
  }

  // ---- WHERE clause tests ----

  @Test
  public void testWhereClauseSimpleEquals() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.WhereClause whereClause = new CypherIR.WhereClause(
        new CypherIR.ComparisonPredicate("a", "id", "=", 1L));
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, whereClause, returnClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test
  public void testWhereClauseWithAndPredicate() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "name")));
    CypherIR.WhereClause whereClause = new CypherIR.WhereClause(
        new CypherIR.AndPredicate(
            new CypherIR.ComparisonPredicate("a", "age", ">", 25L),
            new CypherIR.ComparisonPredicate("b", "age", "<", 30L)));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, whereClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("WHERE (a.age > 25 AND b.age < 30)"));
  }

  @Test
  public void testWhereClauseWithOrPredicate() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.WhereClause whereClause = new CypherIR.WhereClause(
        new CypherIR.OrPredicate(
            new CypherIR.ComparisonPredicate("a", "name", "=", "Alice"),
            new CypherIR.ComparisonPredicate("a", "name", "=", "Bob")));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, whereClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("WHERE (a.user_name = 'Alice' OR a.user_name = 'Bob')"));
  }

  @Test
  public void testWhereClauseWithNotPredicate() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.WhereClause whereClause = new CypherIR.WhereClause(
        new CypherIR.NotPredicate(
            new CypherIR.ComparisonPredicate("a", "active", "=", Boolean.FALSE)));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, whereClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("WHERE (NOT a.active = false)"));
  }

  @Test
  public void testWhereClauseCombinedWithInlineFilters() {
    Map<String, Object> sourceProps = new LinkedHashMap<>();
    sourceProps.put("id", "123");

    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", sourceProps);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.WhereClause whereClause = new CypherIR.WhereClause(
        new CypherIR.ComparisonPredicate("b", "name", "=", "Alice"));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, whereClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    // Both inline filter and WHERE clause condition should be present
    assertTrue(sql.contains("a.user_id = '123'"));
    assertTrue(sql.contains("b.user_name = 'Alice'"));
    assertTrue(sql.contains("WHERE"));
  }

  @Test
  public void testWhereClausePropertyMapping() {
    // 'name' maps to 'user_name' in schema; WHERE should use the mapped column
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.WhereClause whereClause = new CypherIR.WhereClause(
        new CypherIR.ComparisonPredicate("a", "name", "<>", "test"));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, whereClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("a.user_name <> 'test'"));
  }

  // ---- COUNT aggregation tests ----

  @Test
  public void testCountAggregation() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(
            new CypherIR.ReturnItem("b", "id", CypherIR.AggregationFunction.COUNT)));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("SELECT COUNT(b.user_id)"));
    assertFalse(sql.contains("GROUP BY"));
  }

  @Test
  public void testCountStar() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(
            new CypherIR.ReturnItem(null, null, CypherIR.AggregationFunction.COUNT)));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("SELECT COUNT(*)"));
    assertFalse(sql.contains("GROUP BY"));
  }

  @Test
  public void testCountDistinct() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(
            new CypherIR.ReturnItem("b", "id", CypherIR.AggregationFunction.COUNT_DISTINCT)));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("SELECT COUNT(DISTINCT b.user_id)"));
    assertFalse(sql.contains("GROUP BY"));
  }

  @Test
  public void testMixedReturnItemsWithGroupBy() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Arrays.asList(
            new CypherIR.ReturnItem("a", "name"),
            new CypherIR.ReturnItem("b", "id", CypherIR.AggregationFunction.COUNT)));
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    assertEquals(sql,
        "SELECT a.user_name, COUNT(b.user_id) FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "GROUP BY a.user_name "
            + "LIMIT 100");
  }

  @Test
  public void testMixedReturnItemsCountDistinctWithGroupBy() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Arrays.asList(
            new CypherIR.ReturnItem("a", "name"),
            new CypherIR.ReturnItem("b", "id", CypherIR.AggregationFunction.COUNT_DISTINCT)));
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    assertEquals(sql,
        "SELECT a.user_name, COUNT(DISTINCT b.user_id) FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "GROUP BY a.user_name "
            + "LIMIT 100");
  }

  @Test
  public void testCountAliasWithoutProperty() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(
            new CypherIR.ReturnItem("b", null, CypherIR.AggregationFunction.COUNT)));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    // count(b) uses primary key
    assertTrue(sql.contains("SELECT COUNT(b.user_id)"));
  }

  // ---- ORDER BY tests ----

  @Test
  public void testOrderByAsc() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.OrderByClause orderByClause = new CypherIR.OrderByClause(
        Collections.singletonList(new CypherIR.OrderByItem("b", "name", CypherIR.SortDirection.ASC)));
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(
        matchClause, null, returnClause, orderByClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "ORDER BY b.user_name ASC "
            + "LIMIT 100");
  }

  @Test
  public void testOrderByDesc() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "name")));
    CypherIR.OrderByClause orderByClause = new CypherIR.OrderByClause(
        Collections.singletonList(new CypherIR.OrderByItem("b", "name", CypherIR.SortDirection.DESC)));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(
        matchClause, null, returnClause, orderByClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("ORDER BY b.user_name DESC"));
  }

  @Test
  public void testOrderByMultipleItems() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Arrays.asList(new CypherIR.ReturnItem("a", "id"), new CypherIR.ReturnItem("b", "id")));
    CypherIR.OrderByClause orderByClause = new CypherIR.OrderByClause(
        Arrays.asList(
            new CypherIR.OrderByItem("a", "id", CypherIR.SortDirection.ASC),
            new CypherIR.OrderByItem("b", "id", CypherIR.SortDirection.DESC)));
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(
        matchClause, null, returnClause, orderByClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("ORDER BY a.user_id ASC, b.user_id DESC"));
  }

  // ---- RETURN AS alias tests ----

  @Test
  public void testReturnAsAlias() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(
            new CypherIR.ReturnItem("b", "id", CypherIR.AggregationFunction.NONE, "userId")));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("SELECT b.user_id AS userId"));
  }

  @Test
  public void testReturnMultipleAsAliases() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Arrays.asList(
            new CypherIR.ReturnItem("b", "id", CypherIR.AggregationFunction.NONE, "userId"),
            new CypherIR.ReturnItem("b", "name", CypherIR.AggregationFunction.NONE, "userName")));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertEquals(sql,
        "SELECT b.user_id AS userId, b.user_name AS userName FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id");
  }

  @Test
  public void testReturnAsWithOrderByAndLimit() {
    Map<String, Object> sourceProps = new LinkedHashMap<>();
    sourceProps.put("id", 1L);

    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", sourceProps);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Arrays.asList(
            new CypherIR.ReturnItem("b", "id", CypherIR.AggregationFunction.NONE, "userId"),
            new CypherIR.ReturnItem("b", "name", CypherIR.AggregationFunction.NONE, "userName")));
    CypherIR.OrderByClause orderByClause = new CypherIR.OrderByClause(
        Collections.singletonList(new CypherIR.OrderByItem("b", "name", CypherIR.SortDirection.ASC)));
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(
        matchClause, null, returnClause, orderByClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    assertEquals(sql,
        "SELECT b.user_id AS userId, b.user_name AS userName FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "ORDER BY b.user_name ASC "
            + "LIMIT 100");
  }

  // ---- SKIP / OFFSET tests ----

  @Test
  public void testSkipGeneratesOffset() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.SkipClause skipClause = new CypherIR.SkipClause(20);
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(
        matchClause, null, returnClause, null, skipClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("LIMIT 100 OFFSET 20"));
  }

  @Test
  public void testSkipWithOrderBy() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.OrderByClause orderByClause = new CypherIR.OrderByClause(
        Collections.singletonList(new CypherIR.OrderByItem("b", "id", CypherIR.SortDirection.ASC)));
    CypherIR.SkipClause skipClause = new CypherIR.SkipClause(10);
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(50);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(
        matchClause, null, returnClause, orderByClause, skipClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "ORDER BY b.user_id ASC "
            + "LIMIT 50 OFFSET 10");
  }

  @Test
  public void testNoSkipNoOffset() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("LIMIT 100"));
    assertFalse(sql.contains("OFFSET"));
  }

  // ---- SUM, AVG, MIN, MAX aggregation tests ----

  @Test
  public void testSumAggregation() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(
            new CypherIR.ReturnItem("b", "name", CypherIR.AggregationFunction.SUM)));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("SELECT SUM(b.user_name)"));
    assertFalse(sql.contains("GROUP BY"));
  }

  @Test
  public void testAvgAggregation() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(
            new CypherIR.ReturnItem("b", "id", CypherIR.AggregationFunction.AVG)));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("SELECT AVG(b.user_id)"));
  }

  @Test
  public void testMinAggregation() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(
            new CypherIR.ReturnItem("b", "id", CypherIR.AggregationFunction.MIN)));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("SELECT MIN(b.user_id)"));
  }

  @Test
  public void testMaxAggregation() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(
            new CypherIR.ReturnItem("b", "id", CypherIR.AggregationFunction.MAX)));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("SELECT MAX(b.user_id)"));
  }

  @Test
  public void testMixedReturnItemsWithSumAndGroupBy() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Arrays.asList(
            new CypherIR.ReturnItem("a", "name"),
            new CypherIR.ReturnItem("b", "id", CypherIR.AggregationFunction.SUM)));
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    assertEquals(sql,
        "SELECT a.user_name, SUM(b.user_id) FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "GROUP BY a.user_name "
            + "LIMIT 100");
  }

  @Test
  public void testSumAggregationWithResultAlias() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(
            new CypherIR.ReturnItem("b", "id", CypherIR.AggregationFunction.SUM, "total")));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("SELECT SUM(b.user_id) AS total"));
  }

  // ---- String predicate tests ----

  @Test
  public void testStringPredicateStartsWith() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.WhereClause whereClause = new CypherIR.WhereClause(
        new CypherIR.StringPredicate("a", "name", CypherIR.StringOperator.STARTS_WITH, "Al"));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, whereClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("a.user_name LIKE 'Al%'"));
  }

  @Test
  public void testStringPredicateEndsWith() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.WhereClause whereClause = new CypherIR.WhereClause(
        new CypherIR.StringPredicate("a", "name", CypherIR.StringOperator.ENDS_WITH, "ice"));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, whereClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("a.user_name LIKE '%ice'"));
  }

  @Test
  public void testStringPredicateContains() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.WhereClause whereClause = new CypherIR.WhereClause(
        new CypherIR.StringPredicate("a", "name", CypherIR.StringOperator.CONTAINS, "lic"));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, whereClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("a.user_name LIKE '%lic%'"));
  }

  @Test
  public void testStringPredicateLikeEscaping() {
    // Value containing LIKE special chars % and _ should be escaped
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.WhereClause whereClause = new CypherIR.WhereClause(
        new CypherIR.StringPredicate("a", "name", CypherIR.StringOperator.CONTAINS, "100%_done"));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, whereClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    // The % and _ in the value should be escaped
    assertTrue(sql.contains("LIKE '%100\\%\\_done%'"));
  }

  // ---- IN list predicate tests ----

  @Test
  public void testInPredicateIntegerList() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.WhereClause whereClause = new CypherIR.WhereClause(
        new CypherIR.InPredicate("a", "id", Arrays.asList(1L, 2L, 3L)));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, whereClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("a.user_id IN (1, 2, 3)"));
  }

  @Test
  public void testInPredicateStringList() {
    CypherIR.NodePattern source = new CypherIR.NodePattern("a", "User", null);
    CypherIR.RelationshipPattern edge =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.NodePattern target = new CypherIR.NodePattern("b", "User", null);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(source, edge, target);
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("b", "id")));
    CypherIR.WhereClause whereClause = new CypherIR.WhereClause(
        new CypherIR.InPredicate("a", "name", Arrays.asList("Alice", "Bob")));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, whereClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    assertTrue(sql.contains("a.user_name IN ('Alice', 'Bob')"));
  }

  @Test
  public void testEscapeLikePattern() {
    assertEquals(SqlGenerator.escapeLikePattern("hello"), "hello");
    assertEquals(SqlGenerator.escapeLikePattern("100%"), "100\\%");
    assertEquals(SqlGenerator.escapeLikePattern("a_b"), "a\\_b");
    assertEquals(SqlGenerator.escapeLikePattern("100%_x"), "100\\%\\_x");
  }

  // ---- 2-hop SQL generation tests ----

  @Test
  public void testTwoHopOutgoingGeneratesFourJoins() {
    // MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) RETURN c.id LIMIT 100
    Map<String, Object> sourceProps = new LinkedHashMap<>();
    sourceProps.put("id", 1L);

    CypherIR.NodePattern nodeA = new CypherIR.NodePattern("a", "User", sourceProps);
    CypherIR.NodePattern nodeB = new CypherIR.NodePattern("b", "User", null);
    CypherIR.NodePattern nodeC = new CypherIR.NodePattern("c", "User", null);

    CypherIR.RelationshipPattern edge1 =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.RelationshipPattern edge2 =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(
        Arrays.asList(nodeA, nodeB, nodeC), Arrays.asList(edge1, edge2));
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("c", "id")));
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    assertEquals(sql,
        "SELECT c.user_id FROM follows_table AS e1 "
            + "JOIN users_table AS a ON e1.follower_id = a.user_id "
            + "JOIN users_table AS b ON e1.followee_id = b.user_id "
            + "JOIN follows_table AS e2 ON e2.follower_id = b.user_id "
            + "JOIN users_table AS c ON e2.followee_id = c.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test
  public void testTwoHopIncomingSecondEdge() {
    // MATCH (a:User)-[:FOLLOWS]->(b:User)<-[:FOLLOWS]-(c:User) RETURN c.id LIMIT 100
    CypherIR.NodePattern nodeA = new CypherIR.NodePattern("a", "User", null);
    CypherIR.NodePattern nodeB = new CypherIR.NodePattern("b", "User", null);
    CypherIR.NodePattern nodeC = new CypherIR.NodePattern("c", "User", null);

    CypherIR.RelationshipPattern edge1 =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.RelationshipPattern edge2 =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.INCOMING);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(
        Arrays.asList(nodeA, nodeB, nodeC), Arrays.asList(edge1, edge2));
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("c", "id")));
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    // For the second edge (incoming), the join keys are reversed:
    // b is the "target" of the incoming edge (maps to followee_id)
    // c is the "source" of the incoming edge (maps to follower_id)
    assertEquals(sql,
        "SELECT c.user_id FROM follows_table AS e1 "
            + "JOIN users_table AS a ON e1.follower_id = a.user_id "
            + "JOIN users_table AS b ON e1.followee_id = b.user_id "
            + "JOIN follows_table AS e2 ON e2.followee_id = b.user_id "
            + "JOIN users_table AS c ON e2.follower_id = c.user_id "
            + "LIMIT 100");
  }

  @Test
  public void testTwoHopWithWhereClause() {
    // MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) WHERE c.name = 'Alice' RETURN c.id
    Map<String, Object> sourceProps = new LinkedHashMap<>();
    sourceProps.put("id", 1L);

    CypherIR.NodePattern nodeA = new CypherIR.NodePattern("a", "User", sourceProps);
    CypherIR.NodePattern nodeB = new CypherIR.NodePattern("b", "User", null);
    CypherIR.NodePattern nodeC = new CypherIR.NodePattern("c", "User", null);

    CypherIR.RelationshipPattern edge1 =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.RelationshipPattern edge2 =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(
        Arrays.asList(nodeA, nodeB, nodeC), Arrays.asList(edge1, edge2));
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("c", "id")));
    CypherIR.WhereClause whereClause = new CypherIR.WhereClause(
        new CypherIR.ComparisonPredicate("c", "name", "=", "Alice"));

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, whereClause, returnClause, null);

    String sql = _sqlGenerator.generate(query);

    // Both inline filter on a and WHERE clause on c should appear
    assertTrue(sql.contains("a.user_id = 1"));
    assertTrue(sql.contains("c.user_name = 'Alice'"));
    assertTrue(sql.contains("WHERE"));
  }

  @Test
  public void testTwoHopWithDifferentEdgeTypes() {
    // MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User)-[:LIKES]->(c:Post) RETURN c.id LIMIT 100
    SqlGenerator generator = new SqlGenerator(createSchemaWithPost());

    Map<String, Object> sourceProps = new LinkedHashMap<>();
    sourceProps.put("id", 1L);

    CypherIR.NodePattern nodeA = new CypherIR.NodePattern("a", "User", sourceProps);
    CypherIR.NodePattern nodeB = new CypherIR.NodePattern("b", "User", null);
    CypherIR.NodePattern nodeC = new CypherIR.NodePattern("c", "Post", null);

    CypherIR.RelationshipPattern edge1 =
        new CypherIR.RelationshipPattern(null, "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.RelationshipPattern edge2 =
        new CypherIR.RelationshipPattern(null, "LIKES", CypherIR.Direction.OUTGOING);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(
        Arrays.asList(nodeA, nodeB, nodeC), Arrays.asList(edge1, edge2));
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("c", "id")));
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, limitClause);

    String sql = generator.generate(query);

    assertEquals(sql,
        "SELECT c.post_id FROM follows_table AS e1 "
            + "JOIN users_table AS a ON e1.follower_id = a.user_id "
            + "JOIN users_table AS b ON e1.followee_id = b.user_id "
            + "JOIN likes_table AS e2 ON e2.liker_id = b.user_id "
            + "JOIN posts_table AS c ON e2.post_id = c.post_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test
  public void testTwoHopWithCustomEdgeAliases() {
    // MATCH (a:User)-[r1:FOLLOWS]->(b:User)-[r2:FOLLOWS]->(c:User) RETURN c.id LIMIT 100
    CypherIR.NodePattern nodeA = new CypherIR.NodePattern("a", "User", null);
    CypherIR.NodePattern nodeB = new CypherIR.NodePattern("b", "User", null);
    CypherIR.NodePattern nodeC = new CypherIR.NodePattern("c", "User", null);

    CypherIR.RelationshipPattern edge1 =
        new CypherIR.RelationshipPattern("r1", "FOLLOWS", CypherIR.Direction.OUTGOING);
    CypherIR.RelationshipPattern edge2 =
        new CypherIR.RelationshipPattern("r2", "FOLLOWS", CypherIR.Direction.OUTGOING);

    CypherIR.MatchClause matchClause = new CypherIR.MatchClause(
        Arrays.asList(nodeA, nodeB, nodeC), Arrays.asList(edge1, edge2));
    CypherIR.ReturnClause returnClause = new CypherIR.ReturnClause(
        Collections.singletonList(new CypherIR.ReturnItem("c", "id")));
    CypherIR.LimitClause limitClause = new CypherIR.LimitClause(100);

    CypherIR.CypherQuery query = new CypherIR.CypherQuery(matchClause, returnClause, limitClause);

    String sql = _sqlGenerator.generate(query);

    // Custom edge aliases r1, r2 should be used
    assertTrue(sql.contains("follows_table AS r1"));
    assertTrue(sql.contains("follows_table AS r2"));
    assertTrue(sql.contains("r1.follower_id"));
    assertTrue(sql.contains("r2.follower_id"));
  }

  // ---- Helper methods ----

  /**
   * Creates a test schema with:
   * - Vertex label "User" backed by "users_table" with primary key "user_id"
   * - Edge label "FOLLOWS" backed by "follows_table" connecting User -> User
   */
  private static GraphSchemaConfig createTestSchema() {
    Map<String, String> userProperties = new HashMap<>();
    userProperties.put("id", "user_id");
    userProperties.put("name", "user_name");

    GraphSchemaConfig.VertexLabel userLabel =
        new GraphSchemaConfig.VertexLabel("users_table", "user_id", userProperties);

    Map<String, GraphSchemaConfig.VertexLabel> vertexLabels = new HashMap<>();
    vertexLabels.put("User", userLabel);

    GraphSchemaConfig.EdgeLabel followsLabel =
        new GraphSchemaConfig.EdgeLabel("follows_table", "User", "follower_id", "User", "followee_id",
            Collections.emptyMap());

    Map<String, GraphSchemaConfig.EdgeLabel> edgeLabels = new HashMap<>();
    edgeLabels.put("FOLLOWS", followsLabel);

    return new GraphSchemaConfig(vertexLabels, edgeLabels);
  }

  /**
   * Creates a test schema with User, Post vertices and FOLLOWS, LIKES edges.
   */
  private static GraphSchemaConfig createSchemaWithPost() {
    Map<String, String> userProperties = new HashMap<>();
    userProperties.put("id", "user_id");
    userProperties.put("name", "user_name");

    Map<String, String> postProperties = new HashMap<>();
    postProperties.put("id", "post_id");
    postProperties.put("title", "post_title");

    GraphSchemaConfig.VertexLabel userLabel =
        new GraphSchemaConfig.VertexLabel("users_table", "user_id", userProperties);
    GraphSchemaConfig.VertexLabel postLabel =
        new GraphSchemaConfig.VertexLabel("posts_table", "post_id", postProperties);

    Map<String, GraphSchemaConfig.VertexLabel> vertexLabels = new HashMap<>();
    vertexLabels.put("User", userLabel);
    vertexLabels.put("Post", postLabel);

    GraphSchemaConfig.EdgeLabel followsLabel =
        new GraphSchemaConfig.EdgeLabel("follows_table", "User", "follower_id", "User", "followee_id",
            Collections.emptyMap());
    GraphSchemaConfig.EdgeLabel likesLabel =
        new GraphSchemaConfig.EdgeLabel("likes_table", "User", "liker_id", "Post", "post_id",
            Collections.emptyMap());

    Map<String, GraphSchemaConfig.EdgeLabel> edgeLabels = new HashMap<>();
    edgeLabels.put("FOLLOWS", followsLabel);
    edgeLabels.put("LIKES", likesLabel);

    return new GraphSchemaConfig(vertexLabels, edgeLabels);
  }
}
