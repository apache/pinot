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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.graph.spi.GraphSchemaConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * End-to-end tests: Cypher query in, SQL string out.
 */
public class CypherToSqlTranslatorTest {

  private CypherToSqlTranslator _translator;

  @BeforeMethod
  public void setUp() {
    _translator = new CypherToSqlTranslator(createTestSchema());
  }

  @Test
  public void testEndToEndBasicQuery() {
    String cypher = "MATCH (a:User {id: '123'})-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = '123' "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndMultipleReturnItems() {
    String cypher = "MATCH (a:User {id: '42'})-[:FOLLOWS]->(b:User) RETURN b.id, b.name";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id, b.user_name FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = '42'");
  }

  @Test
  public void testEndToEndNoFilters() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 50";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "LIMIT 50");
  }

  @Test
  public void testEndToEndIncomingEdge() {
    String cypher = "MATCH (a:User {id: '99'})<-[:FOLLOWS]-(b:User) RETURN b.id";
    String sql = _translator.translate(cypher);

    // Incoming: a is the followee (target in schema), b is the follower (source in schema)
    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.followee_id = a.user_id "
            + "JOIN users_table AS b ON e.follower_id = b.user_id "
            + "WHERE a.user_id = '99'");
  }

  @Test
  public void testEndToEndCaseInsensitiveKeywords() {
    String cypher = "match (a:User {id: '1'})-[:FOLLOWS]->(b:User) return b.id limit 10";
    String sql = _translator.translate(cypher);

    assertTrue(sql.contains("SELECT b.user_id"));
    assertTrue(sql.contains("LIMIT 10"));
  }

  @Test(expectedExceptions = CypherParseException.class, expectedExceptionsMessageRegExp = ".*Unknown vertex label.*")
  public void testRejectUnknownVertexLabel() {
    _translator.translate("MATCH (a:Unknown {id: '1'})-[:FOLLOWS]->(b:User) RETURN b.id");
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*Unknown relationship type.*")
  public void testRejectUnknownEdgeType() {
    _translator.translate("MATCH (a:User {id: '1'})-[:UNKNOWN]->(b:User) RETURN b.id");
  }

  @Test(expectedExceptions = CypherParseException.class, expectedExceptionsMessageRegExp = ".*unknown alias.*")
  public void testRejectInvalidReturnAlias() {
    _translator.translate("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN c.id");
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*Relationship type must be specified.*")
  public void testRejectUntypedRelationship() {
    _translator.translate("MATCH (a:User)-[]->(b:User) RETURN b.id");
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*Source node label.*does not match.*")
  public void testRejectMismatchedSourceLabel() {
    // Create a schema where FOLLOWS goes from User to User, but query says Post->User
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSchemaWithPost());
    translator.translate("MATCH (a:Post)-[:FOLLOWS]->(b:User) RETURN b.id");
  }

  @Test
  public void testEndToEndWithDifferentVertexLabels() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSchemaWithPost());
    String sql = translator.translate("MATCH (a:User {id: '1'})-[:AUTHORED]->(b:Post) RETURN b.title");

    assertEquals(sql,
        "SELECT b.post_title FROM authored_table AS e "
            + "JOIN users_table AS a ON e.author_id = a.user_id "
            + "JOIN posts_table AS b ON e.post_id = b.post_id "
            + "WHERE a.user_id = '1'");
  }

  @Test
  public void testEndToEndDistinct() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN DISTINCT b.id LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT DISTINCT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndBooleanProperty() {
    String cypher = "MATCH (a:User {active: true})-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.active = true "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndBooleanFalseWithDistinct() {
    String cypher = "MATCH (a:User {active: false})-[:FOLLOWS]->(b:User) RETURN DISTINCT b.name LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT DISTINCT b.user_name FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.active = false "
            + "LIMIT 100");
  }

  // ---- WHERE clause end-to-end tests ----

  @Test
  public void testEndToEndWhereEquals() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.id = 1 RETURN b.id LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndWhereAnd() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.age > 25 AND b.age < 30 RETURN b.name LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_name FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE (a.age > 25 AND b.age < 30) "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndWhereOr() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' OR a.name = 'Bob' RETURN b.id LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE (a.user_name = 'Alice' OR a.user_name = 'Bob') "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndWhereNot() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE NOT a.active = false RETURN b.id LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE (NOT a.active = false) "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndWhereWithParentheses() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) "
            + "WHERE a.id = 1 AND (b.name = 'Bob' OR b.name = 'Charlie') RETURN b.id LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE (a.user_id = 1 AND (b.user_name = 'Bob' OR b.user_name = 'Charlie')) "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndWhereWithInlineFilters() {
    String cypher =
        "MATCH (a:User {id: '123'})-[:FOLLOWS]->(b:User) WHERE b.name = 'Alice' RETURN b.id";
    String sql = _translator.translate(cypher);

    // Both inline filter and WHERE condition should appear in WHERE clause
    assertTrue(sql.contains("a.user_id = '123'"));
    assertTrue(sql.contains("b.user_name = 'Alice'"));
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*unknown alias.*")
  public void testRejectWhereWithUnknownAlias() {
    _translator.translate(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE c.id = 1 RETURN b.id");
  }

  // ---- COUNT aggregation end-to-end tests ----

  @Test
  public void testEndToEndCountAggregation() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN count(b.id) LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT COUNT(b.user_id) FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndCountStar() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN count(*) LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT COUNT(*) FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndCountDistinct() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, count(DISTINCT b.id) LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT a.user_name, COUNT(DISTINCT b.user_id) FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "GROUP BY a.user_name "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndMixedReturnWithCount() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.id, count(b.id) LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT a.user_id, COUNT(b.user_id) FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "GROUP BY a.user_id "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndCountAlias() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN count(b) LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT COUNT(b.user_id) FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  // ---- ORDER BY end-to-end tests ----

  @Test
  public void testEndToEndOrderBy() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN b.id ORDER BY b.name LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "ORDER BY b.user_name ASC "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndOrderByDesc() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN b.name ORDER BY b.name DESC LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_name FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "ORDER BY b.user_name DESC "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndOrderByMultipleItems() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.id, b.id ORDER BY a.id ASC, b.id DESC LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT a.user_id, b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "ORDER BY a.user_id ASC, b.user_id DESC "
            + "LIMIT 100");
  }

  // ---- RETURN AS alias end-to-end tests ----

  @Test
  public void testEndToEndReturnAs() {
    String cypher =
        "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN b.id AS userId, b.name AS userName ORDER BY b.name "
            + "LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id AS userId, b.user_name AS userName FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "ORDER BY b.user_name ASC "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndReturnAsWithoutOrderBy() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN b.id AS userId LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id AS userId FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*ORDER BY item references unknown alias.*")
  public void testRejectOrderByWithUnknownAlias() {
    _translator.translate(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id ORDER BY c.id");
  }

  // ---- SKIP end-to-end tests ----

  @Test
  public void testEndToEndSkip() {
    String cypher =
        "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN b.id ORDER BY b.name SKIP 20 LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "ORDER BY b.user_name ASC "
            + "LIMIT 100 OFFSET 20");
  }

  @Test
  public void testEndToEndSkipWithoutLimit() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id SKIP 10";
    String sql = _translator.translate(cypher);

    assertTrue(sql.contains("OFFSET 10"));
    assertFalse(sql.contains("LIMIT"));
  }

  @Test
  public void testEndToEndSkipWithLimit() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id SKIP 5 LIMIT 50";
    String sql = _translator.translate(cypher);

    assertTrue(sql.contains("LIMIT 50 OFFSET 5"));
  }

  // ---- SUM/AVG/MIN/MAX end-to-end tests ----

  @Test
  public void testEndToEndSumAggregation() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN sum(b.age) LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT SUM(b.age) FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndAvgAggregation() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN avg(b.age) LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT AVG(b.age) FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndMinAggregation() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN min(b.age) LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT MIN(b.age) FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndMaxAggregation() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN max(b.age) LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT MAX(b.age) FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndMixedReturnWithSum() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.id, sum(b.age) LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT a.user_id, SUM(b.age) FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "GROUP BY a.user_id "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndSumWithResultAlias() {
    String cypher =
        "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN sum(b.age) AS totalAge LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT SUM(b.age) AS totalAge FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  // ---- String predicate end-to-end tests ----

  @Test
  public void testEndToEndStartsWith() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name STARTS WITH 'Al' RETURN b.id LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_name LIKE 'Al%' "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndEndsWith() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name ENDS WITH 'ice' RETURN b.id LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_name LIKE '%ice' "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndContains() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name CONTAINS 'lic' RETURN b.id LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_name LIKE '%lic%' "
            + "LIMIT 100");
  }

  // ---- IN list predicate end-to-end tests ----

  @Test
  public void testEndToEndInIntegerList() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.id IN [1, 2, 3] RETURN b.id LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id IN (1, 2, 3) "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndInStringList() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name IN ['Alice', 'Bob'] RETURN b.id LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_name IN ('Alice', 'Bob') "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndStartsWithAndIn() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name STARTS WITH 'A' AND a.id IN [1, 2] RETURN b.id LIMIT 100";
    String sql = _translator.translate(cypher);

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE (a.user_name LIKE 'A%' AND a.user_id IN (1, 2)) "
            + "LIMIT 100");
  }

  // ---- 2-hop end-to-end tests ----

  @Test
  public void testEndToEndTwoHopOutgoing() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createTestSchema());
    String sql = translator.translate(
        "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) RETURN c.id LIMIT 100");

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
  public void testEndToEndTwoHopWithDifferentVertexLabels() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSchemaWithLikes());
    String sql = translator.translate(
        "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User)-[:LIKES]->(c:Post) RETURN c.title LIMIT 100");

    assertEquals(sql,
        "SELECT c.post_title FROM follows_table AS e1 "
            + "JOIN users_table AS a ON e1.follower_id = a.user_id "
            + "JOIN users_table AS b ON e1.followee_id = b.user_id "
            + "JOIN likes_table AS e2 ON e2.liker_id = b.user_id "
            + "JOIN posts_table AS c ON e2.post_id = c.post_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndTwoHopMixedDirection() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createTestSchema());
    String sql = translator.translate(
        "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User)<-[:FOLLOWS]-(c:User) RETURN c.id LIMIT 100");

    assertEquals(sql,
        "SELECT c.user_id FROM follows_table AS e1 "
            + "JOIN users_table AS a ON e1.follower_id = a.user_id "
            + "JOIN users_table AS b ON e1.followee_id = b.user_id "
            + "JOIN follows_table AS e2 ON e2.followee_id = b.user_id "
            + "JOIN users_table AS c ON e2.follower_id = c.user_id "
            + "WHERE a.user_id = 1 "
            + "LIMIT 100");
  }

  @Test
  public void testEndToEndTwoHopWithWhereOnLastNode() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createTestSchema());
    String sql = translator.translate(
        "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) "
            + "WHERE c.name = 'Alice' RETURN c.id LIMIT 100");

    assertTrue(sql.contains("a.user_id = 1"));
    assertTrue(sql.contains("c.user_name = 'Alice'"));
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*At most 2-hop patterns are supported.*")
  public void testEndToEndRejectThreeHop() {
    _translator.translate(
        "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)-[:FOLLOWS]->(d:User) RETURN d.id");
  }

  // ---- Helper methods ----

  private static GraphSchemaConfig createSchemaWithLikes() {
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
    GraphSchemaConfig.EdgeLabel authoredLabel =
        new GraphSchemaConfig.EdgeLabel("authored_table", "User", "author_id", "Post", "post_id",
            Collections.emptyMap());

    Map<String, GraphSchemaConfig.EdgeLabel> edgeLabels = new HashMap<>();
    edgeLabels.put("FOLLOWS", followsLabel);
    edgeLabels.put("AUTHORED", authoredLabel);

    return new GraphSchemaConfig(vertexLabels, edgeLabels);
  }
}
