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

import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for the hand-written Cypher parser.
 */
public class CypherParserTest {

  @Test
  public void testBasicMatchReturnLimit() {
    String cypher = "MATCH (a:User {id: '123'})-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    // Match clause
    CypherIR.MatchClause match = query.getMatchClause();
    assertNotNull(match);

    // Source node
    CypherIR.NodePattern source = match.getSource();
    assertEquals(source.getAlias(), "a");
    assertEquals(source.getLabel(), "User");
    assertEquals(source.getProperties().size(), 1);
    assertEquals(source.getProperties().get("id"), "123");

    // Edge
    CypherIR.RelationshipPattern edge = match.getEdge();
    assertNull(edge.getAlias());
    assertEquals(edge.getType(), "FOLLOWS");
    assertEquals(edge.getDirection(), CypherIR.Direction.OUTGOING);

    // Target node
    CypherIR.NodePattern target = match.getTarget();
    assertEquals(target.getAlias(), "b");
    assertEquals(target.getLabel(), "User");
    assertTrue(target.getProperties().isEmpty());

    // Return clause
    CypherIR.ReturnClause returnClause = query.getReturnClause();
    assertEquals(returnClause.getItems().size(), 1);
    assertEquals(returnClause.getItems().get(0).getAlias(), "b");
    assertEquals(returnClause.getItems().get(0).getProperty(), "id");

    // Limit clause
    assertNotNull(query.getLimitClause());
    assertEquals(query.getLimitClause().getCount(), 100);
  }

  @Test
  public void testMultipleReturnItems() {
    String cypher = "MATCH (a:User {id: '1'})-[:FOLLOWS]->(b:User) RETURN b.id, b.name, a.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnClause returnClause = query.getReturnClause();
    assertEquals(returnClause.getItems().size(), 3);

    assertEquals(returnClause.getItems().get(0).getAlias(), "b");
    assertEquals(returnClause.getItems().get(0).getProperty(), "id");

    assertEquals(returnClause.getItems().get(1).getAlias(), "b");
    assertEquals(returnClause.getItems().get(1).getProperty(), "name");

    assertEquals(returnClause.getItems().get(2).getAlias(), "a");
    assertEquals(returnClause.getItems().get(2).getProperty(), "id");
  }

  @Test
  public void testNoLimit() {
    String cypher = "MATCH (a:User {id: '1'})-[:FOLLOWS]->(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNull(query.getLimitClause());
  }

  @Test
  public void testIntegerPropertyValue() {
    String cypher = "MATCH (a:User {age: 25})-[:FOLLOWS]->(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertEquals(query.getMatchClause().getSource().getProperties().get("age"), 25L);
  }

  @Test
  public void testMultiplePropertyFilters() {
    String cypher = "MATCH (a:User {id: '123', name: 'alice'})-[:FOLLOWS]->(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.NodePattern source = query.getMatchClause().getSource();
    assertEquals(source.getProperties().size(), 2);
    assertEquals(source.getProperties().get("id"), "123");
    assertEquals(source.getProperties().get("name"), "alice");
  }

  @Test
  public void testIncomingRelationship() {
    String cypher = "MATCH (a:User)<-[:FOLLOWS]-(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertEquals(query.getMatchClause().getEdge().getDirection(), CypherIR.Direction.INCOMING);
  }

  @Test
  public void testUndirectedRelationship() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]-(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertEquals(query.getMatchClause().getEdge().getDirection(), CypherIR.Direction.UNDIRECTED);
  }

  @Test
  public void testRelationshipWithAlias() {
    String cypher = "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertEquals(query.getMatchClause().getEdge().getAlias(), "r");
    assertEquals(query.getMatchClause().getEdge().getType(), "FOLLOWS");
  }

  @Test
  public void testNodeWithoutProperties() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertTrue(query.getMatchClause().getSource().getProperties().isEmpty());
  }

  @Test
  public void testCaseInsensitiveKeywords() {
    String cypher = "match (a:User)-[:FOLLOWS]->(b:User) return b.id limit 10";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNotNull(query.getMatchClause());
    assertNotNull(query.getReturnClause());
    assertEquals(query.getLimitClause().getCount(), 10);
  }

  @Test
  public void testReturnAliasWithoutProperty() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertEquals(item.getAlias(), "b");
    assertNull(item.getProperty());
  }

  // ---- Rejection tests ----

  @Test(expectedExceptions = CypherParseException.class, expectedExceptionsMessageRegExp = ".*OPTIONAL MATCH.*")
  public void testRejectOptionalMatch() {
    new CypherParser("OPTIONAL MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id").parse();
  }

  @Test(expectedExceptions = CypherParseException.class, expectedExceptionsMessageRegExp = ".*CREATE.*")
  public void testRejectCreate() {
    new CypherParser("CREATE (a:User {id: '1'})").parse();
  }

  @Test(expectedExceptions = CypherParseException.class, expectedExceptionsMessageRegExp = ".*MERGE.*")
  public void testRejectMerge() {
    new CypherParser("MERGE (a:User {id: '1'})").parse();
  }

  @Test(expectedExceptions = CypherParseException.class, expectedExceptionsMessageRegExp = ".*DELETE.*")
  public void testRejectDelete() {
    new CypherParser("MATCH (a:User) DELETE a").parse();
  }

  @Test(expectedExceptions = CypherParseException.class, expectedExceptionsMessageRegExp = ".*SET.*")
  public void testRejectSet() {
    new CypherParser("MATCH (a:User) SET a.name = 'bob'").parse();
  }

  @Test(expectedExceptions = CypherParseException.class, expectedExceptionsMessageRegExp = ".*REMOVE.*")
  public void testRejectRemove() {
    new CypherParser("MATCH (a:User) REMOVE a.name").parse();
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*Multiple MATCH clauses.*")
  public void testRejectWith() {
    // WITH is no longer rejected at the keyword level (needed for STARTS WITH / ENDS WITH),
    // but the WITH clause usage still fails because the grammar does not support it.
    new CypherParser("MATCH (a:User) WITH a MATCH (a)-[:FOLLOWS]->(b:User) RETURN b.id").parse();
  }

  @Test(expectedExceptions = CypherParseException.class, expectedExceptionsMessageRegExp = ".*UNION.*")
  public void testRejectUnion() {
    new CypherParser(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id UNION MATCH (a:User)-[:LIKES]->(b:User) RETURN b.id")
        .parse();
  }

  // ---- SKIP tests ----

  @Test
  public void testSkipAfterOrderBy() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id ORDER BY b.id SKIP 20 LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNotNull(query.getSkipClause());
    assertEquals(query.getSkipClause().getCount(), 20);
    assertNotNull(query.getLimitClause());
    assertEquals(query.getLimitClause().getCount(), 100);
  }

  @Test
  public void testSkipWithoutLimit() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id SKIP 10";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNotNull(query.getSkipClause());
    assertEquals(query.getSkipClause().getCount(), 10);
    assertNull(query.getLimitClause());
  }

  @Test
  public void testSkipWithLimitNoOrderBy() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id SKIP 5 LIMIT 50";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNull(query.getOrderByClause());
    assertNotNull(query.getSkipClause());
    assertEquals(query.getSkipClause().getCount(), 5);
    assertNotNull(query.getLimitClause());
    assertEquals(query.getLimitClause().getCount(), 50);
  }

  @Test
  public void testSkipZero() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id SKIP 0 LIMIT 10";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNotNull(query.getSkipClause());
    assertEquals(query.getSkipClause().getCount(), 0);
  }

  @Test
  public void testNoSkipClause() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNull(query.getSkipClause());
  }

  @Test
  public void testSkipCaseInsensitive() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id skip 20 limit 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNotNull(query.getSkipClause());
    assertEquals(query.getSkipClause().getCount(), 20);
  }

  @Test
  public void testReturnDistinct() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN DISTINCT b.id LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertTrue(query.getReturnClause().isDistinct());
    assertEquals(query.getReturnClause().getItems().size(), 1);
    assertEquals(query.getReturnClause().getItems().get(0).getAlias(), "b");
    assertEquals(query.getReturnClause().getItems().get(0).getProperty(), "id");
  }

  @Test
  public void testReturnWithoutDistinct() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertFalse(query.getReturnClause().isDistinct());
  }

  @Test
  public void testBooleanTruePropertyValue() {
    String cypher = "MATCH (a:User {active: true})-[:FOLLOWS]->(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertEquals(query.getMatchClause().getSource().getProperties().get("active"), Boolean.TRUE);
  }

  @Test
  public void testBooleanFalsePropertyValue() {
    String cypher = "MATCH (a:User {active: false})-[:FOLLOWS]->(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertEquals(query.getMatchClause().getSource().getProperties().get("active"), Boolean.FALSE);
  }

  @Test
  public void testBooleanCaseInsensitive() {
    // TRUE, True, true should all work
    for (String variant : new String[]{"TRUE", "True", "true"}) {
      String cypher = "MATCH (a:User {active: " + variant + "})-[:FOLLOWS]->(b:User) RETURN b.id";
      CypherParser parser = new CypherParser(cypher);
      CypherIR.CypherQuery query = parser.parse();

      assertEquals(query.getMatchClause().getSource().getProperties().get("active"), Boolean.TRUE,
          "Failed for variant: " + variant);
    }
  }

  @Test
  public void testDistinctCaseInsensitive() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN distinct b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertTrue(query.getReturnClause().isDistinct());
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*Variable-length paths.*")
  public void testRejectVariableLengthPaths() {
    new CypherParser("MATCH (a:User)-[*1..3]->(b:User) RETURN b.id").parse();
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*Multiple MATCH clauses.*")
  public void testRejectMultipleMatchClauses() {
    new CypherParser("MATCH (a:User)-[:FOLLOWS]->(b:User) MATCH (b)-[:LIKES]->(c:Post) RETURN c.id").parse();
  }

  @Test(expectedExceptions = CypherParseException.class, expectedExceptionsMessageRegExp = ".*Expected MATCH.*")
  public void testRejectEmptyQuery() {
    new CypherParser("").parse();
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*Unterminated string.*")
  public void testRejectUnterminatedString() {
    new CypherParser("MATCH (a:User {id: 'abc})-[:FOLLOWS]->(b:User) RETURN b.id").parse();
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*RETURN \\* is not supported.*")
  public void testRejectReturnStar() {
    new CypherParser("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN *").parse();
  }

  // ---- WHERE clause tests ----

  @Test
  public void testWhereEquals() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.id = 1 RETURN b.id LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNotNull(query.getWhereClause());
    CypherIR.Predicate pred = query.getWhereClause().getPredicate();
    assertTrue(pred instanceof CypherIR.ComparisonPredicate);
    CypherIR.ComparisonPredicate comp = (CypherIR.ComparisonPredicate) pred;
    assertEquals(comp.getAlias(), "a");
    assertEquals(comp.getProperty(), "id");
    assertEquals(comp.getOperator(), "=");
    assertEquals(comp.getValue(), 1L);
  }

  @Test
  public void testWhereStringValue() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ComparisonPredicate comp = (CypherIR.ComparisonPredicate) query.getWhereClause().getPredicate();
    assertEquals(comp.getAlias(), "a");
    assertEquals(comp.getProperty(), "name");
    assertEquals(comp.getOperator(), "=");
    assertEquals(comp.getValue(), "Alice");
  }

  @Test
  public void testWhereNotEquals() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.id <> 1 RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ComparisonPredicate comp = (CypherIR.ComparisonPredicate) query.getWhereClause().getPredicate();
    assertEquals(comp.getOperator(), "<>");
  }

  @Test
  public void testWhereLessThan() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.age < 30 RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ComparisonPredicate comp = (CypherIR.ComparisonPredicate) query.getWhereClause().getPredicate();
    assertEquals(comp.getOperator(), "<");
    assertEquals(comp.getValue(), 30L);
  }

  @Test
  public void testWhereGreaterThan() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.age > 25 RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ComparisonPredicate comp = (CypherIR.ComparisonPredicate) query.getWhereClause().getPredicate();
    assertEquals(comp.getOperator(), ">");
    assertEquals(comp.getValue(), 25L);
  }

  @Test
  public void testWhereLessEqual() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.age <= 30 RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ComparisonPredicate comp = (CypherIR.ComparisonPredicate) query.getWhereClause().getPredicate();
    assertEquals(comp.getOperator(), "<=");
  }

  @Test
  public void testWhereGreaterEqual() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.age >= 25 RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ComparisonPredicate comp = (CypherIR.ComparisonPredicate) query.getWhereClause().getPredicate();
    assertEquals(comp.getOperator(), ">=");
  }

  @Test
  public void testWhereAnd() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.age > 25 AND b.age < 30 RETURN b.name";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.Predicate pred = query.getWhereClause().getPredicate();
    assertTrue(pred instanceof CypherIR.AndPredicate);
    CypherIR.AndPredicate and = (CypherIR.AndPredicate) pred;

    CypherIR.ComparisonPredicate left = (CypherIR.ComparisonPredicate) and.getLeft();
    assertEquals(left.getAlias(), "a");
    assertEquals(left.getProperty(), "age");
    assertEquals(left.getOperator(), ">");
    assertEquals(left.getValue(), 25L);

    CypherIR.ComparisonPredicate right = (CypherIR.ComparisonPredicate) and.getRight();
    assertEquals(right.getAlias(), "b");
    assertEquals(right.getProperty(), "age");
    assertEquals(right.getOperator(), "<");
    assertEquals(right.getValue(), 30L);
  }

  @Test
  public void testWhereOr() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' OR a.name = 'Bob' RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.Predicate pred = query.getWhereClause().getPredicate();
    assertTrue(pred instanceof CypherIR.OrPredicate);
    CypherIR.OrPredicate or = (CypherIR.OrPredicate) pred;

    CypherIR.ComparisonPredicate left = (CypherIR.ComparisonPredicate) or.getLeft();
    assertEquals(left.getValue(), "Alice");

    CypherIR.ComparisonPredicate right = (CypherIR.ComparisonPredicate) or.getRight();
    assertEquals(right.getValue(), "Bob");
  }

  @Test
  public void testWhereNot() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE NOT a.active = false RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.Predicate pred = query.getWhereClause().getPredicate();
    assertTrue(pred instanceof CypherIR.NotPredicate);
    CypherIR.NotPredicate not = (CypherIR.NotPredicate) pred;

    CypherIR.ComparisonPredicate inner = (CypherIR.ComparisonPredicate) not.getInner();
    assertEquals(inner.getAlias(), "a");
    assertEquals(inner.getProperty(), "active");
    assertEquals(inner.getOperator(), "=");
    assertEquals(inner.getValue(), Boolean.FALSE);
  }

  @Test
  public void testWhereParentheses() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.id = 1 AND (b.name = 'Bob' OR b.name = 'Charlie') RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    // Top level should be AND
    CypherIR.AndPredicate and = (CypherIR.AndPredicate) query.getWhereClause().getPredicate();

    // Left side: a.id = 1
    CypherIR.ComparisonPredicate left = (CypherIR.ComparisonPredicate) and.getLeft();
    assertEquals(left.getAlias(), "a");
    assertEquals(left.getValue(), 1L);

    // Right side: (b.name = 'Bob' OR b.name = 'Charlie')
    CypherIR.OrPredicate or = (CypherIR.OrPredicate) and.getRight();
    CypherIR.ComparisonPredicate orLeft = (CypherIR.ComparisonPredicate) or.getLeft();
    assertEquals(orLeft.getValue(), "Bob");
    CypherIR.ComparisonPredicate orRight = (CypherIR.ComparisonPredicate) or.getRight();
    assertEquals(orRight.getValue(), "Charlie");
  }

  @Test
  public void testWhereWithBooleanTrue() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.active = true RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ComparisonPredicate comp = (CypherIR.ComparisonPredicate) query.getWhereClause().getPredicate();
    assertEquals(comp.getValue(), Boolean.TRUE);
  }

  @Test
  public void testNoWhereClause() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNull(query.getWhereClause());
  }

  // ---- COUNT aggregation tests ----

  @Test
  public void testCountAggregation() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN count(b.id) LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnClause returnClause = query.getReturnClause();
    assertEquals(returnClause.getItems().size(), 1);

    CypherIR.ReturnItem item = returnClause.getItems().get(0);
    assertEquals(item.getAlias(), "b");
    assertEquals(item.getProperty(), "id");
    assertEquals(item.getAggregation(), CypherIR.AggregationFunction.COUNT);
  }

  @Test
  public void testCountStar() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN count(*) LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertNull(item.getAlias());
    assertNull(item.getProperty());
    assertEquals(item.getAggregation(), CypherIR.AggregationFunction.COUNT);
  }

  @Test
  public void testCountDistinct() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN count(DISTINCT b.id) LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertEquals(item.getAlias(), "b");
    assertEquals(item.getProperty(), "id");
    assertEquals(item.getAggregation(), CypherIR.AggregationFunction.COUNT_DISTINCT);
  }

  @Test
  public void testMixedReturnItemsWithCount() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, count(b.id) LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnClause returnClause = query.getReturnClause();
    assertEquals(returnClause.getItems().size(), 2);

    // First item: plain a.name
    CypherIR.ReturnItem first = returnClause.getItems().get(0);
    assertEquals(first.getAlias(), "a");
    assertEquals(first.getProperty(), "name");
    assertEquals(first.getAggregation(), CypherIR.AggregationFunction.NONE);

    // Second item: count(b.id)
    CypherIR.ReturnItem second = returnClause.getItems().get(1);
    assertEquals(second.getAlias(), "b");
    assertEquals(second.getProperty(), "id");
    assertEquals(second.getAggregation(), CypherIR.AggregationFunction.COUNT);
  }

  @Test
  public void testCountAlias() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN count(b) LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertEquals(item.getAlias(), "b");
    assertNull(item.getProperty());
    assertEquals(item.getAggregation(), CypherIR.AggregationFunction.COUNT);
  }

  @Test
  public void testCountCaseInsensitive() {
    // COUNT, Count, count should all work
    for (String variant : new String[]{"COUNT", "Count", "count"}) {
      String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN " + variant + "(b.id) LIMIT 10";
      CypherParser parser = new CypherParser(cypher);
      CypherIR.CypherQuery query = parser.parse();

      CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
      assertEquals(item.getAggregation(), CypherIR.AggregationFunction.COUNT,
          "Failed for variant: " + variant);
    }
  }

  @Test
  public void testCountDistinctCaseInsensitive() {
    // DISTINCT inside count should be case-insensitive
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN count(distinct b.id) LIMIT 10";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertEquals(item.getAggregation(), CypherIR.AggregationFunction.COUNT_DISTINCT);
  }

  @Test
  public void testNonAggregatedReturnItemHasNoneAggregation() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertEquals(item.getAggregation(), CypherIR.AggregationFunction.NONE);
  }

  @Test
  public void testWhereOperatorPrecedence() {
    // AND binds tighter than OR: a.x = 1 OR a.y = 2 AND a.z = 3 => a.x = 1 OR (a.y = 2 AND a.z = 3)
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.x = 1 OR a.y = 2 AND a.z = 3 RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    // Top level should be OR
    CypherIR.OrPredicate or = (CypherIR.OrPredicate) query.getWhereClause().getPredicate();

    // Left: a.x = 1
    assertTrue(or.getLeft() instanceof CypherIR.ComparisonPredicate);

    // Right: a.y = 2 AND a.z = 3
    assertTrue(or.getRight() instanceof CypherIR.AndPredicate);
  }

  // ---- ORDER BY tests ----

  @Test
  public void testOrderByBasic() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN b.id ORDER BY b.name LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNotNull(query.getOrderByClause());
    assertEquals(query.getOrderByClause().getItems().size(), 1);

    CypherIR.OrderByItem item = query.getOrderByClause().getItems().get(0);
    assertEquals(item.getAlias(), "b");
    assertEquals(item.getProperty(), "name");
    assertEquals(item.getDirection(), CypherIR.SortDirection.ASC);
  }

  @Test
  public void testOrderByDesc() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN b.name ORDER BY b.name DESC LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.OrderByItem item = query.getOrderByClause().getItems().get(0);
    assertEquals(item.getAlias(), "b");
    assertEquals(item.getProperty(), "name");
    assertEquals(item.getDirection(), CypherIR.SortDirection.DESC);
  }

  @Test
  public void testOrderByMultipleItems() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.id, b.id ORDER BY a.id ASC, b.id DESC LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNotNull(query.getOrderByClause());
    assertEquals(query.getOrderByClause().getItems().size(), 2);

    CypherIR.OrderByItem first = query.getOrderByClause().getItems().get(0);
    assertEquals(first.getAlias(), "a");
    assertEquals(first.getProperty(), "id");
    assertEquals(first.getDirection(), CypherIR.SortDirection.ASC);

    CypherIR.OrderByItem second = query.getOrderByClause().getItems().get(1);
    assertEquals(second.getAlias(), "b");
    assertEquals(second.getProperty(), "id");
    assertEquals(second.getDirection(), CypherIR.SortDirection.DESC);
  }

  @Test
  public void testOrderByDefaultAsc() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id ORDER BY b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.OrderByItem item = query.getOrderByClause().getItems().get(0);
    assertEquals(item.getDirection(), CypherIR.SortDirection.ASC);
  }

  @Test
  public void testNoOrderByClause() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNull(query.getOrderByClause());
  }

  @Test
  public void testOrderByCaseInsensitive() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id order by b.id desc";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertNotNull(query.getOrderByClause());
    CypherIR.OrderByItem item = query.getOrderByClause().getItems().get(0);
    assertEquals(item.getDirection(), CypherIR.SortDirection.DESC);
  }

  // ---- RETURN AS alias tests ----

  @Test
  public void testReturnAsAlias() {
    String cypher = "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN b.id AS userId LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertEquals(item.getAlias(), "b");
    assertEquals(item.getProperty(), "id");
    assertEquals(item.getResultAlias(), "userId");
  }

  @Test
  public void testReturnMultipleAsAliases() {
    String cypher =
        "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN b.id AS userId, b.name AS userName LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnClause returnClause = query.getReturnClause();
    assertEquals(returnClause.getItems().size(), 2);

    assertEquals(returnClause.getItems().get(0).getResultAlias(), "userId");
    assertEquals(returnClause.getItems().get(1).getResultAlias(), "userName");
  }

  @Test
  public void testReturnWithoutAsAlias() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertNull(item.getResultAlias());
  }

  @Test
  public void testReturnAsWithOrderByAndLimit() {
    String cypher =
        "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User) RETURN b.id AS userId ORDER BY b.name LIMIT 10";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertEquals(query.getReturnClause().getItems().get(0).getResultAlias(), "userId");
    assertNotNull(query.getOrderByClause());
    assertEquals(query.getOrderByClause().getItems().get(0).getProperty(), "name");
    assertEquals(query.getLimitClause().getCount(), 10);
  }

  @Test
  public void testReturnAsCaseInsensitive() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id as userId";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    assertEquals(query.getReturnClause().getItems().get(0).getResultAlias(), "userId");
  }

  // ---- SUM, AVG, MIN, MAX aggregation tests ----

  @Test
  public void testSumAggregation() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN sum(b.age) LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertEquals(item.getAlias(), "b");
    assertEquals(item.getProperty(), "age");
    assertEquals(item.getAggregation(), CypherIR.AggregationFunction.SUM);
  }

  @Test
  public void testAvgAggregation() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN avg(b.age) LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertEquals(item.getAlias(), "b");
    assertEquals(item.getProperty(), "age");
    assertEquals(item.getAggregation(), CypherIR.AggregationFunction.AVG);
  }

  @Test
  public void testMinAggregation() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN min(b.age) LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertEquals(item.getAlias(), "b");
    assertEquals(item.getProperty(), "age");
    assertEquals(item.getAggregation(), CypherIR.AggregationFunction.MIN);
  }

  @Test
  public void testMaxAggregation() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN max(b.age) LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertEquals(item.getAlias(), "b");
    assertEquals(item.getProperty(), "age");
    assertEquals(item.getAggregation(), CypherIR.AggregationFunction.MAX);
  }

  @Test
  public void testSumAggregationAlias() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN sum(b) LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertEquals(item.getAlias(), "b");
    assertNull(item.getProperty());
    assertEquals(item.getAggregation(), CypherIR.AggregationFunction.SUM);
  }

  @Test
  public void testMixedReturnItemsWithSum() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, sum(b.age) LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnClause returnClause = query.getReturnClause();
    assertEquals(returnClause.getItems().size(), 2);

    CypherIR.ReturnItem first = returnClause.getItems().get(0);
    assertEquals(first.getAlias(), "a");
    assertEquals(first.getProperty(), "name");
    assertEquals(first.getAggregation(), CypherIR.AggregationFunction.NONE);

    CypherIR.ReturnItem second = returnClause.getItems().get(1);
    assertEquals(second.getAlias(), "b");
    assertEquals(second.getProperty(), "age");
    assertEquals(second.getAggregation(), CypherIR.AggregationFunction.SUM);
  }

  @Test
  public void testSumCaseInsensitive() {
    for (String variant : new String[]{"SUM", "Sum", "sum"}) {
      String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN " + variant + "(b.age) LIMIT 10";
      CypherParser parser = new CypherParser(cypher);
      CypherIR.CypherQuery query = parser.parse();

      CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
      assertEquals(item.getAggregation(), CypherIR.AggregationFunction.SUM,
          "Failed for variant: " + variant);
    }
  }

  @Test
  public void testAvgCaseInsensitive() {
    for (String variant : new String[]{"AVG", "Avg", "avg"}) {
      String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN " + variant + "(b.age) LIMIT 10";
      CypherParser parser = new CypherParser(cypher);
      CypherIR.CypherQuery query = parser.parse();

      CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
      assertEquals(item.getAggregation(), CypherIR.AggregationFunction.AVG,
          "Failed for variant: " + variant);
    }
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*SUM\\(\\*\\) is not supported.*")
  public void testRejectSumStar() {
    new CypherParser("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN sum(*) LIMIT 10").parse();
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*AVG\\(DISTINCT.*")
  public void testRejectAvgDistinct() {
    new CypherParser("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN avg(DISTINCT b.age) LIMIT 10").parse();
  }

  @Test
  public void testAggregationWithResultAlias() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN sum(b.age) AS totalAge LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.ReturnItem item = query.getReturnClause().getItems().get(0);
    assertEquals(item.getAggregation(), CypherIR.AggregationFunction.SUM);
    assertEquals(item.getResultAlias(), "totalAge");
  }

  // ---- String predicate tests ----

  @Test
  public void testWhereStartsWith() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name STARTS WITH 'Al' RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.Predicate pred = query.getWhereClause().getPredicate();
    assertTrue(pred instanceof CypherIR.StringPredicate);
    CypherIR.StringPredicate sp = (CypherIR.StringPredicate) pred;
    assertEquals(sp.getAlias(), "a");
    assertEquals(sp.getProperty(), "name");
    assertEquals(sp.getOperator(), CypherIR.StringOperator.STARTS_WITH);
    assertEquals(sp.getValue(), "Al");
  }

  @Test
  public void testWhereEndsWith() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name ENDS WITH 'ice' RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.Predicate pred = query.getWhereClause().getPredicate();
    assertTrue(pred instanceof CypherIR.StringPredicate);
    CypherIR.StringPredicate sp = (CypherIR.StringPredicate) pred;
    assertEquals(sp.getAlias(), "a");
    assertEquals(sp.getProperty(), "name");
    assertEquals(sp.getOperator(), CypherIR.StringOperator.ENDS_WITH);
    assertEquals(sp.getValue(), "ice");
  }

  @Test
  public void testWhereContains() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name CONTAINS 'lic' RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.Predicate pred = query.getWhereClause().getPredicate();
    assertTrue(pred instanceof CypherIR.StringPredicate);
    CypherIR.StringPredicate sp = (CypherIR.StringPredicate) pred;
    assertEquals(sp.getAlias(), "a");
    assertEquals(sp.getProperty(), "name");
    assertEquals(sp.getOperator(), CypherIR.StringOperator.CONTAINS);
    assertEquals(sp.getValue(), "lic");
  }

  // ---- IN list predicate tests ----

  @Test
  public void testWhereInIntegerList() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.id IN [1, 2, 3] RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.Predicate pred = query.getWhereClause().getPredicate();
    assertTrue(pred instanceof CypherIR.InPredicate);
    CypherIR.InPredicate in = (CypherIR.InPredicate) pred;
    assertEquals(in.getAlias(), "a");
    assertEquals(in.getProperty(), "id");
    assertEquals(in.getValues().size(), 3);
    assertEquals(in.getValues().get(0), 1L);
    assertEquals(in.getValues().get(1), 2L);
    assertEquals(in.getValues().get(2), 3L);
  }

  @Test
  public void testWhereInStringList() {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name IN ['Alice', 'Bob'] RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.Predicate pred = query.getWhereClause().getPredicate();
    assertTrue(pred instanceof CypherIR.InPredicate);
    CypherIR.InPredicate in = (CypherIR.InPredicate) pred;
    assertEquals(in.getAlias(), "a");
    assertEquals(in.getProperty(), "name");
    assertEquals(in.getValues().size(), 2);
    assertEquals(in.getValues().get(0), "Alice");
    assertEquals(in.getValues().get(1), "Bob");
  }

  @Test
  public void testWhereStartsWithAndIn() {
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name STARTS WITH 'A' AND a.id IN [1, 2] RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.Predicate pred = query.getWhereClause().getPredicate();
    assertTrue(pred instanceof CypherIR.AndPredicate);
    CypherIR.AndPredicate and = (CypherIR.AndPredicate) pred;

    assertTrue(and.getLeft() instanceof CypherIR.StringPredicate);
    CypherIR.StringPredicate sp = (CypherIR.StringPredicate) and.getLeft();
    assertEquals(sp.getOperator(), CypherIR.StringOperator.STARTS_WITH);
    assertEquals(sp.getValue(), "A");

    assertTrue(and.getRight() instanceof CypherIR.InPredicate);
    CypherIR.InPredicate in = (CypherIR.InPredicate) and.getRight();
    assertEquals(in.getValues().size(), 2);
  }

  @Test
  public void testWhereStringPredicateCaseInsensitive() {
    // Keywords STARTS, ENDS, CONTAINS, WITH, IN should be case-insensitive
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name starts with 'Al' RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.StringPredicate sp = (CypherIR.StringPredicate) query.getWhereClause().getPredicate();
    assertEquals(sp.getOperator(), CypherIR.StringOperator.STARTS_WITH);
  }

  // ---- 2-hop pattern tests ----

  @Test
  public void testTwoHopOutgoing() {
    String cypher =
        "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) RETURN c.id LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.MatchClause match = query.getMatchClause();
    assertEquals(match.getHopCount(), 2);
    assertEquals(match.getNodes().size(), 3);
    assertEquals(match.getEdges().size(), 2);

    // First node
    assertEquals(match.getNodes().get(0).getAlias(), "a");
    assertEquals(match.getNodes().get(0).getLabel(), "User");
    assertEquals(match.getNodes().get(0).getProperties().get("id"), 1L);

    // Second node
    assertEquals(match.getNodes().get(1).getAlias(), "b");
    assertEquals(match.getNodes().get(1).getLabel(), "User");

    // Third node
    assertEquals(match.getNodes().get(2).getAlias(), "c");
    assertEquals(match.getNodes().get(2).getLabel(), "User");

    // First edge
    assertEquals(match.getEdges().get(0).getType(), "FOLLOWS");
    assertEquals(match.getEdges().get(0).getDirection(), CypherIR.Direction.OUTGOING);

    // Second edge
    assertEquals(match.getEdges().get(1).getType(), "FOLLOWS");
    assertEquals(match.getEdges().get(1).getDirection(), CypherIR.Direction.OUTGOING);

    // Backward compat: getSource() and getTarget() still work
    assertEquals(match.getSource().getAlias(), "a");
    assertEquals(match.getTarget().getAlias(), "c");
  }

  @Test
  public void testTwoHopMixedDirection() {
    // Friends-of-friends who follow b
    String cypher =
        "MATCH (a:User)-[:FOLLOWS]->(b:User)<-[:FOLLOWS]-(c:User) RETURN c.id LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.MatchClause match = query.getMatchClause();
    assertEquals(match.getHopCount(), 2);

    // First edge: outgoing
    assertEquals(match.getEdges().get(0).getDirection(), CypherIR.Direction.OUTGOING);
    assertEquals(match.getEdges().get(0).getType(), "FOLLOWS");

    // Second edge: incoming
    assertEquals(match.getEdges().get(1).getDirection(), CypherIR.Direction.INCOMING);
    assertEquals(match.getEdges().get(1).getType(), "FOLLOWS");
  }

  @Test
  public void testTwoHopWithInlinePropertiesOnFirstNode() {
    String cypher =
        "MATCH (a:User {id: '42', name: 'alice'})-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) RETURN c.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.MatchClause match = query.getMatchClause();
    assertEquals(match.getHopCount(), 2);
    assertEquals(match.getNodes().get(0).getProperties().size(), 2);
    assertEquals(match.getNodes().get(0).getProperties().get("id"), "42");
    assertEquals(match.getNodes().get(0).getProperties().get("name"), "alice");
  }

  @Test
  public void testTwoHopWithDifferentEdgeTypes() {
    String cypher =
        "MATCH (a:User {id: 1})-[:FOLLOWS]->(b:User)-[:LIKES]->(c:Post) RETURN c.id LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.MatchClause match = query.getMatchClause();
    assertEquals(match.getHopCount(), 2);

    assertEquals(match.getEdges().get(0).getType(), "FOLLOWS");
    assertEquals(match.getEdges().get(1).getType(), "LIKES");

    assertEquals(match.getNodes().get(0).getLabel(), "User");
    assertEquals(match.getNodes().get(1).getLabel(), "User");
    assertEquals(match.getNodes().get(2).getLabel(), "Post");
  }

  @Test(expectedExceptions = CypherParseException.class,
      expectedExceptionsMessageRegExp = ".*At most 2-hop patterns are supported.*")
  public void testRejectThreeHopPattern() {
    new CypherParser(
        "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)-[:FOLLOWS]->(d:User) RETURN d.id"
    ).parse();
  }

  @Test
  public void testTwoHopWithEdgeAliases() {
    String cypher =
        "MATCH (a:User)-[r1:FOLLOWS]->(b:User)-[r2:FOLLOWS]->(c:User) RETURN c.id LIMIT 100";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.MatchClause match = query.getMatchClause();
    assertEquals(match.getEdges().get(0).getAlias(), "r1");
    assertEquals(match.getEdges().get(1).getAlias(), "r2");
  }

  @Test
  public void testOneHopBackwardCompat() {
    // Verify that 1-hop getEdge() still works
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id";
    CypherParser parser = new CypherParser(cypher);
    CypherIR.CypherQuery query = parser.parse();

    CypherIR.MatchClause match = query.getMatchClause();
    assertEquals(match.getHopCount(), 1);
    assertNotNull(match.getEdge()); // single-hop accessor
    assertEquals(match.getEdge().getType(), "FOLLOWS");
    assertEquals(match.getNodes().size(), 2);
    assertEquals(match.getEdges().size(), 1);
  }
}
