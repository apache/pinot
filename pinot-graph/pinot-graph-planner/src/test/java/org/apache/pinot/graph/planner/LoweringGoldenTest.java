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
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Golden tests for the Cypher-to-SQL lowering pipeline.
 *
 * <p>Each test verifies the exact SQL output produced from a Cypher query,
 * ensuring the full end-to-end lowering is correct and stable.</p>
 */
public class LoweringGoldenTest {

  // ---- Schema: social network (User -FOLLOWS-> User) ----

  private static GraphSchemaConfig createSocialSchema() {
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

  // ---- Schema: content platform (User -AUTHORED-> Article) ----

  private static GraphSchemaConfig createContentSchema() {
    Map<String, String> userProperties = new HashMap<>();
    userProperties.put("id", "uid");
    userProperties.put("email", "email_addr");

    Map<String, String> articleProperties = new HashMap<>();
    articleProperties.put("id", "article_id");
    articleProperties.put("title", "headline");
    articleProperties.put("body", "content_body");

    GraphSchemaConfig.VertexLabel userLabel =
        new GraphSchemaConfig.VertexLabel("members", "uid", userProperties);
    GraphSchemaConfig.VertexLabel articleLabel =
        new GraphSchemaConfig.VertexLabel("articles", "article_id", articleProperties);

    Map<String, GraphSchemaConfig.VertexLabel> vertexLabels = new HashMap<>();
    vertexLabels.put("Author", userLabel);
    vertexLabels.put("Article", articleLabel);

    GraphSchemaConfig.EdgeLabel wroteLabel =
        new GraphSchemaConfig.EdgeLabel("writes", "Author", "writer_uid", "Article", "written_article_id",
            Collections.emptyMap());

    Map<String, GraphSchemaConfig.EdgeLabel> edgeLabels = new HashMap<>();
    edgeLabels.put("WROTE", wroteLabel);

    return new GraphSchemaConfig(vertexLabels, edgeLabels);
  }

  // ---- Test 1: Basic 1-hop outgoing query ----

  @Test
  public void testBasicOneHopOutgoing() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSocialSchema());
    String sql = translator.translate(
        "MATCH (a:User {id: '123'})-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 100");

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = '123' "
            + "LIMIT 100");
  }

  // ---- Test 2: 1-hop incoming query (reversed join direction) ----

  @Test
  public void testOneHopIncoming() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSocialSchema());
    String sql = translator.translate(
        "MATCH (a:User {id: '456'})<-[:FOLLOWS]-(b:User) RETURN b.id LIMIT 50");

    // Incoming: a is the followee (target in schema), b is the follower (source in schema)
    // JOIN keys are swapped: source uses targetKey, target uses sourceKey
    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.followee_id = a.user_id "
            + "JOIN users_table AS b ON e.follower_id = b.user_id "
            + "WHERE a.user_id = '456' "
            + "LIMIT 50");
  }

  // ---- Test 3: Multiple property filters on source node ----

  @Test
  public void testMultiplePropertyFiltersOnSource() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSocialSchema());
    String sql = translator.translate(
        "MATCH (a:User {id: '789', name: 'alice'})-[:FOLLOWS]->(b:User) RETURN b.name LIMIT 10");

    assertEquals(sql,
        "SELECT b.user_name FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = '789' AND a.user_name = 'alice' "
            + "LIMIT 10");
  }

  // ---- Test 4: Multiple return columns ----

  @Test
  public void testMultipleReturnColumns() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSocialSchema());
    String sql = translator.translate(
        "MATCH (a:User {id: '42'})-[:FOLLOWS]->(b:User) RETURN b.id, b.name, a.name");

    assertEquals(sql,
        "SELECT b.user_id, b.user_name, a.user_name FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = '42'");
  }

  // ---- Test 5: Query without LIMIT ----

  @Test
  public void testQueryWithoutLimit() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSocialSchema());
    String sql = translator.translate(
        "MATCH (a:User {id: '1'})-[:FOLLOWS]->(b:User) RETURN b.id");

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = '1'");
  }

  // ---- Test 6: Different table/column names in schema ----

  @Test
  public void testDifferentSchemaTableColumnNames() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createContentSchema());
    String sql = translator.translate(
        "MATCH (a:Author {id: '99'})-[:WROTE]->(b:Article) RETURN b.title LIMIT 25");

    assertEquals(sql,
        "SELECT b.headline FROM writes AS e "
            + "JOIN members AS a ON e.writer_uid = a.uid "
            + "JOIN articles AS b ON e.written_article_id = b.article_id "
            + "WHERE a.uid = '99' "
            + "LIMIT 25");
  }

  // ---- Test 7: String values with special characters (single quotes) ----

  @Test
  public void testStringWithSingleQuotes() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSocialSchema());
    String sql = translator.translate(
        "MATCH (a:User {name: 'O\\'Brien'})-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 10");

    // Single quote in the value should be escaped as '' in the SQL output
    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_name = 'O''Brien' "
            + "LIMIT 10");
  }

  // ---- Test 8: String values with backslashes ----

  @Test
  public void testStringWithBackslashes() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSocialSchema());
    String sql = translator.translate(
        "MATCH (a:User {name: 'path\\\\to\\\\file'})-[:FOLLOWS]->(b:User) RETURN b.id");

    // Backslashes in the value should be escaped as \\\\ in the SQL output
    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_name = 'path\\\\to\\\\file'");
  }

  // ---- Test 9: Integer property values ----

  @Test
  public void testIntegerPropertyValue() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSocialSchema());
    // 'age' is not in the property map, so it falls back to column name 'age'
    String sql = translator.translate(
        "MATCH (a:User {age: 30})-[:FOLLOWS]->(b:User) RETURN b.name LIMIT 5");

    assertEquals(sql,
        "SELECT b.user_name FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.age = 30 "
            + "LIMIT 5");
  }

  // ---- Test 10: No property filters at all ----

  @Test
  public void testNoPropertyFilters() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSocialSchema());
    String sql = translator.translate(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 200");

    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "LIMIT 200");
  }

  // ---- Test 11: Incoming with different vertex labels ----

  @Test
  public void testIncomingWithDifferentVertexLabels() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createContentSchema());
    // "Who wrote this article?" — incoming from Article perspective
    String sql = translator.translate(
        "MATCH (b:Article {id: '500'})<-[:WROTE]-(a:Author) RETURN a.email");

    // Incoming: b is the article (target in schema), a is the author (source in schema)
    assertEquals(sql,
        "SELECT a.email_addr FROM writes AS e "
            + "JOIN articles AS b ON e.written_article_id = b.article_id "
            + "JOIN members AS a ON e.writer_uid = a.uid "
            + "WHERE b.article_id = '500'");
  }

  // ---- Test 12: Return alias without property (returns primary key) ----

  @Test
  public void testReturnAliasWithoutProperty() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSocialSchema());
    String sql = translator.translate(
        "MATCH (a:User {id: '1'})-[:FOLLOWS]->(b:User) RETURN b LIMIT 10");

    // When no property specified, should return primary key
    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS e "
            + "JOIN users_table AS a ON e.follower_id = a.user_id "
            + "JOIN users_table AS b ON e.followee_id = b.user_id "
            + "WHERE a.user_id = '1' "
            + "LIMIT 10");
  }

  // ---- Test 13: Edge with named alias ----

  @Test
  public void testEdgeWithNamedAlias() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSocialSchema());
    String sql = translator.translate(
        "MATCH (a:User {id: '1'})-[r:FOLLOWS]->(b:User) RETURN b.id LIMIT 10");

    // The edge alias 'r' should be used as the edge table alias
    assertEquals(sql,
        "SELECT b.user_id FROM follows_table AS r "
            + "JOIN users_table AS a ON r.follower_id = a.user_id "
            + "JOIN users_table AS b ON r.followee_id = b.user_id "
            + "WHERE a.user_id = '1' "
            + "LIMIT 10");
  }

  // ---- Test 14: Explain output contains expected sections ----

  @Test
  public void testExplainContainsSections() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSocialSchema());
    String explain = translator.explain(
        "MATCH (a:User {id: '123'})-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 100");

    assertTrue(explain.contains("=== Cypher to SQL Explain ==="));
    assertTrue(explain.contains("--- Parsed AST Summary ---"));
    assertTrue(explain.contains("--- Schema Bindings ---"));
    assertTrue(explain.contains("--- Join Structure ---"));
    assertTrue(explain.contains("--- Generated SQL ---"));

    // Verify AST summary content
    assertTrue(explain.contains("MATCH:"));
    assertTrue(explain.contains(":User"));
    assertTrue(explain.contains("FOLLOWS"));
    assertTrue(explain.contains("RETURN: b.id"));
    assertTrue(explain.contains("LIMIT: 100"));

    // Verify schema bindings content
    assertTrue(explain.contains("Vertex User -> table 'users_table'"));
    assertTrue(explain.contains("Edge FOLLOWS -> table 'follows_table'"));

    // Verify join structure content
    assertTrue(explain.contains("Direction: outgoing"));
    assertTrue(explain.contains("1-hop relational join"));

    // Verify SQL is included
    assertTrue(explain.contains("SELECT b.user_id FROM follows_table"));
  }

  // ---- Test 15: Explain for incoming edge ----

  @Test
  public void testExplainIncoming() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createSocialSchema());
    String explain = translator.explain(
        "MATCH (a:User {id: '99'})<-[:FOLLOWS]-(b:User) RETURN b.id");

    assertTrue(explain.contains("Direction: incoming"));
    assertTrue(explain.contains("reversed for INCOMING"));
    assertTrue(explain.contains("Filters:"));
    assertTrue(explain.contains("source.id = '99'"));
  }

  // ---- Test 16: Property that maps to a different column name ----

  @Test
  public void testPropertyColumnMapping() {
    CypherToSqlTranslator translator = new CypherToSqlTranslator(createContentSchema());
    String sql = translator.translate(
        "MATCH (a:Author {email: 'test@example.com'})-[:WROTE]->(b:Article) RETURN b.title, b.body");

    // 'email' maps to 'email_addr', 'title' maps to 'headline', 'body' maps to 'content_body'
    assertEquals(sql,
        "SELECT b.headline, b.content_body FROM writes AS e "
            + "JOIN members AS a ON e.writer_uid = a.uid "
            + "JOIN articles AS b ON e.written_article_id = b.article_id "
            + "WHERE a.email_addr = 'test@example.com'");
  }
}
