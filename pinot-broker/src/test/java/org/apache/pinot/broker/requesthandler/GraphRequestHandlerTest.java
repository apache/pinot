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
package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.graph.spi.GraphSchemaConfig;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


/**
 * Tests the {@link GraphRequestHandler} translation logic in isolation, without
 * requiring a running Pinot cluster.
 *
 * <p>The handler's job is to: (1) parse the inline graph schema, (2) translate
 * Cypher to SQL, and (3) forward the request to the underlying broker handler
 * with {@code useMultistageEngine=true}. These tests verify all three steps.</p>
 */
public class GraphRequestHandlerTest {

  private AutoCloseable _mocks;

  @Mock
  private BrokerRequestHandler _mockBrokerRequestHandler;

  @Mock
  private RequestContext _mockRequestContext;

  private GraphRequestHandler _graphRequestHandler;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _mocks = MockitoAnnotations.openMocks(this);
    _graphRequestHandler = new GraphRequestHandler(_mockBrokerRequestHandler);

    // Default: the mock broker handler returns an empty success response
    when(_mockBrokerRequestHandler.handleRequest(any(JsonNode.class), isNull(), isNull(), any(), isNull()))
        .thenReturn(BrokerResponseNative.empty());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  // ---- Schema parsing tests ----

  @Test
  public void testParseGraphSchemaValid()
      throws Exception {
    JsonNode schemaJson = JsonUtils.stringToJsonNode(buildSchemaJson());
    GraphSchemaConfig config = GraphRequestHandler.parseGraphSchema(schemaJson);

    assertEquals(config.getVertexLabels().size(), 1);
    assertTrue(config.getVertexLabels().containsKey("User"));
    assertEquals(config.getVertexLabels().get("User").getTableName(), "users");
    assertEquals(config.getVertexLabels().get("User").getPrimaryKey(), "id");

    assertEquals(config.getEdgeLabels().size(), 1);
    assertTrue(config.getEdgeLabels().containsKey("FOLLOWS"));
    assertEquals(config.getEdgeLabels().get("FOLLOWS").getTableName(), "follows");
    assertEquals(config.getEdgeLabels().get("FOLLOWS").getSourceVertexLabel(), "User");
    assertEquals(config.getEdgeLabels().get("FOLLOWS").getSourceKey(), "follower_id");
    assertEquals(config.getEdgeLabels().get("FOLLOWS").getTargetVertexLabel(), "User");
    assertEquals(config.getEdgeLabels().get("FOLLOWS").getTargetKey(), "followee_id");
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must not be null.*")
  public void testParseGraphSchemaNullInput() {
    GraphRequestHandler.parseGraphSchema(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*at least one vertex label.*")
  public void testParseGraphSchemaNoVertexLabels()
      throws Exception {
    String json = "{ \"vertexLabels\": {}, "
        + "\"edgeLabels\": { \"E\": { \"tableName\": \"e\", \"sourceVertexLabel\": \"A\", "
        + "\"sourceKey\": \"a\", \"targetVertexLabel\": \"B\", \"targetKey\": \"b\" } } }";
    GraphRequestHandler.parseGraphSchema(JsonUtils.stringToJsonNode(json));
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*at least one edge label.*")
  public void testParseGraphSchemaNoEdgeLabels()
      throws Exception {
    String json = "{ \"vertexLabels\": { \"A\": { \"tableName\": \"a\", \"primaryKey\": \"pk\" } }, "
        + "\"edgeLabels\": {} }";
    GraphRequestHandler.parseGraphSchema(JsonUtils.stringToJsonNode(json));
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*Missing required field 'tableName'.*")
  public void testParseGraphSchemaMissingRequiredField()
      throws Exception {
    String json = "{ \"vertexLabels\": { \"A\": { \"primaryKey\": \"pk\" } }, "
        + "\"edgeLabels\": { \"E\": { \"tableName\": \"e\", \"sourceVertexLabel\": \"A\", "
        + "\"sourceKey\": \"a\", \"targetVertexLabel\": \"B\", \"targetKey\": \"b\" } } }";
    GraphRequestHandler.parseGraphSchema(JsonUtils.stringToJsonNode(json));
  }

  // ---- Translation and delegation tests ----

  @Test
  public void testHandleRequestTranslatesCypherToSql()
      throws Exception {
    String cypher = "MATCH (a:User {id: '123'})-[:FOLLOWS]->(b:User) RETURN b.id LIMIT 100";
    JsonNode schemaJson = JsonUtils.stringToJsonNode(buildSchemaJson());

    _graphRequestHandler.handleRequest(cypher, schemaJson, null, _mockRequestContext, null);

    // Capture the JSON sent to the mock broker handler
    ArgumentCaptor<JsonNode> captor = ArgumentCaptor.forClass(JsonNode.class);
    verify(_mockBrokerRequestHandler).handleRequest(captor.capture(), isNull(), isNull(), any(), isNull());

    JsonNode sqlRequest = captor.getValue();

    // Verify the SQL field is set and contains JOIN (graph lowering)
    assertTrue(sqlRequest.has(Request.SQL), "SQL field must be present in forwarded request");
    String sql = sqlRequest.get(Request.SQL).asText();
    assertTrue(sql.contains("SELECT"), "Generated SQL must contain SELECT");
    assertTrue(sql.contains("JOIN"), "Generated SQL must contain JOIN for graph traversal");
    assertTrue(sql.contains("follows"), "Generated SQL must reference the edge table");
    assertTrue(sql.contains("users"), "Generated SQL must reference the vertex table");
    assertTrue(sql.contains("LIMIT 100"), "Generated SQL must preserve LIMIT clause");
    assertTrue(sql.contains("'123'"), "Generated SQL must include the property filter value");
  }

  @Test
  public void testHandleRequestSetsMultistageEngine()
      throws Exception {
    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id";
    JsonNode schemaJson = JsonUtils.stringToJsonNode(buildSchemaJson());

    _graphRequestHandler.handleRequest(cypher, schemaJson, null, _mockRequestContext, null);

    ArgumentCaptor<JsonNode> captor = ArgumentCaptor.forClass(JsonNode.class);
    verify(_mockBrokerRequestHandler).handleRequest(captor.capture(), isNull(), isNull(), any(), isNull());

    JsonNode sqlRequest = captor.getValue();

    // Verify useMultistageEngine=true is set in query options
    assertTrue(sqlRequest.has(Request.QUERY_OPTIONS),
        "queryOptions field must be present in forwarded request");
    String queryOptions = sqlRequest.get(Request.QUERY_OPTIONS).asText();
    assertTrue(queryOptions.contains("useMultistageEngine=true"),
        "queryOptions must contain useMultistageEngine=true");
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*Failed to translate Cypher query.*")
  public void testHandleRequestInvalidCypher()
      throws Exception {
    // This uses unsupported CREATE syntax
    String cypher = "CREATE (n:User {id: '1'}) RETURN n";
    JsonNode schemaJson = JsonUtils.stringToJsonNode(buildSchemaJson());

    _graphRequestHandler.handleRequest(cypher, schemaJson, null, _mockRequestContext, null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*Failed to translate Cypher query.*")
  public void testHandleRequestUnknownVertexLabel()
      throws Exception {
    String cypher = "MATCH (a:UnknownLabel)-[:FOLLOWS]->(b:User) RETURN b.id";
    JsonNode schemaJson = JsonUtils.stringToJsonNode(buildSchemaJson());

    _graphRequestHandler.handleRequest(cypher, schemaJson, null, _mockRequestContext, null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*Failed to translate Cypher query.*")
  public void testHandleRequestUnknownEdgeType()
      throws Exception {
    String cypher = "MATCH (a:User)-[:UNKNOWN_EDGE]->(b:User) RETURN b.id";
    JsonNode schemaJson = JsonUtils.stringToJsonNode(buildSchemaJson());

    _graphRequestHandler.handleRequest(cypher, schemaJson, null, _mockRequestContext, null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must not be null.*")
  public void testHandleRequestNullSchema()
      throws Exception {
    _graphRequestHandler.handleRequest("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id",
        null, null, _mockRequestContext, null);
  }

  @Test
  public void testHandleRequestReturnsResponse()
      throws Exception {
    BrokerResponseNative expectedResponse = BrokerResponseNative.empty();
    when(_mockBrokerRequestHandler.handleRequest(any(JsonNode.class), isNull(), isNull(), any(), isNull()))
        .thenReturn(expectedResponse);

    String cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id";
    JsonNode schemaJson = JsonUtils.stringToJsonNode(buildSchemaJson());

    BrokerResponse response = _graphRequestHandler.handleRequest(cypher, schemaJson, null,
        _mockRequestContext, null);

    assertSame(response, expectedResponse, "Handler must return the response from the underlying broker handler");
  }

  // ---- Helper methods ----

  private static String buildSchemaJson() {
    return "{"
        + "  \"vertexLabels\": {"
        + "    \"User\": {"
        + "      \"tableName\": \"users\","
        + "      \"primaryKey\": \"id\","
        + "      \"properties\": { \"id\": \"id\", \"name\": \"user_name\" }"
        + "    }"
        + "  },"
        + "  \"edgeLabels\": {"
        + "    \"FOLLOWS\": {"
        + "      \"tableName\": \"follows\","
        + "      \"sourceVertexLabel\": \"User\","
        + "      \"sourceKey\": \"follower_id\","
        + "      \"targetVertexLabel\": \"User\","
        + "      \"targetKey\": \"followee_id\","
        + "      \"properties\": {}"
        + "    }"
        + "  }"
        + "}";
  }
}
