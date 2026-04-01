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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.graph.planner.CypherToSqlTranslator;
import org.apache.pinot.graph.spi.GraphSchemaConfig;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handles graph (Cypher) query requests by translating them to SQL and delegating
 * to the multi-stage query engine (MSE) broker request handler.
 *
 * <p>This handler is instantiated only when the {@code pinot.graph.enabled} feature flag
 * is set to {@code true}. It is thread-safe because it delegates all state to the
 * underlying {@link BrokerRequestHandler}.
 */
public class GraphRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphRequestHandler.class);

  private final BrokerRequestHandler _requestHandler;

  public GraphRequestHandler(BrokerRequestHandler requestHandler) {
    _requestHandler = requestHandler;
  }

  /**
   * Handles a graph query request by translating Cypher to SQL and delegating to the MSE handler.
   *
   * @param cypherQuery the Cypher query string
   * @param graphSchemaJson the inline graph schema as a JSON node
   * @param requesterIdentity the identity of the requester, may be null
   * @param requestContext the request context for tracing
   * @param httpHeaders the HTTP headers from the original request, may be null
   * @return the broker response from executing the translated SQL query
   * @throws Exception if translation or query execution fails
   */
  public BrokerResponse handleRequest(String cypherQuery, JsonNode graphSchemaJson,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      @Nullable HttpHeaders httpHeaders)
      throws Exception {
    // Parse the graph schema from JSON
    GraphSchemaConfig schemaConfig = parseGraphSchema(graphSchemaJson);

    // Translate Cypher to SQL
    String sql;
    try {
      CypherToSqlTranslator translator = new CypherToSqlTranslator(schemaConfig);
      sql = translator.translate(cypherQuery);
    } catch (Exception e) {
      LOGGER.error("Failed to translate Cypher query to SQL: {}", cypherQuery, e);
      throw new IllegalArgumentException("Failed to translate Cypher query to SQL: " + e.getMessage(), e);
    }
    LOGGER.debug("Translated Cypher query [{}] to SQL [{}]", cypherQuery, sql);

    // Build a SQL request JSON and delegate to the request handler.
    // The translated SQL uses JOINs, which requires the multi-stage engine.
    ObjectNode sqlRequestJson = JsonUtils.newObjectNode();
    sqlRequestJson.put(Request.SQL, sql);

    // Force multi-stage engine for graph queries since they require JOINs.
    // Query options are passed as a semicolon-delimited string.
    sqlRequestJson.put(Request.QUERY_OPTIONS,
        Request.QueryOptionKey.USE_MULTISTAGE_ENGINE + "=true");

    return _requestHandler.handleRequest(sqlRequestJson, null, requesterIdentity, requestContext,
        httpHeaders);
  }

  /**
   * Parses the inline graph schema JSON into a {@link GraphSchemaConfig}.
   *
   * <p>Expected JSON format:</p>
   * <pre>
   * {
   *   "vertexLabels": {
   *     "User": {
   *       "tableName": "users",
   *       "primaryKey": "id",
   *       "properties": { "id": "id", "name": "user_name" }
   *     }
   *   },
   *   "edgeLabels": {
   *     "FOLLOWS": {
   *       "tableName": "follows",
   *       "sourceVertexLabel": "User",
   *       "sourceKey": "follower_id",
   *       "targetVertexLabel": "User",
   *       "targetKey": "followee_id",
   *       "properties": {}
   *     }
   *   }
   * }
   * </pre>
   */
  static GraphSchemaConfig parseGraphSchema(JsonNode schemaJson) {
    if (schemaJson == null) {
      throw new IllegalArgumentException("Graph schema JSON must not be null");
    }

    // Parse vertex labels
    Map<String, GraphSchemaConfig.VertexLabel> vertexLabels = new HashMap<>();
    JsonNode vertexLabelsNode = schemaJson.get("vertexLabels");
    if (vertexLabelsNode != null && vertexLabelsNode.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fields = vertexLabelsNode.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        String labelName = entry.getKey();
        JsonNode labelNode = entry.getValue();

        String tableName = getRequiredField(labelNode, "tableName", "vertexLabels." + labelName);
        String primaryKey = getRequiredField(labelNode, "primaryKey", "vertexLabels." + labelName);
        Map<String, String> properties = parseStringMap(labelNode.get("properties"));

        vertexLabels.put(labelName, new GraphSchemaConfig.VertexLabel(tableName, primaryKey, properties));
      }
    }

    // Parse edge labels
    Map<String, GraphSchemaConfig.EdgeLabel> edgeLabels = new HashMap<>();
    JsonNode edgeLabelsNode = schemaJson.get("edgeLabels");
    if (edgeLabelsNode != null && edgeLabelsNode.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fields = edgeLabelsNode.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        String labelName = entry.getKey();
        JsonNode labelNode = entry.getValue();

        String tableName = getRequiredField(labelNode, "tableName", "edgeLabels." + labelName);
        String sourceVertexLabel = getRequiredField(labelNode, "sourceVertexLabel", "edgeLabels." + labelName);
        String sourceKey = getRequiredField(labelNode, "sourceKey", "edgeLabels." + labelName);
        String targetVertexLabel = getRequiredField(labelNode, "targetVertexLabel", "edgeLabels." + labelName);
        String targetKey = getRequiredField(labelNode, "targetKey", "edgeLabels." + labelName);
        Map<String, String> properties = parseStringMap(labelNode.get("properties"));

        edgeLabels.put(labelName, new GraphSchemaConfig.EdgeLabel(
            tableName, sourceVertexLabel, sourceKey, targetVertexLabel, targetKey, properties));
      }
    }

    if (vertexLabels.isEmpty()) {
      throw new IllegalArgumentException("Graph schema must define at least one vertex label");
    }
    if (edgeLabels.isEmpty()) {
      throw new IllegalArgumentException("Graph schema must define at least one edge label");
    }

    return new GraphSchemaConfig(vertexLabels, edgeLabels);
  }

  private static String getRequiredField(JsonNode node, String fieldName, String parentPath) {
    JsonNode field = node.get(fieldName);
    if (field == null || field.isNull()) {
      throw new IllegalArgumentException("Missing required field '" + fieldName + "' in " + parentPath);
    }
    return field.asText();
  }

  private static Map<String, String> parseStringMap(JsonNode node) {
    Map<String, String> map = new HashMap<>();
    if (node != null && node.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        map.put(entry.getKey(), entry.getValue().asText());
      }
    }
    return map;
  }
}
