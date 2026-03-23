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
package org.apache.pinot.server.api.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * Server REST endpoint for row-based INSERT INTO operations.
 *
 * <p>This resource receives row data from the broker/coordinator and delegates to the
 * configured {@link InsertExecutor} for processing. The executor handles partitioning,
 * durable logging, and prepared-store persistence.
 *
 * <p>Thread-safety is delegated to the underlying executor implementation.
 */
@Api(tags = "Insert", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class PinotInsertRowResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotInsertRowResource.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // The executor is injected by the server's DI framework. For v1, this may be null if not configured.
  @Inject
  private InsertExecutor _insertExecutor;

  /**
   * Receives row data for INSERT INTO and delegates to the executor.
   *
   * <p>The request body is a JSON object with:
   * <pre>
   * {
   *   "statementId": "...",
   *   "tableName": "myTable_OFFLINE",
   *   "tableType": "OFFLINE",
   *   "rows": [
   *     {"field1": "value1", "field2": 42},
   *     ...
   *   ]
   * }
   * </pre>
   */
  @POST
  @Path("/insert/rows")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Insert rows into a table", notes = "Receives row data from broker/coordinator for INSERT INTO")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Insert accepted"),
      @ApiResponse(code = 400, message = "Bad request"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Response insertRows(
      @ApiParam(value = "Insert request JSON", required = true) String requestBody) {
    if (_insertExecutor == null) {
      throw new WebApplicationException("Row insert executor not configured", Response.Status.SERVICE_UNAVAILABLE);
    }

    try {
      JsonNode requestNode = MAPPER.readTree(requestBody);

      String statementId = requestNode.has("statementId") ? requestNode.get("statementId").asText() : null;
      String tableName = requestNode.has("tableName") ? requestNode.get("tableName").asText() : null;
      String tableTypeStr = requestNode.has("tableType") ? requestNode.get("tableType").asText() : null;
      TableType tableType = tableTypeStr != null ? TableType.valueOf(tableTypeStr) : null;

      List<GenericRow> rows = parseRows(requestNode.get("rows"));

      InsertRequest insertRequest = new InsertRequest.Builder()
          .setStatementId(statementId)
          .setTableName(tableName)
          .setTableType(tableType)
          .setInsertType(InsertType.ROW)
          .setRows(rows)
          .build();

      InsertResult result = _insertExecutor.execute(insertRequest);

      ObjectNode response = MAPPER.createObjectNode();
      response.put("statementId", result.getStatementId());
      response.put("state", result.getState().name());
      if (result.getMessage() != null) {
        response.put("message", result.getMessage());
      }
      if (result.getErrorCode() != null) {
        response.put("errorCode", result.getErrorCode());
      }
      if (!result.getSegmentNames().isEmpty()) {
        ArrayNode segments = MAPPER.createArrayNode();
        for (String seg : result.getSegmentNames()) {
          segments.add(seg);
        }
        response.set("segmentNames", segments);
      }

      return Response.ok(MAPPER.writeValueAsString(response)).build();
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Bad insert request", e);
      throw new WebApplicationException(e.getMessage(), Response.Status.BAD_REQUEST);
    } catch (Exception e) {
      LOGGER.error("Failed to process insert request", e);
      throw new WebApplicationException("Internal error: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns the status of a previously submitted insert statement.
   */
  @GET
  @Path("/insert/status/{statementId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get insert statement status", notes = "Returns the current state of an insert statement")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Status retrieved"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Response getInsertStatus(
      @ApiParam(value = "Statement ID", required = true) @PathParam("statementId") String statementId) {
    if (_insertExecutor == null) {
      throw new WebApplicationException("Row insert executor not configured", Response.Status.SERVICE_UNAVAILABLE);
    }

    InsertResult result = _insertExecutor.getStatus(statementId);

    ObjectNode response = MAPPER.createObjectNode();
    response.put("statementId", result.getStatementId());
    response.put("state", result.getState().name());
    if (result.getMessage() != null) {
      response.put("message", result.getMessage());
    }

    try {
      return Response.ok(MAPPER.writeValueAsString(response)).build();
    } catch (Exception e) {
      throw new WebApplicationException("Failed to serialize response", Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Aborts a previously submitted insert statement.
   */
  @DELETE
  @Path("/insert/abort/{statementId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Abort insert statement", notes = "Aborts a previously submitted insert statement")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Statement aborted"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Response abortInsert(
      @ApiParam(value = "Statement ID", required = true) @PathParam("statementId") String statementId) {
    if (_insertExecutor == null) {
      throw new WebApplicationException("Row insert executor not configured", Response.Status.SERVICE_UNAVAILABLE);
    }

    InsertResult result = _insertExecutor.abort(statementId);

    ObjectNode response = MAPPER.createObjectNode();
    response.put("statementId", result.getStatementId());
    response.put("state", result.getState().name());
    if (result.getMessage() != null) {
      response.put("message", result.getMessage());
    }

    try {
      return Response.ok(MAPPER.writeValueAsString(response)).build();
    } catch (Exception e) {
      throw new WebApplicationException("Failed to serialize response", Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private List<GenericRow> parseRows(JsonNode rowsNode) {
    List<GenericRow> rows = new ArrayList<>();
    if (rowsNode == null || !rowsNode.isArray()) {
      return rows;
    }
    for (JsonNode rowNode : rowsNode) {
      GenericRow row = new GenericRow();
      Iterator<Map.Entry<String, JsonNode>> fields = rowNode.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        row.putValue(field.getKey(), parseFieldValue(field.getValue()));
      }
      rows.add(row);
    }
    return rows;
  }

  private Object parseFieldValue(JsonNode node) {
    if (node.isNull()) {
      return null;
    }
    if (node.isTextual()) {
      return node.asText();
    }
    if (node.isInt()) {
      return node.intValue();
    }
    if (node.isLong()) {
      return node.longValue();
    }
    if (node.isFloat() || node.isDouble()) {
      return node.doubleValue();
    }
    if (node.isBoolean()) {
      return node.booleanValue();
    }
    if (node.isArray()) {
      Object[] arr = new Object[node.size()];
      for (int i = 0; i < node.size(); i++) {
        arr[i] = parseFieldValue(node.get(i));
      }
      return arr;
    }
    return node.toString();
  }
}
