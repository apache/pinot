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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;

/**
 * REST endpoint that manages page‑cache warm‑up query files.
 *
 * <p>This resource lets callers list, create, overwrite, and delete warm‑up query
 * files stored under {@code controllerConf.pageCacheWarmupDataDir}. Pinot servers
 * read these files on startup to pre‑warm the OS page cache.</p>
 *
 * <h2>Endpoints</h2>
 * <table border="1">
 *   <tr><th>Method</th><th>Path</th><th>Description</th></tr>
 *   <tr><td>GET</td><td>/pagecache/queries/{tableName}</td><td>Return warm‑up queries</td></tr>
 *   <tr><td>POST</td><td>/pagecache/queries/{tableName}</td><td>Store or overwrite queries</td></tr>
 *   <tr><td>DELETE</td><td>/pagecache/queries/{tableName}</td><td>Remove stored queries</td></tr>
 * </table>
 *
 */
@Api(tags = Constants.PAGE_CACHE_WARMUP_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class PageCacheWarmupRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PageCacheWarmupRestletResource.class);

  private final PinotFS _pinotFS;
  private final String _pageCacheWarmupDataDir;

  private static final String DEFAULT_QUERY_FILE_NAME = "queries";

  @Inject
  public PageCacheWarmupRestletResource(ControllerConf controllerConf) {
    _pageCacheWarmupDataDir = controllerConf.getPageCacheWarmupQueriesDataDir();
    _pinotFS = PinotFSFactory.create(URIUtils.getUri(_pageCacheWarmupDataDir).getScheme());
  }

  /**
   * Retrieves the warm‑up query list for the specified table and type.
   *
   * <p>If {@code queryFileName} is omitted the default file
   * {@value #DEFAULT_QUERY_FILE_NAME} is used.</p>
   *
   * <pre>{@code
   * GET /pagecache/queries/myTable?tableType=OFFLINE
   * Response: ["SELECT COUNT(*) FROM myTable", "SELECT SUM(col) FROM myTable"]
   * }</pre>
   *
   * @param tableName           table name (no type suffix)
   * @param tableType           table type: {@code OFFLINE} or {@code REALTIME}
   * @param queryFileNameParam  optional custom file name
   * @return 200 OK with a JSON array containing SQL query strings
   * @throws ControllerApplicationException if validation fails or the file cannot be found
   */
  @GET
  @Path("/pagecache/queries/{tableName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Fetches Warmup queries for a table", notes = "Fetches Warmup queries for a table")
  public Response getWarmupQueries(
      @ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Table type (OFFLINE | REALTIME)", required = true) @QueryParam("tableType") String tableType,
      @ApiParam(value = "Custom query file name (optional)") @QueryParam("queryFileName") String queryFileNameParam) {
    try {
      String queryFileName = (queryFileNameParam == null || queryFileNameParam.isEmpty())
        ? DEFAULT_QUERY_FILE_NAME
        : queryFileNameParam;
      validateInput(tableName, tableType, queryFileNameParam);

      LOGGER.info("Fetching warmup queries for tableName: {}, tableType: {}", tableName, tableType);
      File tableDir = new File(_pageCacheWarmupDataDir, tableName + "_" + tableType);
      File queryFile = new File(tableDir, queryFileName);
      if (!_pinotFS.exists(queryFile.toURI())) {
        throw new ControllerApplicationException(LOGGER, String.format("No queries found for table: %s of type: %s"
            + " path %s", tableName, tableType, queryFile.getPath()), 404);
      }

      try (InputStream inputStream = _pinotFS.open(queryFile.toURI())) {
        String json = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        List<String> queries = JsonUtils.stringToObject(json, new TypeReference<>() { });
        LOGGER.info("Fetched {} queries for tableName: {}, tableType: {}", queries.size(), tableName, tableType);
        return Response.ok(queries, MediaType.APPLICATION_JSON).build();
      }
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, "Failed to serialize response", 500);
    } catch (Exception e) {
      LOGGER.error("Unexpected error occurred while fetching warmup queries for tableName: {}, tableType: {}",
          tableName, tableType, e);
      throw new ControllerApplicationException(LOGGER, "Unexpected error occurred while fetching warmup queries", 500);
    }
  }

  private void validateInput(String tableName, String tableType, String queryFileName) {
    if (tableName == null || tableName.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Table name is required", 400);
    }
    if (tableType == null || tableType.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Table type is required", 400);
    }
    if (queryFileName != null
        && (queryFileName.contains("..") || queryFileName.contains("/") || queryFileName.contains("\\"))) {
      throw new ControllerApplicationException(LOGGER, "Invalid file name", 403);
    }
    String tableNameWithType = tableName + "_" + tableType;
    if (tableNameWithType.contains("..") || tableNameWithType.contains("/") || tableNameWithType.contains("\\")) {
      throw new ControllerApplicationException(LOGGER, "Invalid table name with type", 403);
    }
  }

  /**
   * Stores (or overwrites) the warm‑up query file for the given table and type.
   *
   * <p>The request body must be a JSON array of SQL strings. The write is performed
   * atomically by first writing to a temporary file and then moving it into place.</p>
   *
   * <pre>{@code
   * POST /pagecache/queries/myTable?tableType=OFFLINE&queryFileName=custom.txt
   * Body: ["SELECT ...", "SELECT ..."]
   * }</pre>
   *
   * @param tableName           table name
   * @param tableType           table type: {@code OFFLINE} or {@code REALTIME}
   * @param queryFileNameParam  optional custom file name
   * @param requestString       JSON array containing SQL queries
   * @return 200 OK with a human‑readable confirmation message
   * @throws ControllerApplicationException if validation fails or the file cannot be written
   */
  @POST
  @Path("/pagecache/queries/{tableName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Stores warmup queries for a table",
      notes = "Provide a JSON array of queries; optional queryFileName overrides the default queries file name")
  public Response storeWarmupQueries(
      @ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Table type (OFFLINE | REALTIME)", required = true) @QueryParam("tableType") String tableType,
      @ApiParam(value = "Custom query file name (optional)") @QueryParam("queryFileName") String queryFileNameParam,
      String requestString) {

    try {
      String queryFileName = (queryFileNameParam == null || queryFileNameParam.isEmpty())
        ? DEFAULT_QUERY_FILE_NAME
        : queryFileNameParam;
      validateInput(tableName, tableType, queryFileName);

      List<String> queries = JsonUtils.stringToObject(requestString, new TypeReference<>() { });
      if (queries == null || queries.isEmpty()) {
        throw new ControllerApplicationException(LOGGER, "Queries list cannot be empty", 400);
      }
      LOGGER.info("Storing {} queries for {}_{}", queries.size(), tableName, tableType);
      storeQueriesInPageCacheWarmupDataDir(tableName + "_" + tableType, queryFileName, queries);

      String responseString = String.format("Successfully stored %d queries for table: %s_%s (file: %s)",
          queries.size(), tableName, tableType, queryFileName);
      return Response.ok(responseString).build();
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, "Failed to parse queries list", 400);
    } catch (Exception e) {
      LOGGER.error("Unexpected error while storing warmup queries", e);
      throw e;
    }
  }

  private void storeQueriesInPageCacheWarmupDataDir(String tableNameWithType, String queryFileName,
                                                    List<String> queries) {
    File tempFile = null;
    try {
      // Get the base directory and prepare paths
      File tableDir = new File(_pageCacheWarmupDataDir, tableNameWithType);
      File queryFile = new File(tableDir, queryFileName);

      if (!_pinotFS.exists(tableDir.toURI())) {
        _pinotFS.mkdir(tableDir.toURI());
      }
      // Create a temporary file to write the queries
      tempFile = File.createTempFile(queryFileName, null);
      tempFile.deleteOnExit();
      // Write queries to the temporary file in JSON format
      String json = JsonUtils.objectToString(queries);
      try (OutputStream outputStream = new FileOutputStream(tempFile)) {
        outputStream.write(json.getBytes(StandardCharsets.UTF_8));
      }
      _pinotFS.move(tempFile.toURI(), queryFile.toURI(), true);
    } catch (Exception e) {
      // Delete the temporary file if it exists
      if (tempFile != null && tempFile.exists()) {
        if (tempFile.delete()) {
          LOGGER.info("Temporary file deleted successfully: {}", tempFile.getAbsolutePath());
        } else {
          LOGGER.warn("Failed to delete temporary file: {}", tempFile.getAbsolutePath());
        }
      }
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to write queries to file for table: %s, exception: %s", tableNameWithType, e), 500);
    }
  }
  /**
   * Deletes the warm‑up query file for the specified table and type.
   *
   * <pre>{@code
   * DELETE /pagecache/queries/myTable?tableType=OFFLINE
   * }</pre>
   *
   * @param tableName           table name
   * @param tableType           table type: {@code OFFLINE} or {@code REALTIME}
   * @param queryFileNameParam  optional custom file name
   * @return 200 OK with a confirmation message (even if the file did not exist)
   * @throws ControllerApplicationException on unexpected I/O errors
   */
  @DELETE
  @Path("/pagecache/queries/{tableName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Deletes Warmup queries for a table",
      notes = "Deletes Warmup queries for a specific table and table type")
  public Response deleteWarmupQueries(
      @ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Table type (OFFLINE | REALTIME)", required = true) @QueryParam("tableType") String tableType,
      @ApiParam(value = "Custom query file name (optional)") @QueryParam("queryFileName") String queryFileNameParam) {
    try {
      String queryFileName = (queryFileNameParam == null || queryFileNameParam.isEmpty())
        ? DEFAULT_QUERY_FILE_NAME
        : queryFileNameParam;
      validateInput(tableName, tableType, queryFileName);

      LOGGER.info("Deleting warmup queries for tableName: {}, tableType: {}", tableName, tableType);
      File tableDir = new File(_pageCacheWarmupDataDir, tableName + "_" + tableType);
      File queryFile = new File(tableDir, queryFileName);
      if (!_pinotFS.exists(queryFile.toURI())) {
        String responseString = String.format("No queries found for table: %s of type: %s and file: %s",
            tableName, tableType, queryFileName);
        return Response.ok(responseString).build();
      }
      _pinotFS.delete(queryFile.toURI(), true);
      String responseString = String.format("Successfully deleted warmup queries for table: %s of type: %s"
          + " and file: %s", tableName, tableType, queryFileName);
      LOGGER.info(responseString);
      return Response.ok(responseString).build();
    } catch (Exception e) {
      LOGGER.error("Unexpected error occurred while deleting warmup queries for tableName: {}, tableType: {}",
          tableName, tableType, e);
      throw new ControllerApplicationException(LOGGER, "Failed to delete warmup queries", 500);
    }
  }
}
