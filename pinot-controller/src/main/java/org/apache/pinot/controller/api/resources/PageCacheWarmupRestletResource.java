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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.regex.Pattern;

@Path("/")
public class PageCacheWarmupRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PageCacheWarmupRestletResource.class);

  private final PinotFS _pinotFS;
  private final String _pageCacheWarmupDataDir;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String QUERY_FILE_NAME = "queries.txt";
  private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]+_(OFFLINE|REALTIME)$");


  @Inject
  public PageCacheWarmupRestletResource(ControllerConf controllerConf) {
    _pageCacheWarmupDataDir = controllerConf.getProperty(CommonConstants.Controller.PAGE_CACHE_WARMUP_DATA_DIR);
    if (_pageCacheWarmupDataDir == null || _pageCacheWarmupDataDir.isEmpty()) {
      LOGGER.error("PAGE_CACHE_WARMUP_DATA_DIR is not configured");
    }
    URI pageCacheWarmupDataDirUri = URIUtils.getUri(_pageCacheWarmupDataDir);
    _pinotFS = PinotFSFactory.create(pageCacheWarmupDataDirUri.getScheme());
  }


  /**
   * Stores warmup queries for a table.
   * Please note concurrent requests to store queries for the same table is not supported yet and potentially
   * might lead to query file corruption.
   * Example request:
   * {
   *   "tableName": "myTable",
   *   "tableType": "OFFLINE",
   *   "queries": ["SELECT COUNT(*) FROM myTable", "SELECT SUM(column) FROM myTable"]
   * }
   */
  @POST
  @Path("/pagecache/queries")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Stores Warmup queries for tables", notes = "Stores Warmup queries for multiple tables")
  public Response storeWarmupQueries(String requestString) {
    try {
      List<PageCacheWarmupRequest> warmupRequests = OBJECT_MAPPER.readValue(
          requestString, new TypeReference<List<PageCacheWarmupRequest>>() { });
      for (PageCacheWarmupRequest request : warmupRequests) {
        validateInput(request);

        String tableName = request.getTableName();
        String tableType = request.getTableType();
        List<String> queries = request.getQueries();

        LOGGER.info("Processing request to store {} queries for tableName: {}, tableType: {}",
            queries.size(), tableName, tableType);

        storeInPageCacheWarmupDataDir(tableName + "_" + tableType, queries);
      }

      // Return success response
      String responseString = String.format("Successfully stored warmup queries for %d tables.", warmupRequests.size());
      LOGGER.info(responseString);
      return Response.ok(responseString, MediaType.APPLICATION_JSON).build();
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (JsonProcessingException e) {
      throw new ControllerApplicationException(LOGGER, "Failed to parse request", 400);
    } catch (Exception e) {
      LOGGER.error("Unexpected error occurred while processing warmup queries for request: {}", requestString, e);
      throw e;
    }
  }


  private void storeInPageCacheWarmupDataDir(String tableNameWithType, List<String> queries) {
    File tempFile = null;
    try {
      // Get the base directory and prepare paths
      File tableDir = new File(_pageCacheWarmupDataDir, tableNameWithType);
      File queryFile = new File(tableDir, QUERY_FILE_NAME);

      if (!_pinotFS.exists(tableDir.toURI())) {
        _pinotFS.mkdir(tableDir.toURI());
      }

      // Create a temporary file to write the queries
      tempFile = File.createTempFile(QUERY_FILE_NAME, null);
      tempFile.deleteOnExit();

      // Write queries to the temporary file in JSON format
      try (OutputStream outputStream = new FileOutputStream(tempFile)) {
        OBJECT_MAPPER.writeValue(outputStream, queries);
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
   * Fetches warmup queries for a table.
   * Example Request: /pagecache/queries/myTable?tableType=OFFLINE
   * Example Response: ["SELECT COUNT(*) FROM myTable", "SELECT SUM(column) FROM myTable"]
   */
  @GET
  @Path("/pagecache/queries/{tableName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Fetches Warmup queries for a table", notes = "Fetches Warmup queries for a table")
  public Response getWarmupQueries(
      @ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Table type (OFFLINE | REALTIME)", required = true) @QueryParam("tableType") String tableType) {
    try {
      LOGGER.info("Fetching warmup queries for tableName: {}, tableType: {}", tableName, tableType);
      File tableDir = new File(_pageCacheWarmupDataDir, tableName + "_" + tableType);
      File queryFile = new File(tableDir, QUERY_FILE_NAME);
      if (!_pinotFS.exists(queryFile.toURI())) {
        throw new ControllerApplicationException(LOGGER,
            String.format("No queries found for table: %s of type: %s path %s", tableName, tableType, queryFile.getPath()),
            404);
      }

      try (InputStream inputStream = _pinotFS.open(queryFile.toURI())) {
        // Deserialize JSON content into a List<String>
        List<String> queries = OBJECT_MAPPER.readValue(inputStream, new TypeReference<List<String>>() { });
        LOGGER.info("Fetched {} queries for tableName: {}, tableType: {}", queries.size(), tableName, tableType);
        return Response.ok(queries, MediaType.APPLICATION_JSON).build();
      }
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (JsonProcessingException e) {
      throw new ControllerApplicationException(LOGGER, "Failed to serialize response", 500);
    } catch (Exception e) {
      LOGGER.error("Unexpected error occurred while fetching warmup queries for tableName: {}, tableType: {}",
          tableName, tableType, e);
      throw new ControllerApplicationException(LOGGER, "Unexpected error occurred while fetching warmup queries", 500);
    }
  }

  /**
   * Deletes warmup queries for a table.
   * Example Request: /pagecache/queries/myTable?tableType=OFFLINE
   */
  @DELETE
  @Path("/pagecache/queries/{tableName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Deletes Warmup queries for a table", notes = "Deletes Warmup queries for a specific table and table type")
  public Response deleteWarmupQueries(
      @ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Table type (OFFLINE | REALTIME)", required = true) @QueryParam("tableType") String tableType) {
    try {
      LOGGER.info("Deleting warmup queries for tableName: {}, tableType: {}", tableName, tableType);
      File tableDir = new File(_pageCacheWarmupDataDir, tableName + "_" + tableType);
      File queryFile = new File(tableDir, QUERY_FILE_NAME);
      if (!_pinotFS.exists(queryFile.toURI())) {
        return Response.ok(
            String.format("No queries found for table: %s of type: %s", tableName, tableType),
            MediaType.APPLICATION_JSON
        ).build();
      }
      _pinotFS.delete(queryFile.toURI(), true);
      String responseString = String.format("Successfully deleted warmup queries for table: %s of type: %s", tableName, tableType);
      LOGGER.info(responseString);
      return Response.ok(responseString, MediaType.APPLICATION_JSON).build();
    } catch (Exception e) {
      LOGGER.error("Unexpected error occurred while deleting warmup queries for tableName: {}, tableType: {}",
          tableName, tableType, e);
      throw new ControllerApplicationException(LOGGER, "Failed to delete warmup queries", 500);
    }
  }


  private void validateInput(PageCacheWarmupRequest pageCacheWarmupRequest) {
    if (pageCacheWarmupRequest.getTableName() == null || pageCacheWarmupRequest.getTableName().isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Table name is required", 400);
    }
    if (pageCacheWarmupRequest.getTableType() == null || pageCacheWarmupRequest.getTableType().isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Table type is required", 400);
    }
    if (pageCacheWarmupRequest.getQueries() == null || pageCacheWarmupRequest.getQueries().isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Queries list is empty", 400);
    }
    String tableNameWithType = pageCacheWarmupRequest.getTableName() + "_" + pageCacheWarmupRequest.getTableType();
    if (tableNameWithType.contains("..") || tableNameWithType.contains("/") || tableNameWithType.contains("\\")) {
      throw new ControllerApplicationException(LOGGER, "Invalid table name with type", 403);
    }
  }

  public static class PageCacheWarmupRequest {
    private String tableName;
    private String tableType;
    private List<String> queries;

    // Getters
    @JsonProperty("tableName")
    public String getTableName() {
      return tableName;
    }

    @JsonProperty("tableType")
    public String getTableType() {
      return tableType;
    }

    @JsonProperty("queries")
    public List<String> getQueries() {
      return queries;
    }
  }
}
