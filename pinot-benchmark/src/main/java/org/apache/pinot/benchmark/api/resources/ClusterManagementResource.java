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
package org.apache.pinot.benchmark.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.benchmark.api.PinotClusterManager;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = "Cluster Management")
@Path("/")
public class ClusterManagementResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(ClusterManagementResource.class);

  @Inject
  private PinotClusterManager _pinotClusterManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cluster/tables")
  public List<String> listAllTables(@ApiParam(value = "perf|prod") @QueryParam("cluster") String clusterType) {
    return _pinotClusterManager.listTables(clusterType);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cluster/tables/{tableName}")
  public String getTableConfig(
      @ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "perf|prod") @QueryParam("cluster") String clusterType) {
    return _pinotClusterManager.getTableConfig(clusterType, tableName);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cluster/tables")
  @ApiOperation(value = "Adds a table", notes = "Adds a table")
  public String addTable(FormDataMultiPart multiPart) {
    return _pinotClusterManager.addTableConfig(multiPart);
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cluster/tables/{tableName}")
  @ApiOperation(value = "Update a table", notes = "Updates a table")
  public String updateTable(@ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName,
      String tableConfigStr) {
    return _pinotClusterManager.updateTableConfig(tableName, tableConfigStr);
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cluster/tables/{tableName}")
  @ApiOperation(value = "Delete a table", notes = "Deletes a table by name")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Successfully deleted table config"), @ApiResponse(code = 404, message = "Table config not found"), @ApiResponse(code = 409, message = "Table config already exists"), @ApiResponse(code = 500, message = "Error deleting table config")})
  public String deleteTableConfig(
      @ApiParam(value = "Schema name", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) {
    return _pinotClusterManager.deleteTableConfig(tableName, tableTypeStr);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cluster/schemas")
  @ApiOperation(value = "List all schema names", notes = "Lists all schema names")
  public String listSchemaNames(@ApiParam(value = "perf|prod") @QueryParam("cluster") String clusterType) {
    return _pinotClusterManager.listSchemas(clusterType);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cluster/schemas/{schemaName}")
  public String getSchema(@ApiParam(value = "Schema name", required = true) @PathParam("schemaName") String schemaName,
      @ApiParam(value = "perf|prod") @QueryParam("cluster") String clusterType) {
    return _pinotClusterManager.getSchema(clusterType, schemaName);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/cluster/schemas")
  @ApiOperation(value = "Add a new schema", notes = "Adds a new schema")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Successfully added schema"), @ApiResponse(code = 404, message = "Schema not found"), @ApiResponse(code = 400, message = "Missing or invalid request body"), @ApiResponse(code = 500, message = "Internal error")})
  public String addSchema(FormDataMultiPart multiPart) {
    return _pinotClusterManager.addSchema(multiPart);
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/cluster/schemas/{schemaName}")
  public String updateSchema(
      @ApiParam(value = "Name of the schema", required = true) @PathParam("schemaName") String schemaName,
      FormDataMultiPart multiPart) {
    return _pinotClusterManager.updateSchema(schemaName, multiPart);
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/cluster/schemas/{schemaName}")
  @ApiOperation(value = "Delete a schema", notes = "Deletes a schema by name")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Successfully deleted schema"), @ApiResponse(code = 404, message = "Schema not found"), @ApiResponse(code = 409, message = "Schema is in use"), @ApiResponse(code = 500, message = "Error deleting schema")})
  public String deleteSchema(
      @ApiParam(value = "Schema name", required = true) @PathParam("schemaName") String schemaName) {
    return _pinotClusterManager.deleteSchema(schemaName);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/cluster/createTable")
  @ApiOperation(value = "Create a benchmark table in perf cluster", notes = "Creates a benchmark table in perf cluster")
  public SuccessResponse createBenchmarkTable(FormDataMultiPart multiPart) {
    return _pinotClusterManager.createBenchmarkTable(multiPart);
  }
}
