/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.resources;

import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Api(tags = Constants.SCHEMA_TAG)
@Path("/")
public class PinotSchemaRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSchemaRestletResource.class);
  @Inject
  PinotHelixResourceManager pinotHelixResourceManager;
  @Inject
  ControllerMetrics metrics;

  @GET
  @Path("/schemas")
  @ApiOperation(value = "List all schemas", notes = "Lists all the schema names in the cluster")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error")})
  public List<String> listSchemas() {
    return pinotHelixResourceManager.getSchemaNames();
  }

  @GET
  @Path("/schemas/{schemaName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List schema by name", notes = "Lists schema by the given name")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success", response = Schema.class),
      @ApiResponse(code = 404, message = "Schema not found"),
      @ApiResponse(code = 500, message = "Internal server error")})
  public Schema getSchema(
      @ApiParam(value = "Schema name", required = true)
      @PathParam("schemaName") String schemaName
  ) {
    Schema schema = pinotHelixResourceManager.getSchema(schemaName);
    if (schema == null) {
      throw new WebApplicationException("Schema " + schemaName  + " not found", Response.Status.NOT_FOUND);
    }
    return schema;
  }

  /*
  @POST
  @Path("/schemas")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List schema by name", notes = "Lists schema by the given name")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success", response = SuccessResponse.class),
      @ApiResponse(code = 400, message = "Error converting json to schema or missing schema"),
      @ApiResponse(code = 409, message = "Schema already exists"),
      @ApiResponse(code = 500, message = "Internal server error")})
  public SuccessResponse addNewSchema(
      @ApiParam(value = "Json schema", required = true)
      @FormDataParam("schema") @DefaultValue("") String schemaStr) {
    return addOrUpdateSchema(schemaStr, null);
  }

  @PUT
  @Path("/schemas/{schemaName}")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List schema by name", notes = "Lists schema by the given name")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success", response = SuccessResponse.class),
      @ApiResponse(code = 404, message = "Schema not found"),
      @ApiResponse(code = 500, message = "Internal server error")})
  public SuccessResponse updateSchema(
      @ApiParam(value = "Schema name", required = true)
      @PathParam("schemaName") String schemaName,
      @ApiParam(value = "Schema value as json", required = true)
      @FormDataParam("schema") String schemaStr
  ) {
    return addOrUpdateSchema(schemaStr, schemaName);
  }
  */

  @DELETE
  @Path("/schemas/{schemaName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List schema by name", notes = "Lists schema by the given name")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success", response = SuccessResponse.class),
      @ApiResponse(code = 404, message = "Schema not found"),
      @ApiResponse(code = 409, message = "Conflict: schema in use"),
      @ApiResponse(code = 500, message = "Internal server error")})
  public SuccessResponse deleteSchema(
      @ApiParam(value = "Schema name", required = true)
      @PathParam("schemaName") String schemaName
  ) {
    Schema schema;
    try {
      schema = pinotHelixResourceManager.getSchema(schemaName);
    } catch(Exception e) {
      LOGGER.error("Server error while reading schema: {}", schemaName);
      metrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_GET_ERROR, 1L);
      throw new WebApplicationException("Error while reading schema " + schemaName,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (schema == null) {
      throw new WebApplicationException("Schema " + schemaName + " not found", Response.Status.NOT_FOUND);
    }

    // If the schema is associated with a table, we should not delete it.
    List<String> tableNames = pinotHelixResourceManager.getAllRealtimeTables();
    for (String tableName : tableNames) {
      TableConfig config;
      try {
        config = pinotHelixResourceManager.getTableConfig(tableName, CommonConstants.Helix.TableType.REALTIME);
      } catch (Exception e) {
        LOGGER.error("Failed to read config for table: {}, error: ", tableName, e);
        metrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_GET_ERROR, 1L);
        throw new WebApplicationException("Server error while reading table configuration",
            Response.Status.INTERNAL_SERVER_ERROR);
      }
      String tableSchema = config.getValidationConfig().getSchemaName();

      if (schemaName.equals(tableSchema)) {
        LOGGER.info("Cannot delete schema {}, as it is associated with table {}", schemaName, tableName);
        throw new WebApplicationException("Schema " + schemaName + " is in use by table " + tableName,
            Response.Status.CONFLICT);
      }
    }

    LOGGER.info("Trying to delete schema {}", schemaName);
    if (pinotHelixResourceManager.deleteSchema(schema)) {
      LOGGER.info("Success: Deleted schema {}", schemaName);
      return new SuccessResponse("Deleted schema " + schemaName);
    } else {
      LOGGER.error("Error: could not delete schema {}", schemaName);
      metrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_DELETE_ERROR, 1L);
      throw new WebApplicationException("Server error while deleting schema " + schemaName,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private SuccessResponse addOrUpdateSchema(String schemaStr, String schemaName) {
    if (schemaStr.isEmpty()) {
      LOGGER.info("Missing schema during add or update of schema");
      throw new WebApplicationException("Missing schema data", Response.Status.BAD_REQUEST);
    }
    Schema schema = null;
    try {
      schema = Schema.fromString(schemaStr);
    } catch (IOException e) {
      LOGGER.info("Bad request while adding new schema, error:" , e);
      throw new WebApplicationException("Error reading json schema", Response.Status.BAD_REQUEST);
    }
    if (!schema.validate(LOGGER)) {
      LOGGER.info("Invalid schema during create/update of {}", schemaName);
      throw new WebApplicationException("Invalid schema during create/update, schema: " + schemaName,
          Response.Status.BAD_REQUEST);
    }
    if (schemaName != null && !schemaName.isEmpty()) {
      if (! schema.getSchemaName().equals(schemaName)) {
        throw new WebApplicationException("Request schema name does not match with the body",
            Response.Status.BAD_REQUEST);
      }
    }

    try {
      pinotHelixResourceManager.addOrUpdateSchema(schema);
      return new SuccessResponse("Successfully added schema");
    } catch (Exception e) {
      LOGGER.error("error adding schema ", e);
      metrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new WebApplicationException("Failed to add new schema", Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
