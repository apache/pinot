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

import com.fasterxml.jackson.databind.node.ArrayNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.exception.SchemaBackwardIncompatibleException;
import org.apache.pinot.common.exception.SchemaNotFoundException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessControlUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.events.MetadataEventNotifierFactory;
import org.apache.pinot.controller.api.events.SchemaEventType;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.SCHEMA_TAG)
@Path("/")
public class PinotSchemaRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotSchemaRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerMetrics _controllerMetrics;

  @Inject
  MetadataEventNotifierFactory _metadataEventNotifierFactory;

  @Inject
  AccessControlFactory _accessControlFactory;
  AccessControlUtils _accessControlUtils = new AccessControlUtils();

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas")
  @ApiOperation(value = "List all schema names", notes = "Lists all schema names")
  public String listSchemaNames() {
    List<String> schemaNames = _pinotHelixResourceManager.getSchemaNames();
    ArrayNode ret = JsonUtils.newArrayNode();

    if (schemaNames != null) {
      for (String schema : schemaNames) {
        ret.add(schema);
      }
    }
    return ret.toString();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @ApiOperation(value = "Get a schema", notes = "Gets a schema by name")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 404, message = "Schema not found"), @ApiResponse(code = 500, message = "Internal error")})
  public String getSchema(
      @ApiParam(value = "Schema name", required = true) @PathParam("schemaName") String schemaName) {
    LOGGER.info("looking for schema {}", schemaName);
    Schema schema = _pinotHelixResourceManager.getSchema(schemaName);
    if (schema == null) {
      throw new ControllerApplicationException(LOGGER, "Schema not found", Response.Status.NOT_FOUND);
    }
    return schema.toPrettyJsonString();
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete a schema", notes = "Deletes a schema by name")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Successfully deleted schema"), @ApiResponse(code = 404, message = "Schema not found"), @ApiResponse(code = 409, message = "Schema is in use"), @ApiResponse(code = 500, message = "Error deleting schema")})
  public SuccessResponse deleteSchema(
      @ApiParam(value = "Schema name", required = true) @PathParam("schemaName") String schemaName) {
    deleteSchemaInternal(schemaName);
    return new SuccessResponse("Schema " + schemaName + " deleted");
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Update a schema", notes = "Updates a schema")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Successfully updated schema"), @ApiResponse(code = 404, message = "Schema not found"), @ApiResponse(code = 400, message = "Missing or invalid request body"), @ApiResponse(code = 500, message = "Internal error")})
  public SuccessResponse updateSchema(
      @ApiParam(value = "Name of the schema", required = true) @PathParam("schemaName") String schemaName,
      @ApiParam(value = "Whether to reload the table if the new schema is backward compatible") @DefaultValue("false") @QueryParam("reload") boolean reload,
      FormDataMultiPart multiPart) {
    return updateSchema(schemaName, getSchemaFromMultiPart(multiPart), reload);
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Update a schema", notes = "Updates a schema")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Successfully updated schema"), @ApiResponse(code = 404, message = "Schema not found"), @ApiResponse(code = 400, message = "Missing or invalid request body"), @ApiResponse(code = 500, message = "Internal error")})
  public SuccessResponse updateSchema(
      @ApiParam(value = "Name of the schema", required = true) @PathParam("schemaName") String schemaName,
      @ApiParam(value = "Whether to reload the table if the new schema is backward compatible") @DefaultValue("false") @QueryParam("reload") boolean reload,
      Schema schema) {
    return updateSchema(schemaName, schema, reload);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas")
  @ApiOperation(value = "Add a new schema", notes = "Adds a new schema")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Successfully created schema"), @ApiResponse(code = 404, message = "Schema not found"), @ApiResponse(code = 400, message = "Missing or invalid request body"), @ApiResponse(code = 500, message = "Internal error")})
  public SuccessResponse addSchema(
      @ApiParam(value = "Whether to override the schema if the schema exists") @DefaultValue("true") @QueryParam("override") boolean override,
      FormDataMultiPart multiPart, @Context HttpHeaders httpHeaders, @Context Request request) {
    Schema schema = getSchemaFromMultiPart(multiPart);
    String endpointUrl = request.getRequestURL().toString();
    _accessControlUtils.validatePermission(schema.getSchemaName(), AccessType.CREATE, httpHeaders, endpointUrl,
        _accessControlFactory.create());
    return addSchema(schema, override);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/schemas")
  @ApiOperation(value = "Add a new schema", notes = "Adds a new schema")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Successfully created schema"), @ApiResponse(code = 404, message = "Schema not found"), @ApiResponse(code = 400, message = "Missing or invalid request body"), @ApiResponse(code = 500, message = "Internal error")})
  public SuccessResponse addSchema(
      @ApiParam(value = "Whether to override the schema if the schema exists") @DefaultValue("true") @QueryParam("override") boolean override,
      Schema schema, @Context HttpHeaders httpHeaders, @Context Request request) {
    String endpointUrl = request.getRequestURL().toString();
    _accessControlUtils.validatePermission(schema.getSchemaName(), AccessType.CREATE, httpHeaders, endpointUrl,
        _accessControlFactory.create());
    return addSchema(schema, override);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/validate")
  @ApiOperation(value = "Validate schema", notes = "This API returns the schema that matches the one you get "
      + "from 'GET /schema/{schemaName}'. This allows us to validate schema before apply.")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Successfully validated schema"), @ApiResponse(code = 400, message = "Missing or invalid request body"), @ApiResponse(code = 500, message = "Internal error")})
  public String validateSchema(FormDataMultiPart multiPart) {
    Schema schema = getSchemaFromMultiPart(multiPart);
    try {
      List<TableConfig> tableConfigs = _pinotHelixResourceManager.getTableConfigsForSchema(schema.getSchemaName());
      SchemaUtils.validate(schema, tableConfigs);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Invalid schema: " + schema.getSchemaName() + ". Reason: " + e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
    return schema.toPrettyJsonString();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/schemas/validate")
  @ApiOperation(value = "Validate schema", notes = "This API returns the schema that matches the one you get "
      + "from 'GET /schema/{schemaName}'. This allows us to validate schema before apply.")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Successfully validated schema"), @ApiResponse(code = 400, message = "Missing or invalid request body"), @ApiResponse(code = 500, message = "Internal error")})
  public String validateSchema(Schema schema) {
    try {
      List<TableConfig> tableConfigs = _pinotHelixResourceManager.getTableConfigsForSchema(schema.getSchemaName());
      SchemaUtils.validate(schema, tableConfigs);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Invalid schema: " + schema.getSchemaName() + ". Reason: " + e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
    return schema.toPrettyJsonString();
  }

  /**
   * Internal method to add schema
   * @param schema  schema
   * @param override  set to true to override the existing schema with the same name
   */
  private SuccessResponse addSchema(Schema schema, boolean override) {
    String schemaName = schema.getSchemaName();
    try {
      List<TableConfig> tableConfigs = _pinotHelixResourceManager.getTableConfigsForSchema(schemaName);
      SchemaUtils.validate(schema, tableConfigs);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Cannot add invalid schema: " + schemaName + ". Reason: " + e.getMessage(), Response.Status.BAD_REQUEST, e);
    }

    try {
      _pinotHelixResourceManager.addSchema(schema, override);
      // Best effort notification. If controller fails at this point, no notification is given.
      LOGGER.info("Notifying metadata event for adding new schema {}", schemaName);
      _metadataEventNotifierFactory.create().notifyOnSchemaEvents(schema, SchemaEventType.CREATE);

      return new SuccessResponse(schemaName + " successfully added");
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, String.format("Failed to add new schema %s.", schemaName),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Internal method to update schema
   * @param schemaName  name of the schema to update
   * @param schema  schema
   * @param reload  set to true to reload the tables using the schema, so committed segments can pick up the new schema
   * @return
   */
  private SuccessResponse updateSchema(String schemaName, Schema schema, boolean reload) {
    try {
      List<TableConfig> tableConfigs = _pinotHelixResourceManager.getTableConfigsForSchema(schemaName);
      SchemaUtils.validate(schema, tableConfigs);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Cannot add invalid schema: %s. Reason: %s", schemaName, e.getMessage()),
          Response.Status.BAD_REQUEST, e);
    }

    if (schemaName != null && !schema.getSchemaName().equals(schemaName)) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, String
          .format("Schema name mismatch for uploaded schema, tried to add schema with name %s as %s",
              schema.getSchemaName(), schema), Response.Status.BAD_REQUEST);
    }

    try {
      _pinotHelixResourceManager.updateSchema(schema, reload);
      // Best effort notification. If controller fails at this point, no notification is given.
      LOGGER.info("Notifying metadata event for updating schema: {}", schemaName);
      _metadataEventNotifierFactory.create().notifyOnSchemaEvents(schema, SchemaEventType.UPDATE);
      return new SuccessResponse(schema.getSchemaName() + " successfully added");
    } catch (SchemaNotFoundException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, String.format("Failed to find schema %s", schemaName),
          Response.Status.NOT_FOUND, e);
    } catch (SchemaBackwardIncompatibleException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          String.format("Backward incompatible schema %s. Only allow adding new columns", schemaName),
          Response.Status.BAD_REQUEST, e);
    } catch (TableNotFoundException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, String.format("Failed to find table %s to reload", schemaName),
          Response.Status.NOT_FOUND, e);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, String.format("Failed to update schema %s", schemaName),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private Schema getSchemaFromMultiPart(FormDataMultiPart multiPart) {
    try {
      Map<String, List<FormDataBodyPart>> map = multiPart.getFields();
      if (!PinotSegmentUploadDownloadRestletResource.validateMultiPart(map, null)) {
        throw new ControllerApplicationException(LOGGER, "Found not exactly one file from the multi-part fields",
            Response.Status.BAD_REQUEST);
      }
      FormDataBodyPart bodyPart = map.values().iterator().next().get(0);
      try (InputStream inputStream = bodyPart.getValueAs(InputStream.class)) {
        return Schema.fromInputSteam(inputStream);
      } catch (IOException e) {
        throw new ControllerApplicationException(LOGGER,
            "Caught exception while de-serializing the schema from request body: " + e.getMessage(),
            Response.Status.BAD_REQUEST);
      }
    } finally {
      multiPart.cleanup();
    }
  }

  private void deleteSchemaInternal(String schemaName) {
    Schema schema = _pinotHelixResourceManager.getSchema(schemaName);
    if (schema == null) {
      throw new ControllerApplicationException(LOGGER, String.format("Schema %s not found", schemaName),
          Response.Status.NOT_FOUND);
    }

    // If the schema is associated with a table, we should not delete it.
    List<String> tableNames = _pinotHelixResourceManager.getAllRealtimeTables();
    for (String tableName : tableNames) {
      TableConfig config = _pinotHelixResourceManager.getRealtimeTableConfig(tableName);
      String tableSchema = config.getValidationConfig().getSchemaName();

      if (schemaName.equals(tableSchema)) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Cannot delete schema %s, as it is associated with table %s", schemaName, tableName),
            Response.Status.CONFLICT);
      }
    }

    LOGGER.info("Trying to delete schema {}", schemaName);
    if (_pinotHelixResourceManager.deleteSchema(schema)) {
      LOGGER.info("Success: Deleted schema {}", schemaName);
    } else {
      throw new ControllerApplicationException(LOGGER, String.format("Failed to delete schema %s", schemaName),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
