/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.controller.api.events.MetadataEventNotifierFactory;
import com.linkedin.pinot.controller.api.events.SchemaEventType;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.json.JSONArray;
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

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas")
  @ApiOperation(value = "List all schema names", notes = "Lists all schema names")
  public String listSchemaNames() {
    List<String> schemaNames = _pinotHelixResourceManager.getSchemaNames();
    JSONArray ret = new JSONArray();

    if (schemaNames != null) {
      for (String schema : schemaNames) {
        ret.put(schema);
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
    // We need to return schema.getJSONSchema(). Returning schema ends up with many extra fields, "jsonSchema" being one of them,
    // Others like fieldSpecMap, etc., serialzing the entire Schema object.
    return schema.getJSONSchema();
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
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
  @ApiOperation(value = "Update a schema", notes = "Updates a schema")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Successfully updated schema"), @ApiResponse(code = 404, message = "Schema not found"), @ApiResponse(code = 400, message = "Missing or invalid request body"), @ApiResponse(code = 500, message = "Internal error")})
  public SuccessResponse updateSchema(
      @ApiParam(value = "Name of the schema", required = true) @PathParam("schemaName") String schemaName,
      FormDataMultiPart multiPart) {
    return addOrUpdateSchema(schemaName, multiPart);
  }

  // TODO: This should not update if the schema already exists
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas")
  @ApiOperation(value = "Add a new schema", notes = "Adds a new schema")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Successfully deleted schema"), @ApiResponse(code = 404, message = "Schema not found"), @ApiResponse(code = 400, message = "Missing or invalid request body"), @ApiResponse(code = 500, message = "Internal error")})
  public SuccessResponse addSchema(FormDataMultiPart multiPart) {
    return addOrUpdateSchema(null, multiPart);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/validate")
  @ApiOperation(value = "Validate schema", notes = "This API returns the schema that matches the one you get "
      + "from 'GET /schema/{schemaName}'. This allows us to validate schema before apply.")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Successfully validated schema"), @ApiResponse(code = 400, message = "Missing or invalid request body"), @ApiResponse(code = 500, message = "Internal error")})
  public String validateSchema(FormDataMultiPart multiPart) {
    Schema schema = getSchemaFromMultiPart(multiPart);
    if (!schema.validate(LOGGER)) {
      throw new ControllerApplicationException(LOGGER, "Invalid schema. Check controller logs",
          Response.Status.BAD_REQUEST);
    }
    return schema.getJSONSchema();
  }

  /**
   * Internal method to add or update schema
   * @param schemaName null value indicates new schema (POST request) where schemaName is
   *                   not part of URI
   * @return
   */
  private SuccessResponse addOrUpdateSchema(@Nullable String schemaName, FormDataMultiPart multiPart) {
    final String schemaNameForLogging = (schemaName == null) ? "new schema" : schemaName + " schema";
    Schema schema = getSchemaFromMultiPart(multiPart);
    if (!schema.validate(LOGGER)) {
      LOGGER.info("Invalid schema during create/update of {}", schemaNameForLogging);
      throw new ControllerApplicationException(LOGGER, "Invalid schema", Response.Status.BAD_REQUEST);
    }

    if (schemaName != null && !schema.getSchemaName().equals(schemaName)) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      final String message =
          "Schema name mismatch for uploaded schema, tried to add schema with name " + schema.getSchemaName() + " as "
              + schemaName;
      throw new ControllerApplicationException(LOGGER, message, Response.Status.BAD_REQUEST);
    }

    try {
      SchemaEventType eventType = SchemaEventType.UPDATE;
      if (schemaName == null) {
        // New schema posted
        eventType = SchemaEventType.CREATE;
      }
      _pinotHelixResourceManager.addOrUpdateSchema(schema);

      // Best effort notification. If controller fails at this point, no notification is given.
      LOGGER.info("Metadata change notification for schema {}, type {}", schema, eventType);
      _metadataEventNotifierFactory.create().notifyOnSchemaEvents(schema, eventType);

      return new SuccessResponse(schema.getSchemaName() + " successfully added");
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to update schema %s", schemaNameForLogging), Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private Schema getSchemaFromMultiPart(FormDataMultiPart multiPart) {
    try {
      Map<String, List<FormDataBodyPart>> map = multiPart.getFields();
      if (!PinotSegmentUploadRestletResource.validateMultiPart(map, null)) {
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
