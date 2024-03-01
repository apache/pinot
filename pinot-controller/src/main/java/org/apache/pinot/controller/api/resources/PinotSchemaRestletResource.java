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

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.exception.SchemaAlreadyExistsException;
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
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.SCHEMA_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
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

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_SCHEMA)
  @ApiOperation(value = "List all schema names", notes = "Lists all schema names")
  public String listSchemaNames(@Context HttpHeaders headers) {
    List<String> schemaNames = _pinotHelixResourceManager.getSchemaNames(
        headers.getHeaderString(CommonConstants.DATABASE));
    ArrayNode ret = JsonUtils.newArrayNode();

    for (String schema : schemaNames) {
      ret.add(schema);
    }
    return ret.toString();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "schemaName", action = Actions.Table.GET_SCHEMA)
  @ApiOperation(value = "Get a schema", notes = "Gets a schema by name")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Schema not found"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public String getSchema(
      @ApiParam(value = "Schema name", required = true) @PathParam("schemaName") String schemaName,
      @Context HttpHeaders headers) {
    schemaName = _pinotHelixResourceManager.translateTableName(schemaName,
        headers.getHeaderString(CommonConstants.DATABASE));
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
  @Authorize(targetType = TargetType.TABLE, paramName = "schemaName", action = Actions.Table.DELETE_SCHEMA)
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete a schema", notes = "Deletes a schema by name")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully deleted schema"),
      @ApiResponse(code = 404, message = "Schema not found"),
      @ApiResponse(code = 409, message = "Schema is in use"),
      @ApiResponse(code = 500, message = "Error deleting schema")
  })
  public SuccessResponse deleteSchema(
      @ApiParam(value = "Schema name", required = true) @PathParam("schemaName") String schemaName,
      @Context HttpHeaders headers) {
    schemaName = _pinotHelixResourceManager.translateTableName(schemaName,
        headers.getHeaderString(CommonConstants.DATABASE));
    deleteSchemaInternal(schemaName);
    return new SuccessResponse("Schema " + schemaName + " deleted");
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "schemaName", action = Actions.Table.UPDATE_SCHEMA)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Update a schema", notes = "Updates a schema")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully updated schema"),
      @ApiResponse(code = 404, message = "Schema not found"),
      @ApiResponse(code = 400, message = "Missing or invalid request body"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public ConfigSuccessResponse updateSchema(
      @ApiParam(value = "Name of the schema", required = true) @PathParam("schemaName") String schemaName,
      @ApiParam(value = "Whether to reload the table if the new schema is backward compatible") @DefaultValue("false")
      @QueryParam("reload") boolean reload, @Context HttpHeaders headers, FormDataMultiPart multiPart) {
    String translatedSchemaName = _pinotHelixResourceManager.translateTableName(schemaName,
        headers.getHeaderString(CommonConstants.DATABASE));
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProps =
        getSchemaAndUnrecognizedPropertiesFromMultiPart(multiPart);
    Schema schema = schemaAndUnrecognizedProps.getLeft();
    if (!_pinotHelixResourceManager.translateTableName(schema.getSchemaName(),
        headers.getHeaderString(CommonConstants.DATABASE)).equals(translatedSchemaName)) {
      throw new ControllerApplicationException(LOGGER,
          "Schema name mismatch: " + schema.getSchemaName() + " is not equivalent to " + schemaName,
          Response.Status.BAD_REQUEST);
    }
    schema.setSchemaName(translatedSchemaName);
    SuccessResponse successResponse = updateSchema(schema, reload);
    return new ConfigSuccessResponse(successResponse.getStatus(), schemaAndUnrecognizedProps.getRight());
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "schemaName", action = Actions.Table.UPDATE_SCHEMA)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Update a schema", notes = "Updates a schema")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully updated schema"),
      @ApiResponse(code = 404, message = "Schema not found"),
      @ApiResponse(code = 400, message = "Missing or invalid request body"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public ConfigSuccessResponse updateSchema(
      @ApiParam(value = "Name of the schema", required = true) @PathParam("schemaName") String schemaName,
      @ApiParam(value = "Whether to reload the table if the new schema is backward compatible") @DefaultValue("false")
      @QueryParam("reload") boolean reload, @Context HttpHeaders headers, String schemaJsonString) {
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProps;
    try {
      schemaAndUnrecognizedProps = JsonUtils.stringToObjectAndUnrecognizedProperties(schemaJsonString, Schema.class);
    } catch (Exception e) {
      String msg = String.format("Invalid schema config json string: %s", schemaJsonString);
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
    Schema schema = schemaAndUnrecognizedProps.getLeft();
    String translatedSchemaName = _pinotHelixResourceManager.translateTableName(schemaName,
        headers.getHeaderString(CommonConstants.DATABASE));
    if (!_pinotHelixResourceManager.translateTableName(schema.getSchemaName(),
        headers.getHeaderString(CommonConstants.DATABASE)).equals(translatedSchemaName)) {
      throw new ControllerApplicationException(LOGGER,
          "Schema name mismatch: " + schema.getSchemaName() + " is not equivalent to " + schemaName,
          Response.Status.BAD_REQUEST);
    }
    schema.setSchemaName(translatedSchemaName);
    SuccessResponse successResponse = updateSchema(schema, reload);
    return new ConfigSuccessResponse(successResponse.getStatus(), schemaAndUnrecognizedProps.getRight());
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas")
  @ApiOperation(value = "Add a new schema", notes = "Adds a new schema")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully created schema"),
      @ApiResponse(code = 409, message = "Schema already exists"),
      @ApiResponse(code = 400, message = "Missing or invalid request body"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  @ManualAuthorization // performed after parsing schema
  public ConfigSuccessResponse addSchema(
      @ApiParam(value = "Whether to override the schema if the schema exists") @DefaultValue("true")
      @QueryParam("override") boolean override,
      @ApiParam(value = "Whether to force overriding the schema if the schema exists") @DefaultValue("false")
      @QueryParam("force") boolean force,
      FormDataMultiPart multiPart,
      @Context HttpHeaders httpHeaders,
      @Context Request request) {
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProps =
        getSchemaAndUnrecognizedPropertiesFromMultiPart(multiPart);
    Schema schema = schemaAndUnrecognizedProps.getLeft();
    String schemaName = _pinotHelixResourceManager.translateTableName(schema.getSchemaName(),
        httpHeaders.getHeaderString(CommonConstants.DATABASE));
    String endpointUrl = request.getRequestURL().toString();
    validateSchemaName(schemaName);
    AccessControlUtils.validatePermission(schemaName, AccessType.CREATE, httpHeaders, endpointUrl,
        _accessControlFactory.create());
    if (!_accessControlFactory.create()
        .hasAccess(httpHeaders, TargetType.TABLE, schemaName, Actions.Table.CREATE_SCHEMA)) {
      throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
    }
    schema.setSchemaName(schemaName);
    SuccessResponse successResponse = addSchema(schema, override, force);
    return new ConfigSuccessResponse(successResponse.getStatus(), schemaAndUnrecognizedProps.getRight());
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/schemas")
  @ApiOperation(value = "Add a new schema", notes = "Adds a new schema")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully created schema"),
      @ApiResponse(code = 409, message = "Schema already exists"),
      @ApiResponse(code = 400, message = "Missing or invalid request body"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  @ManualAuthorization // performed after parsing schema
  public ConfigSuccessResponse addSchema(
      @ApiParam(value = "Whether to override the schema if the schema exists") @DefaultValue("true")
      @QueryParam("override") boolean override,
      @ApiParam(value = "Whether to force overriding the schema if the schema exists") @DefaultValue("false")
      @QueryParam("force") boolean force,
      String schemaJsonString,
      @Context HttpHeaders httpHeaders,
      @Context Request request) {
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProperties = null;
    try {
      schemaAndUnrecognizedProperties =
          JsonUtils.stringToObjectAndUnrecognizedProperties(schemaJsonString, Schema.class);
    } catch (Exception e) {
      String msg = String.format("Invalid schema config json string: %s", schemaJsonString);
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
    Schema schema = schemaAndUnrecognizedProperties.getLeft();
    String schemaName = _pinotHelixResourceManager.translateTableName(schema.getSchemaName(),
        httpHeaders.getHeaderString(CommonConstants.DATABASE));
    String endpointUrl = request.getRequestURL().toString();
    validateSchemaName(schemaName);
    AccessControlUtils.validatePermission(schemaName, AccessType.CREATE, httpHeaders, endpointUrl,
        _accessControlFactory.create());
    if (!_accessControlFactory.create()
        .hasAccess(httpHeaders, TargetType.TABLE, schemaName, Actions.Table.CREATE_SCHEMA)) {
      throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
    }
    schema.setSchemaName(schemaName);
    SuccessResponse successResponse = addSchema(schema, override, force);
    return new ConfigSuccessResponse(successResponse.getStatus(), schemaAndUnrecognizedProperties.getRight());
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/validate")
  @ApiOperation(value = "Validate schema", notes = "This API returns the schema that matches the one you get "
      + "from 'GET /schema/{schemaName}'. This allows us to validate schema before apply.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully validated schema"),
      @ApiResponse(code = 400, message = "Missing or invalid request body"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  @ManualAuthorization // performed after parsing schema
  public String validateSchema(FormDataMultiPart multiPart, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProps =
        getSchemaAndUnrecognizedPropertiesFromMultiPart(multiPart);
    Schema schema = schemaAndUnrecognizedProps.getLeft();
    String endpointUrl = request.getRequestURL().toString();
    validateSchemaInternal(schema);
    AccessControlUtils.validatePermission(schema.getSchemaName(), AccessType.READ, httpHeaders, endpointUrl,
        _accessControlFactory.create());
    if (!_accessControlFactory.create()
        .hasAccess(httpHeaders, TargetType.TABLE, schema.getSchemaName(), Actions.Table.VALIDATE_SCHEMA)) {
      throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
    }
    ObjectNode response = schema.toJsonObject();
    response.set("unrecognizedProperties", JsonUtils.objectToJsonNode(schemaAndUnrecognizedProps.getRight()));
    try {
      return JsonUtils.objectToPrettyString(response);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/schemas/validate")
  @ApiOperation(value = "Validate schema", notes = "This API returns the schema that matches the one you get "
      + "from 'GET /schema/{schemaName}'. This allows us to validate schema before apply.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully validated schema"),
      @ApiResponse(code = 400, message = "Missing or invalid request body"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  @ManualAuthorization // performed after parsing schema
  public String validateSchema(String schemaJsonString, @Context HttpHeaders httpHeaders, @Context Request request) {
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProps = null;
    try {
      schemaAndUnrecognizedProps = JsonUtils.stringToObjectAndUnrecognizedProperties(schemaJsonString, Schema.class);
    } catch (Exception e) {
      String msg = String.format("Invalid schema config json string: %s", schemaJsonString);
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
    Schema schema = schemaAndUnrecognizedProps.getLeft();
    String endpointUrl = request.getRequestURL().toString();
    validateSchemaInternal(schema);
    AccessControlUtils.validatePermission(schema.getSchemaName(), AccessType.READ, httpHeaders, endpointUrl,
        _accessControlFactory.create());
    if (!_accessControlFactory.create()
        .hasAccess(httpHeaders, TargetType.TABLE, schema.getSchemaName(), Actions.Table.VALIDATE_SCHEMA)) {
      throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
    }
    ObjectNode response = schema.toJsonObject();
    response.set("unrecognizedProperties", JsonUtils.objectToJsonNode(schemaAndUnrecognizedProps.getRight()));
    try {
      return JsonUtils.objectToPrettyString(response);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the metadata on the valid {@link FieldSpec.DataType} for each
   * {@link FieldSpec.FieldType} and the default null values for each combination
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/fieldSpec")
  @ManualAuthorization // Always allow this API
  @ApiOperation(value = "Get fieldSpec metadata", notes = "Get fieldSpec metadata")
  public String getFieldSpecMetadata() {
    try {
      return JsonUtils.objectToString(FieldSpec.FIELD_SPEC_METADATA);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private void validateSchemaName(String schemaName) {
    if (StringUtils.isBlank(schemaName)) {
      throw new ControllerApplicationException(LOGGER, "Invalid schema. Reason: 'schemaName' should not be null",
          Response.Status.BAD_REQUEST);
    }
  }

  private void validateSchemaInternal(Schema schema) {
    validateSchemaName(schema.getSchemaName());
    try {
      List<TableConfig> tableConfigs = _pinotHelixResourceManager.getTableConfigsForSchema(schema.getSchemaName());
      boolean isIgnoreCase = _pinotHelixResourceManager.getTableCache().isIgnoreCase();
      SchemaUtils.validate(schema, tableConfigs, isIgnoreCase);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Invalid schema: " + schema.getSchemaName() + ". Reason: " + e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
  }

  /**
   * Internal method to add schema
   * @param schema  schema
   * @param override  set to true to override the existing schema with the same name
   * @param force set to true to skip all rules and force to override the existing schema with the same name
   */
  private SuccessResponse addSchema(Schema schema, boolean override, boolean force) {
    String schemaName = schema.getSchemaName();
    validateSchemaInternal(schema);

    try {
      _pinotHelixResourceManager.addSchema(schema, override, force);
      // Best effort notification. If controller fails at this point, no notification is given.
      LOGGER.info("Notifying metadata event for adding new schema {}", schemaName);
      _metadataEventNotifierFactory.create().notifyOnSchemaEvents(schema, SchemaEventType.CREATE);

      return new SuccessResponse(schemaName + " successfully added");
    } catch (SchemaAlreadyExistsException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (SchemaBackwardIncompatibleException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, String.format("Failed to add new schema %s.", schemaName),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Internal method to update schema
   * @param schema  schema to update
   * @param reload  set to true to reload the tables using the schema, so committed segments can pick up the new schema
   * @return
   */
  private SuccessResponse updateSchema(Schema schema, boolean reload) {
    String schemaName = schema.getSchemaName();
    validateSchemaInternal(schema);

    try {
      _pinotHelixResourceManager.updateSchema(schema, reload, false);
      // Best effort notification. If controller fails at this point, no notification is given.
      LOGGER.info("Notifying metadata event for updating schema: {}", schemaName);
      _metadataEventNotifierFactory.create().notifyOnSchemaEvents(schema, SchemaEventType.UPDATE);
      return new SuccessResponse(schemaName + " successfully added");
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
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to find table %s to reload", schemaName),
          Response.Status.NOT_FOUND, e);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, String.format("Failed to update schema %s", schemaName),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private Pair<Schema, Map<String, Object>> getSchemaAndUnrecognizedPropertiesFromMultiPart(
      FormDataMultiPart multiPart) {
    try {
      Map<String, List<FormDataBodyPart>> map = multiPart.getFields();
      if (!PinotSegmentUploadDownloadRestletResource.validateMultiPart(map, null)) {
        throw new ControllerApplicationException(LOGGER, "Found not exactly one file from the multi-part fields",
            Response.Status.BAD_REQUEST);
      }
      FormDataBodyPart bodyPart = map.values().iterator().next().get(0);
      try (InputStream inputStream = bodyPart.getValueAs(InputStream.class)) {
        return Schema.parseSchemaAndUnrecognizedPropsfromInputStream(inputStream);
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
    // TODO: Check OFFLINE tables as well. There are 2 side effects:
    //       - Increases ZK read when there are lots of OFFLINE tables
    //       - Behavior change since we don't allow deleting schema for OFFLINE tables
    List<String> realtimeTables = _pinotHelixResourceManager.getAllRealtimeTables();
    for (String realtimeTableName : realtimeTables) {
      if (schemaName.equals(TableNameBuilder.extractRawTableName(realtimeTableName))) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Cannot delete schema %s, as it is associated with table %s", schemaName, realtimeTableName),
            Response.Status.CONFLICT);
      }
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(realtimeTableName);
      if (tableConfig != null) {
        if (schemaName.equals(tableConfig.getValidationConfig().getSchemaName())) {
          throw new ControllerApplicationException(LOGGER,
              String.format("Cannot delete schema %s, as it is associated with table %s", schemaName,
                  realtimeTableName), Response.Status.CONFLICT);
        }
      }
    }

    LOGGER.info("Trying to delete schema {}", schemaName);
    if (_pinotHelixResourceManager.deleteSchema(schemaName)) {
      LOGGER.info("Notifying metadata event for deleting schema: {}", schemaName);
      _metadataEventNotifierFactory.create().notifyOnSchemaEvents(schema, SchemaEventType.DELETE);
      LOGGER.info("Success: Deleted schema {}", schemaName);
    } else {
      throw new ControllerApplicationException(LOGGER, String.format("Failed to delete schema %s", schemaName),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
