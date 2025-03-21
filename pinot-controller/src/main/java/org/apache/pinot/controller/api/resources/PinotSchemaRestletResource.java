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
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.exception.SchemaAlreadyExistsException;
import org.apache.pinot.common.exception.SchemaBackwardIncompatibleException;
import org.apache.pinot.common.exception.SchemaNotFoundException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.api.access.AccessControlFactory;
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
import org.apache.pinot.spi.data.SchemaInfo;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.SCHEMA_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
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
  public List<String> listSchemaNames(@Context HttpHeaders headers) {
    return _pinotHelixResourceManager.getSchemaNames(headers.getHeaderString(DATABASE));
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/info")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_SCHEMAS_INFO)
  @ApiOperation(value = "List all schemas info with count of field specs", notes = "Lists all schemas with field "
      + "count details")
  public String getSchemasInfo(@Context HttpHeaders headers)
      throws JsonProcessingException {
    Map<String, Object> schemaInfoObjects = new LinkedHashMap<>();
    List<SchemaInfo> schemasInfo = new ArrayList<>();
    List<String> uncachedSchemas = new ArrayList<>();
    TableCache cache = _pinotHelixResourceManager.getTableCache();
    List<String> schemas = _pinotHelixResourceManager.getSchemaNames(headers.getHeaderString(DATABASE));
    for (String schemaName : schemas) {
      Schema schema = cache.getSchema(schemaName);
      if (schema != null) {
        schemasInfo.add(new SchemaInfo(schema));
      } else {
        uncachedSchemas.add(schemaName);
      }
    }
    schemaInfoObjects.put("schemasInfo", schemasInfo);
    if (!uncachedSchemas.isEmpty()) {
      schemaInfoObjects.put("uncachedSchemas", uncachedSchemas);
    }
    return JsonUtils.objectToPrettyString(schemaInfoObjects);
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
    schemaName = DatabaseUtils.translateTableName(schemaName, headers);
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
    schemaName = DatabaseUtils.translateTableName(schemaName, headers);
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
      @ApiParam(value = "Whether to reload the table after updating the schema") @DefaultValue("false")
      @QueryParam("reload") boolean reload,
      @ApiParam(value = "Whether to force update the schema even if the new schema is backward incompatible")
      @DefaultValue("false") @QueryParam("force") boolean force, @Context HttpHeaders headers,
      FormDataMultiPart multiPart) {
    schemaName = DatabaseUtils.translateTableName(schemaName, headers);
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProps =
        getSchemaAndUnrecognizedPropertiesFromMultiPart(multiPart);
    Schema schema = schemaAndUnrecognizedProps.getLeft();
    validateSchemaName(schema);
    schema.setSchemaName(DatabaseUtils.translateTableName(schema.getSchemaName(), headers));
    SuccessResponse successResponse = updateSchema(schemaName, schema, reload, force);
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
      @ApiParam(value = "Whether to reload the table after updating the schema") @DefaultValue("false")
      @QueryParam("reload") boolean reload,
      @ApiParam(value = "Whether to force update the schema even if the new schema is backward incompatible")
      @DefaultValue("false") @QueryParam("force") boolean force, @Context HttpHeaders headers,
      String schemaJsonString) {
    schemaName = DatabaseUtils.translateTableName(schemaName, headers);
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProps =
        getSchemaAndUnrecognizedPropertiesFromJson(schemaJsonString);
    Schema schema = schemaAndUnrecognizedProps.getLeft();
    validateSchemaName(schema);
    schema.setSchemaName(DatabaseUtils.translateTableName(schema.getSchemaName(), headers));
    SuccessResponse successResponse = updateSchema(schemaName, schema, reload, force);
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
    validateSchemaName(schema);
    String schemaName = DatabaseUtils.translateTableName(schema.getSchemaName(), httpHeaders);
    schema.setSchemaName(schemaName);
    ResourceUtils.checkPermissionAndAccess(schemaName, request, httpHeaders,
        AccessType.CREATE, Actions.Table.CREATE_SCHEMA, _accessControlFactory, LOGGER);
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
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProperties =
        getSchemaAndUnrecognizedPropertiesFromJson(schemaJsonString);
    Schema schema = schemaAndUnrecognizedProperties.getLeft();
    validateSchemaName(schema);
    String schemaName = DatabaseUtils.translateTableName(schema.getSchemaName(), httpHeaders);
    schema.setSchemaName(schemaName);
    ResourceUtils.checkPermissionAndAccess(schemaName, request, httpHeaders,
        AccessType.CREATE, Actions.Table.CREATE_SCHEMA, _accessControlFactory, LOGGER);
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
    validateSchemaName(schema);
    String schemaName = DatabaseUtils.translateTableName(schema.getSchemaName(), httpHeaders);
    schema.setSchemaName(schemaName);
    validateSchemaInternal(schema);
    ResourceUtils.checkPermissionAndAccess(schemaName, request, httpHeaders,
        AccessType.READ, Actions.Table.VALIDATE_SCHEMA, _accessControlFactory, LOGGER);
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
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProps =
        getSchemaAndUnrecognizedPropertiesFromJson(schemaJsonString);
    Schema schema = schemaAndUnrecognizedProps.getLeft();
    validateSchemaName(schema);
    String schemaName = DatabaseUtils.translateTableName(schema.getSchemaName(), httpHeaders);
    schema.setSchemaName(schemaName);
    validateSchemaInternal(schema);
    ResourceUtils.checkPermissionAndAccess(schemaName, request, httpHeaders,
        AccessType.READ, Actions.Table.VALIDATE_SCHEMA, _accessControlFactory, LOGGER);
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

  private void validateSchemaName(Schema schema) {
    if (StringUtils.isEmpty(schema.getSchemaName())) {
      throw new ControllerApplicationException(LOGGER,
          "Invalid schema. Reason: 'schemaName' should not be null or empty", Response.Status.BAD_REQUEST);
    }
    if (schema.getSchemaName().contains(TableConfig.TABLE_NAME_FORBIDDEN_SUBSTRING)) {
      throw new ControllerApplicationException(LOGGER, "'schemaName' cannot contain double underscore ('__')",
          Response.Status.BAD_REQUEST);
    }
  }

  private void validateSchemaInternal(Schema schema) {
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
      LOGGER.info("Successfully added schema: {} with value: {}", schemaName, schema);
      return new SuccessResponse(schemaName + " successfully added");
    } catch (SchemaAlreadyExistsException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (SchemaBackwardIncompatibleException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, String.format("Failed to add new schema %s. Reason: %s",
          schemaName, e.getMessage()), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Internal method to update schema
   * @param schemaName  name of the schema to update
   * @param schema  schema
   * @param reload  set to true to reload the tables using the schema, so committed segments can pick up the new schema
   * @return SuccessResponse
   */
  private SuccessResponse updateSchema(String schemaName, Schema schema, boolean reload, boolean force) {
    validateSchemaInternal(schema);

    if (!schemaName.equals(schema.getSchemaName())) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          String.format("Schema name mismatch for uploaded schema, tried to add schema with name %s as %s",
              schema.getSchemaName(), schemaName), Response.Status.BAD_REQUEST);
    }

    try {
      _pinotHelixResourceManager.updateSchema(schema, reload, force);
      // Best effort notification. If controller fails at this point, no notification is given.
      LOGGER.info("Notifying metadata event for updating schema: {}", schemaName);
      _metadataEventNotifierFactory.create().notifyOnSchemaEvents(schema, SchemaEventType.UPDATE);
      LOGGER.info("Successfully updated schema: {} with new value: {}", schemaName, schema);
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
      throw new ControllerApplicationException(LOGGER, String.format("Failed to update schema %s. Reason: %s",
          schemaName, e.getMessage()), Response.Status.INTERNAL_SERVER_ERROR, e);
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

  /**
   * Parses a JSON string into a {@link Schema} object and extracts any unrecognized properties.
   * This method is designed to handle the deserialization of a schema JSON string, allowing for the
   * identification and separation of known schema fields and any additional properties that do not
   * match the schema model. This is particularly useful for forward compatibility, where new fields
   * may be added to schemas in future versions of the software.
   *
   * @param schemaJsonString The JSON string representing the schema.
   * @return A {@link Pair} object where the left element is the deserialized {@link Schema} object
   *         and the right element is a {@link Map} containing any unrecognized properties as key-value pairs.
   * @throws ControllerApplicationException if the JSON string cannot be parsed into a {@link Schema} object,
   *         indicating invalid or malformed JSON. The exception contains a message detailing the parsing error
   *         and sets the HTTP status to BAD_REQUEST.
   */
  private Pair<Schema, Map<String, Object>> getSchemaAndUnrecognizedPropertiesFromJson(String schemaJsonString)
      throws ControllerApplicationException {
    try {
      return JsonUtils.stringToObjectAndUnrecognizedProperties(schemaJsonString, Schema.class);
    } catch (Exception e) {
      String msg = String.format("Invalid schema config json string: %s. Reason: %s", schemaJsonString, e.getMessage());
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
  }

  private void deleteSchemaInternal(String schemaName) {
    Schema schema = _pinotHelixResourceManager.getSchema(schemaName);
    if (schema == null) {
      throw new ControllerApplicationException(LOGGER, String.format("Schema %s not found", schemaName),
          Response.Status.NOT_FOUND);
    }

    // If the schema is associated with a table, we should not delete it.
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(schemaName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(schemaName);
    for (String tableNameWithType : new String[]{offlineTableName, realtimeTableName}) {
      if (_pinotHelixResourceManager.hasTable(tableNameWithType)) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Cannot delete schema %s, as it is associated with table %s", schemaName, tableNameWithType),
            Response.Status.CONFLICT);
      }
    }

    LOGGER.info("Trying to delete schema {}", schemaName);
    if (_pinotHelixResourceManager.deleteSchema(schemaName)) {
      LOGGER.info("Notifying metadata event for deleting schema: {}", schemaName);
      _metadataEventNotifierFactory.create().notifyOnSchemaEvents(schema, SchemaEventType.DELETE);
      LOGGER.info("Successfully deleted schema: {}", schemaName);
    } else {
      throw new ControllerApplicationException(LOGGER, String.format("Failed to delete schema %s", schemaName),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
