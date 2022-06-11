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
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
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
import org.apache.pinot.controller.api.events.MetadataEventNotifierFactory;
import org.apache.pinot.controller.api.events.SchemaEventType;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.services.PinotSchemaService;
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

@Path("/")
public class PinotSchemaRestletResource implements PinotSchemaService {
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

  @Override
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

  @Override
  public String getSchema(String schemaName) {
    LOGGER.info("looking for schema {}", schemaName);
    Schema schema = _pinotHelixResourceManager.getSchema(schemaName);
    if (schema == null) {
      throw new ControllerApplicationException(LOGGER, "Schema not found", Response.Status.NOT_FOUND);
    }
    return schema.toPrettyJsonString();
  }

  @Override
  public SuccessResponse deleteSchema(String schemaName) {
    deleteSchemaInternal(schemaName);
    return new SuccessResponse("Schema " + schemaName + " deleted");
  }

  @Override
  public ConfigSuccessResponse updateSchema(String schemaName, boolean reload, FormDataMultiPart multiPart) {
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProps =
        getSchemaAndUnrecognizedPropertiesFromMultiPart(multiPart);
    Schema schema = schemaAndUnrecognizedProps.getLeft();
    SuccessResponse successResponse = updateSchema(schemaName, schema, reload);
    return new ConfigSuccessResponse(successResponse.getStatus(), schemaAndUnrecognizedProps.getRight());
  }


  @Override
  public ConfigSuccessResponse updateSchema(String schemaName, boolean reload, String schemaJsonString) {
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProps = null;
    try {
      schemaAndUnrecognizedProps = JsonUtils.stringToObjectAndUnrecognizedProperties(schemaJsonString, Schema.class);
    } catch (Exception e) {
      String msg = String.format("Invalid schema config json string: %s", schemaJsonString);
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
    Schema schema = schemaAndUnrecognizedProps.getLeft();
    SuccessResponse successResponse = updateSchema(schemaName, schema, reload);
    return new ConfigSuccessResponse(successResponse.getStatus(), schemaAndUnrecognizedProps.getRight());
  }

  @Override
  public ConfigSuccessResponse addSchema(boolean override, FormDataMultiPart multiPart, HttpHeaders httpHeaders,
      Request request) {
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProps =
        getSchemaAndUnrecognizedPropertiesFromMultiPart(multiPart);
    Schema schema = schemaAndUnrecognizedProps.getLeft();
    String endpointUrl = request.getRequestURL().toString();
    validateSchemaName(schema.getSchemaName());
    _accessControlUtils.validatePermission(schema.getSchemaName(), AccessType.CREATE, httpHeaders, endpointUrl,
        _accessControlFactory.create());
    SuccessResponse successResponse = addSchema(schema, override);
    return new ConfigSuccessResponse(successResponse.getStatus(), schemaAndUnrecognizedProps.getRight());
  }


  @Override
  public ConfigSuccessResponse addSchema(boolean override, String schemaJsonString, HttpHeaders httpHeaders,
      Request request) {
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProperties = null;
    try {
      schemaAndUnrecognizedProperties =
          JsonUtils.stringToObjectAndUnrecognizedProperties(schemaJsonString, Schema.class);
    } catch (Exception e) {
      String msg = String.format("Invalid schema config json string: %s", schemaJsonString);
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
    Schema schema = schemaAndUnrecognizedProperties.getLeft();
    String endpointUrl = request.getRequestURL().toString();
    validateSchemaName(schema.getSchemaName());
    _accessControlUtils.validatePermission(schema.getSchemaName(), AccessType.CREATE, httpHeaders, endpointUrl,
        _accessControlFactory.create());
    SuccessResponse successResponse = addSchema(schema, override);
    return new ConfigSuccessResponse(successResponse.getStatus(), schemaAndUnrecognizedProperties.getRight());
  }

  @Override
  public String validateSchema(FormDataMultiPart multiPart) {
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProps =
        getSchemaAndUnrecognizedPropertiesFromMultiPart(multiPart);
    Schema schema = schemaAndUnrecognizedProps.getLeft();
    validateSchemaInternal(schema);
    ObjectNode response = schema.toJsonObject();
    response.set("unrecognizedProperties", JsonUtils.objectToJsonNode(schemaAndUnrecognizedProps.getRight()));
    try {
      return JsonUtils.objectToPrettyString(response);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String validateSchema(String schemaJsonString) {
    Pair<Schema, Map<String, Object>> schemaAndUnrecognizedProps = null;
    try {
      schemaAndUnrecognizedProps = JsonUtils.stringToObjectAndUnrecognizedProperties(schemaJsonString, Schema.class);
    } catch (Exception e) {
      String msg = String.format("Invalid schema config json string: %s", schemaJsonString);
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
    Schema schema = schemaAndUnrecognizedProps.getLeft();
    validateSchemaInternal(schema);
    ObjectNode response = schema.toJsonObject();
    response.set("unrecognizedProperties", JsonUtils.objectToJsonNode(schemaAndUnrecognizedProps.getRight()));
    try {
      return JsonUtils.objectToPrettyString(response);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
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
      SchemaUtils.validate(schema, tableConfigs);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Invalid schema: " + schema.getSchemaName() + ". Reason: " + e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
  }

  /**
   * Internal method to add schema
   * @param schema  schema
   * @param override  set to true to override the existing schema with the same name
   */
  private SuccessResponse addSchema(Schema schema, boolean override) {
    String schemaName = schema.getSchemaName();
    validateSchemaInternal(schema);

    try {
      _pinotHelixResourceManager.addSchema(schema, override);
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
   * @param schemaName  name of the schema to update
   * @param schema  schema
   * @param reload  set to true to reload the tables using the schema, so committed segments can pick up the new schema
   * @return
   */
  private SuccessResponse updateSchema(String schemaName, Schema schema, boolean reload) {
    validateSchemaInternal(schema);

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
      LOGGER.info("Notifying metadata event for deleting schema: {}", schemaName);
      _metadataEventNotifierFactory.create().notifyOnSchemaEvents(schema, SchemaEventType.DELETE);
      LOGGER.info("Success: Deleted schema {}", schemaName);
    } else {
      throw new ControllerApplicationException(LOGGER, String.format("Failed to delete schema %s", schemaName),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
