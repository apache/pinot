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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


public class PinotSchemaRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotSchemaRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerConf _controllerConf;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas")
  @ApiOperation(value = "Lists all schema names", notes = "Lists all schema names")
  public JSONArray listSchemaNames(

  ) {
    try {
      List<String> schemaNames = _pinotHelixResourceManager.getSchemaNames();
      JSONArray ret = new JSONArray();

      if (schemaNames != null) {
        for (String schema : schemaNames) {
          ret.put(schema);
        }
      }
      return ret;
    } catch (Exception e) {
      throw new  WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @ApiOperation(value = "Gets a schema", notes = "Gets a schema")
  public String getSchema(
      @ApiParam(value = "Name of the schema", required = true) @PathParam("schemaName") String schemaName
  ) {
    LOGGER.info("looking for schema {}", schemaName);
    Schema schema = _pinotHelixResourceManager.getSchema(schemaName);
    if (schema == null) {
      throw new WebApplicationException("Schema not found", Response.Status.NOT_FOUND);
    }
    return schema.getJSONSchema();
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @ApiOperation(value = "Deletes a schema", notes = "Deletes a schema")
  public SuccessResponse deleteSchema(
      @ApiParam(value = "Name of the schema", required = true) @PathParam("schemaName") String schemaName
  ) {
    try {
      deleteSchemaInternal(schemaName);
      return new SuccessResponse("Schema " + schemaName + " deleted");
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @ApiOperation(value = "Updates a schema", notes = "Updates a schema")
  public SuccessResponse updateSchema(
      @ApiParam(value = "Name of the schema", required = true) @PathParam("schemaName") String schemaName,
      FormDataMultiPart multiPart
  ) {
    try {
      return addOrUpdateSchema(schemaName, multiPart);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/schemas/{schemaName}")
  @ApiOperation(value = "Updates a schema", notes = "Updates a schema")
  public SuccessResponse addSchema(
      @ApiParam(value = "Name of the schema", required = true) @PathParam("schemaName") String schemaName,
      FormDataMultiPart multiPart
  ) {
    try {
      return addOrUpdateSchema(schemaName, multiPart);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Internal method to add or update schema
   * @param schemaName null value indicates new schema (POST request) where schemaName is
   *                   not part of URI
   * @return
   */
  private SuccessResponse addOrUpdateSchema(@Nullable String schemaName, FormDataMultiPart multiPart) {
    File dataFile;
    final String schemaNameForLogging = (schemaName == null) ? "new schema" : schemaName + " schema";
    try {
      dataFile = getUploadContents(multiPart);
    } catch (Exception e) {
          throw new WebApplicationException(e.getMessage(), Response.Status.BAD_REQUEST);
    }

    Schema schema = null;
    if (dataFile == null) {
      // This should not happen since we handle all possible exceptions above
      // Safe to check though
      LOGGER.error("No file was uploaded to add or update {}", schemaNameForLogging);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(
          ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new WebApplicationException("File not received while adding " + schemaNameForLogging, Response.Status.INTERNAL_SERVER_ERROR);
    }

    try {
      schema = Schema.fromFile(dataFile);
    } catch (org.codehaus.jackson.JsonParseException | JsonMappingException e) {
      LOGGER.info("Invalid json while adding {}", schemaNameForLogging, e);
      throw new WebApplicationException("Invalid json in input schema", Response.Status.BAD_REQUEST);
    } catch (IOException e) {
      // this can be due to failure to read from dataFile which is stored locally
      // and hence, server responsibility. return 500 for this error
      LOGGER.error("Failed to read input json while adding {}", schemaNameForLogging, e);
      throw new WebApplicationException("Failed to read input json schema", Response.Status.INTERNAL_SERVER_ERROR);
    }


    if (!schema.validate(LOGGER)) {
      LOGGER.info("Invalid schema during create/update of {}", schemaNameForLogging);
      throw new WebApplicationException("Invalid schema", Response.Status.BAD_REQUEST);
    }

    if (schemaName != null && ! schema.getSchemaName().equals(schemaName)) {
      final String message =
          "Schema name mismatch for uploaded schema, tried to add schema with name " + schema.getSchemaName()
              + " as " + schemaName;
      LOGGER.info(message);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new WebApplicationException(message, Response.Status.BAD_REQUEST);
    }

    try {
      _pinotHelixResourceManager.addOrUpdateSchema(schema);
      return new SuccessResponse(dataFile + " successfully added");
    } catch (Exception e) {
      LOGGER.error("Error updating schema {} ", schemaNameForLogging, e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(
          ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      throw new WebApplicationException("Failed to update schema", Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private File getUploadContents(FormDataMultiPart multiPart) throws Exception {
    File dataFile = null;
    FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);

    Map<String, List<FormDataBodyPart>> map = multiPart.getFields();
    PinotSegmentUploadRestletResource.validateMultiPart(map, null);
    final String name = map.keySet().iterator().next();
    final FormDataBodyPart bodyPart = map.get(name).get(0);
    InputStream is = bodyPart.getValueAs(InputStream.class);

    FileOutputStream os = null;
    try {
      dataFile = new File(provider.getSchemasTmpDir(), name + "." + UUID.randomUUID().toString());
      dataFile.deleteOnExit();
      os = new FileOutputStream(dataFile);
      IOUtils.copy(is, os);
      os.flush();
      return dataFile;
    } catch (Exception e) {
      throw new WebApplicationException(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
    } finally {
      os.close();
      is.close();
    }
  }

  private void deleteSchemaInternal(String schemaName) {
    Schema schema = _pinotHelixResourceManager.getSchema(schemaName);
    if (schema == null) {
      LOGGER.error("Error: could not find schema {}", schemaName);
      throw new WebApplicationException("Schema not found", Response.Status.NOT_FOUND);
    }

    // If the schema is associated with a table, we should not delete it.
    List<String> tableNames = _pinotHelixResourceManager.getAllRealtimeTables();
    for (String tableName : tableNames) {
      TableConfig config = _pinotHelixResourceManager.getRealtimeTableConfig(tableName);
      String tableSchema = config.getValidationConfig().getSchemaName();

      if (schemaName.equals(tableSchema)) {
        LOGGER.error("Cannot delete schema {}, as it is associated with table {}", schemaName, tableName);
        throw new WebApplicationException("Schema is used by table " + tableName, Response.Status.CONFLICT);
      }
    }

    LOGGER.info("Trying to delete schema {}", schemaName);
    if (_pinotHelixResourceManager.deleteSchema(schema)) {
      LOGGER.info("Success: Deleted schema {}", schemaName);
    } else {
      LOGGER.error("Error: could not delete schema {}", schemaName);
      throw new WebApplicationException("Could not delete schema", Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
