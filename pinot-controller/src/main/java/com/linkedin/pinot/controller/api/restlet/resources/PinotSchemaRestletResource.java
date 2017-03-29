package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Response;
import com.linkedin.pinot.common.restlet.swagger.Responses;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.ext.fileupload.RestletFileUpload;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotSchemaRestletResource extends BasePinotControllerRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSchemaRestletResource.class);
  private static final String SCHEMA_NAME = "schemaName";

  private final File baseDataDir;
  private final File tempDir;

  public PinotSchemaRestletResource() throws IOException {
    baseDataDir = new File(_controllerConf.getDataDir());
    if (!baseDataDir.exists()) {
      FileUtils.forceMkdir(baseDataDir);
    }
    tempDir = new File(baseDataDir, "schemasTemp");
    if (!tempDir.exists()) {
      FileUtils.forceMkdir(tempDir);
    }
  }

  @Override
  @Get
  public Representation get() {
    try {
      final String schemaName = (String) getRequest().getAttributes().get(SCHEMA_NAME);
      if (schemaName != null) {
        return getSchema(schemaName);
      } else {
        return getAllSchemas();
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while fetching schema ", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_GET_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("get")
  @Summary("Get a list of all schemas")
  @Tags({"schema"})
  @Paths({
      "/schemas",
      "/schemas/"
  })
  @Responses({
      @Response(statusCode = "200", description = "A list of all schemas")
  })
  private Representation getAllSchemas() {
    List<String> schemaNames = _pinotHelixResourceManager.getSchemaNames();
    JSONArray ret = new JSONArray();

    if (schemaNames == null){
      return new StringRepresentation(ret.toString());
    }

    for (String schema : schemaNames) {
      ret.put(schema);
    }
    return new StringRepresentation(ret.toString());
  }

  @HttpVerb("get")
  @Summary("Gets a schema")
  @Tags({"schema"})
  @Paths({
      "/schemas/{schemaName}",
      "/schemas/{schemaName}/"
  })
  @Responses({
      @Response(statusCode = "200", description = "The contents of the specified schema"),
      @Response(statusCode = "404", description = "The specified schema does not exist")
  })
  private Representation getSchema(
      @Parameter(name = "schemaName", in = "path", description = "The name of the schema to get")
      String schemaName)
      throws IOException {
    LOGGER.info("looking for schema {}", schemaName);
    Schema schema = _pinotHelixResourceManager.getSchema(schemaName);
    if (schema == null) {
      setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation("{}");
    }
    LOGGER.info("schema string is : " + schema.getJSONSchema());
    return new StringRepresentation(schema.getJSONSchema());
  }

  @Override
  @Post
  public Representation post(Representation entity) {
    try {
      return uploadNewSchema();
    } catch (final Exception e) {
      LOGGER.error("Caught exception in file upload", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("post")
  @Summary("Adds a new schema")
  @Tags({"schema"})
  @Paths({
      "/schemas",
      "/schemas/"
  })
  @Responses({
      @Response(statusCode = "200", description = "The schema was added"),
      @Response(statusCode = "500", description = "There was an error while adding the schema")
  })
  private Representation uploadNewSchema()
      throws Exception {
    File dataFile = getUploadContents();

    if (dataFile != null) {
      Schema schema = Schema.fromFile(dataFile);
      try {
        if (!schema.validate(LOGGER)) {
          throw new RuntimeException("Schema validation failed");
        }
        _pinotHelixResourceManager.addOrUpdateSchema(schema);
        return new StringRepresentation(dataFile + " sucessfully added", MediaType.TEXT_PLAIN);
      } catch (Exception e) {
        LOGGER.error("error adding schema ", e);
        LOGGER.error("Caught exception in file upload", e);
        setStatus(Status.SERVER_ERROR_INTERNAL);
        ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
        return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
      }
    } else {
      // Some problem occurs, send back a simple line of text.
      LOGGER.warn("No file was uploaded");
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation("schema not added", MediaType.TEXT_PLAIN);
    }
  }

  @Override
  @Put
  public Representation put(Representation entity) {
    final String schemaName = (String) getRequest().getAttributes().get(SCHEMA_NAME);

    if (schemaName == null) {
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation("No schema name specified in path", MediaType.TEXT_PLAIN);
    }

    try {
      return uploadSchema(schemaName);
    } catch (final Exception e) {
      LOGGER.error("Caught exception in file upload", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(
          ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("put")
  @Summary("Updates an existing schema")
  @Tags({"schema"})
  @Paths({
      "/schemas/{schemaName}",
      "/schemas/{schemaName}/"
  })
  @Responses({
      @Response(statusCode = "200", description = "The schema was updated"),
      @Response(statusCode = "500", description = "There was an error while updating the schema")
  })
  private Representation uploadSchema(
      @Parameter(name = "schemaName", in = "path", description = "The name of the schema to get")
      String schemaName) throws Exception {
    File dataFile = getUploadContents();

    if (dataFile != null) {
      Schema schema = Schema.fromFile(dataFile);
      try {
        if (schema.getSchemaName().equals(schemaName)) {
          _pinotHelixResourceManager.addOrUpdateSchema(schema);
          return new StringRepresentation(dataFile + " sucessfully added", MediaType.TEXT_PLAIN);
        } else {
          final String message =
              "Schema name mismatch for uploaded schema, tried to add schema with name " + schema.getSchemaName()
                  + " as " + schemaName;
          LOGGER.warn(message);
          ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
          setStatus(Status.SERVER_ERROR_INTERNAL);
          return new StringRepresentation(message, MediaType.TEXT_PLAIN);
        }
      } catch (Exception e) {
        LOGGER.error("error adding schema ", e);
        LOGGER.error("Caught exception in file upload", e);
        ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
        setStatus(Status.SERVER_ERROR_INTERNAL);
        return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
      }
    } else {
      // Some problem occurs, send back a simple line of text.
      LOGGER.warn("No file was uploaded");
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_UPLOAD_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation("schema not added", MediaType.TEXT_PLAIN);
    }
  }

  private File getUploadContents()
      throws Exception {
    File dataFile = null;

    // 1/ Create a factory for disk-based file items
    final DiskFileItemFactory factory = new DiskFileItemFactory();

    // 2/ Create a new file upload handler based on the Restlet
    // FileUpload extension that will parse Restlet requests and
    // generates FileItems.
    final RestletFileUpload upload = new RestletFileUpload(factory);
    final List<FileItem> items;

    // 3/ Request is parsed by the handler which generates a
    // list of FileItems
    items = upload.parseRequest(getRequest());

    for (FileItem fileItem : items) {
      String fieldName = fileItem.getFieldName();
      if (dataFile == null) {
        if (fieldName != null) {
          dataFile = new File(tempDir, fieldName + "-" + System.currentTimeMillis());
          fileItem.write(dataFile);
        } else {
          LOGGER.warn("Null field name");
        }
      } else {
        LOGGER.warn("Got extra file item while uploading schema: " + fieldName);
      }

      // Remove the temp file
      // When the file is copied to instead of renamed to the new file, the temp file might be left in the dir
      fileItem.delete();
    }

    return dataFile;
  }

  @Override
  @Delete
  public Representation delete() {
    final String schemaName = (String) getRequest().getAttributes().get(SCHEMA_NAME);
    if (schemaName == null) {
      LOGGER.error("Error: Null schema name.");
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation("Error: Null schema name.");
    }

    StringRepresentation result = null;
    try {
      result = deleteSchema(schemaName);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing delete request", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_DELETE_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      result = new StringRepresentation("Error: Caught Exception " + e.getStackTrace());
    }
    return result;
  }

  @HttpVerb("delete")
  @Summary("Deletes a schema")
  @Tags({"schema"})
  @Paths({
      "/schemas/{schemaName}",
      "/schemas/{schemaName}/"
  })
  @Responses({
      @Response(statusCode = "200", description = "The schema was deleted"),
      @Response(statusCode = "404", description = "The schema does not exist"),
      @Response(statusCode = "409", description = "The schema could not be deleted due to being in use"),
      @Response(statusCode = "500", description = "There was an error while deleting the schema")
  })
  StringRepresentation deleteSchema(@Parameter(name = "schemaName", in = "path",
      description = "The name of the schema to get", required = true) String schemaName) throws JSONException, IOException {

    Schema schema = _pinotHelixResourceManager.getSchema(schemaName);
    if (schema == null) {
      LOGGER.error("Error: could not find schema {}", schemaName);
      setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation("Error: Could not find schema " + schemaName);
    }

    // If the schema is associated with a table, we should not delete it.
    List<String> tableNames = _pinotHelixResourceManager.getAllRealtimeTables();
    for (String tableName : tableNames) {
      AbstractTableConfig config = _pinotHelixResourceManager.getTableConfig(tableName, CommonConstants.Helix.TableType.REALTIME);
      String tableSchema = config.getValidationConfig().getSchemaName();

      if (schemaName.equals(tableSchema)) {
        LOGGER.error("Cannot delete schema {}, as it is associated with table {}", schemaName, tableName);
        setStatus(Status.CLIENT_ERROR_CONFLICT);
        return new StringRepresentation("Error: Cannot delete schema " + schemaName + " as it is associated with table: " +
          TableNameBuilder.extractRawTableName(tableName));
      }
    }

    LOGGER.info("Trying to delete schema {}", schemaName);
    if (_pinotHelixResourceManager.deleteSchema(schema)) {
      LOGGER.info("Success: Deleted schema {}", schemaName);
      setStatus(Status.SUCCESS_OK);
      return new StringRepresentation("Success: Deleted schema " + schemaName);
    } else {
      LOGGER.error("Error: could not delete schema {}", schemaName);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SCHEMA_DELETE_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation("Error: Could not delete schema " + schemaName);
    }
  }
}
