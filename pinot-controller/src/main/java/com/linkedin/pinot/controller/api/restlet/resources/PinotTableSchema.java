package com.linkedin.pinot.controller.api.restlet.resources;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.TableNameBuilder;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.api.ControllerRestApplication;


public class PinotTableSchema extends BasePinotControllerRestletResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableSchema.class);
  private final File baseDataDir;
  private final File tempDir;

  public PinotTableSchema() throws IOException {
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
    final String tableName = (String) getRequest().getAttributes().get("tableName");

    return getTableSchema(tableName);
  }

  @HttpVerb("get")
  @Summary("Gets schema for a table")
  @Tags({"schema", "table"})
  @Paths({
      "/tables/{tableName}/schema",
      "/tables/{tableName}/schema/"
  })
  private Representation getTableSchema(
      @Parameter(name = "tableName", in = "path", description = "Table name for which to get the schema",
          required = true) String tableName) {
    if (_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      try {
        AbstractTableConfig config = _pinotHelixResourceManager.getTableConfig(tableName, TableType.REALTIME);
        String schemaName = config.getValidationConfig().getSchemaName();
        Schema schema = _pinotHelixResourceManager.getSchema(schemaName);
        Preconditions.checkNotNull(schema, "Failed to fetch schema: %s for REALTIME table: %s", schemaName, tableName);
        return new StringRepresentation(schema.getJSONSchema());
      } catch (Exception e) {
        LOGGER.error("Caught exception while fetching schema for REALTIME table: {} ", tableName, e);
        ControllerRestApplication.getControllerMetrics()
            .addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_SCHEMA_GET_ERROR, 1L);
        setStatus(Status.SERVER_ERROR_INTERNAL);
        return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
      }
    }

    if (_pinotHelixResourceManager.hasOfflineTable(tableName)) {
      // For OFFLINE table, schema name is the same as table name
      String schemaName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
      try {
        Schema schema = _pinotHelixResourceManager.getSchema(schemaName);
        if (schema == null) {
          setStatus(Status.CLIENT_ERROR_NOT_FOUND);
          return new StringRepresentation("Error: schema " + schemaName + " not found");
        }
        return new StringRepresentation(schema.getJSONSchema());
      } catch (Exception e) {
        LOGGER.error("Caught exception while fetching schema for OFFLINE table :{} ", tableName, e);
        ControllerRestApplication.getControllerMetrics()
            .addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_SCHEMA_GET_ERROR, 1L);
        setStatus(Status.SERVER_ERROR_INTERNAL);
        return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
      }
    }

    setStatus(Status.CLIENT_ERROR_NOT_FOUND);
    return new StringRepresentation("Error: table " + tableName + " not found");
  }
}
