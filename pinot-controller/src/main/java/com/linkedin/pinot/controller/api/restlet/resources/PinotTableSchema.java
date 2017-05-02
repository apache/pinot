package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotTableSchema extends BasePinotControllerRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableSchema.class);

  public PinotTableSchema()
      throws IOException {
    File baseDataDir = new File(_controllerConf.getDataDir());
    if (!baseDataDir.exists()) {
      FileUtils.forceMkdir(baseDataDir);
    }
    File schemaTempDir = new File(baseDataDir, "schemasTemp");
    if (!schemaTempDir.exists()) {
      FileUtils.forceMkdir(schemaTempDir);
    }
  }

  @Override
  @Get
  public Representation get() {
    String tableName = (String) getRequest().getAttributes().get("tableName");
    return getTableSchema(tableName);
  }

  @HttpVerb("get")
  @Summary("Gets schema for a table")
  @Tags({"schema", "table"})
  @Paths({"/tables/{tableName}/schema", "/tables/{tableName}/schema/"})
  private Representation getTableSchema(
      @Parameter(name = "tableName", in = "path", description = "Table name for which to get the schema",
          required = true) String tableName) {
    // TODO: After uniform schema, directly use table name as schema name

    if (_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      Schema schema = _pinotHelixResourceManager.getRealtimeTableSchema(tableName);
      if (schema == null) {
        LOGGER.error("Failed to fetch schema for realtime table: {}", tableName);
        ControllerRestApplication.getControllerMetrics()
            .addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_SCHEMA_GET_ERROR, 1L);
        setStatus(Status.SERVER_ERROR_INTERNAL);
        return new StringRepresentation("Error: failed to fetch schema for realtime table: " + tableName);
      }
      return new StringRepresentation(schema.getJSONSchema());
    }

    if (_pinotHelixResourceManager.hasOfflineTable(tableName)) {
      Schema schema = _pinotHelixResourceManager.getOfflineTableSchema(tableName);
      if (schema == null) {
        setStatus(Status.CLIENT_ERROR_NOT_FOUND);
        return new StringRepresentation("Error: schema not found for offline table: " + tableName);
      }
      return new StringRepresentation(schema.getJSONSchema());
    }

    setStatus(Status.CLIENT_ERROR_NOT_FOUND);
    return new StringRepresentation("Error: table: " + tableName + " not found");
  }
}
