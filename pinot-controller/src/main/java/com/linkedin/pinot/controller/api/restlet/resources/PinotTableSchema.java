package com.linkedin.pinot.controller.api.restlet.resources;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.api.swagger.HttpVerb;
import com.linkedin.pinot.controller.api.swagger.Parameter;
import com.linkedin.pinot.controller.api.swagger.Paths;
import com.linkedin.pinot.controller.api.swagger.Summary;
import com.linkedin.pinot.controller.api.swagger.Tags;


public class PinotTableSchema extends PinotRestletResourceBase {

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
      @Parameter(name = "tableName", in = "path", description = "Table name for which to get the schema", required = true)
      String tableName) {
    if (_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      try {
        AbstractTableConfig config = _pinotHelixResourceManager.getTableConfig(tableName, TableType.REALTIME);
        return new StringRepresentation(_pinotHelixResourceManager.getSchema(config.getValidationConfig().getSchemaName()).getJSONSchema()
            .toString());
      } catch (Exception e) {
        LOGGER.error("Caught exception while fetching schema for a realtime table : {} ", tableName, e);
        setStatus(Status.SERVER_ERROR_INTERNAL);
        return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
      }
    } else {
      AbstractTableConfig config;
      try {
        config = _pinotHelixResourceManager.getTableConfig(tableName, TableType.OFFLINE);
        return new StringRepresentation(_pinotHelixResourceManager.getSchema(config.getValidationConfig().getSchemaName()).getJSONSchema()
            .toString());
      } catch (Exception e) {
        LOGGER.error("Caught exception while fetching schema for a offline table : {} ", tableName, e);
        setStatus(Status.SERVER_ERROR_INTERNAL);
        return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
      }
    }
  }
}
