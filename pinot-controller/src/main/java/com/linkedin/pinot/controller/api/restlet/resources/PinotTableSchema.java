package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.api.swagger.HttpVerb;
import com.linkedin.pinot.controller.api.swagger.Parameter;
import com.linkedin.pinot.controller.api.swagger.Paths;
import com.linkedin.pinot.controller.api.swagger.Summary;
import com.linkedin.pinot.controller.api.swagger.Tags;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;


public class PinotTableSchema extends ServerResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableSchema.class);
  private final ControllerConf conf;
  private final PinotHelixResourceManager manager;
  private final File baseDataDir;
  private final File tempDir;

  public PinotTableSchema() throws IOException {
    conf = (ControllerConf) getApplication().getContext().getAttributes().get(ControllerConf.class.toString());
    manager =
        (PinotHelixResourceManager) getApplication().getContext().getAttributes()
            .get(PinotHelixResourceManager.class.toString());
    baseDataDir = new File(conf.getDataDir());
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
    if (manager.hasRealtimeTable(tableName)) {
      try {
        AbstractTableConfig config = manager.getTableConfig(tableName, TableType.REALTIME);
        return new StringRepresentation(manager.getSchema(config.getValidationConfig().getSchemaName()).getJSONSchema()
            .toString());
      } catch (Exception e) {
        LOGGER.error("error fetching schema for a realtime table : {} ", tableName, e);
        return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
      }
    } else {
      AbstractTableConfig config;
      try {
        config = manager.getTableConfig(tableName, TableType.OFFLINE);
        return new StringRepresentation(manager.getSchema(config.getValidationConfig().getSchemaName()).getJSONSchema()
            .toString());
      } catch (Exception e) {
        LOGGER.error("error fetching schema for a offline table : {} ", tableName, e);
        return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
      }
    }
  }
}
