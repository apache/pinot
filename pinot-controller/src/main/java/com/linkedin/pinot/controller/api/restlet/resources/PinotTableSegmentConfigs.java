package com.linkedin.pinot.controller.api.restlet.resources;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.api.swagger.HttpVerb;
import com.linkedin.pinot.controller.api.swagger.Parameter;
import com.linkedin.pinot.controller.api.swagger.Paths;
import com.linkedin.pinot.controller.api.swagger.Summary;
import com.linkedin.pinot.controller.api.swagger.Tags;


public class PinotTableSegmentConfigs extends PinotRestletResourceBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableSegmentConfigs.class);
  private final File baseDataDir;
  private final File tempDir;

  public PinotTableSegmentConfigs() throws IOException {
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
  @Put("json")
  public Representation put(Representation entity) {
    final String tableName = (String) getRequest().getAttributes().get("tableName");
    if (tableName == null) {
      return new StringRepresentation("tableName is not present");
    }

    try {
      return updateSegmentConfig(tableName, entity);
    } catch (final Exception e) {
      LOGGER.error("Error updating segments configs for table {}", tableName, e);
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }

  }

  @HttpVerb("put")
  @Summary("Updates the segment configuration (validation and retention) for a table")
  @Tags({"table"})
  @Paths({
      "/tables/{tableName}/segmentConfigs"
  })
  private Representation updateSegmentConfig(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to update the segment configuration", required = true)
      String tableName, Representation entity)
      throws Exception {
    AbstractTableConfig config = AbstractTableConfig.init(entity.getText());
    _pinotHelixResourceManager.updateSegmentsValidationAndRetentionConfigFor(config.getTableName(),
        TableType.valueOf(config.getTableType().toUpperCase()), config.getValidationConfig());
    return new StringRepresentation("done");
  }
}
