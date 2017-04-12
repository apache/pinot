package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotTableSegmentConfigs extends BasePinotControllerRestletResource {
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

  @Deprecated
  @Override
  @Put("json")
  public Representation put(Representation entity) {
    final String tableName = (String) getRequest().getAttributes().get("tableName");
    if (tableName == null) {
      LOGGER.error("Error: Table name null.");
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation("Table name is not null");
    }

    try {
      return updateSegmentConfig(tableName, entity);
    } catch (PinotHelixResourceManager.InvalidTableConfigException e) {
      LOGGER.info("Failed to update metadata configuration for table {}, error: {}", tableName, e.getMessage());
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(
          ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      return errorResponseRepresentation(Status.CLIENT_ERROR_BAD_REQUEST, e.getMessage());
    } catch (final Exception e) {
      LOGGER.error("Caught exception while updating segment config ", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }

  }

  @Deprecated
  @HttpVerb("put")
  @Summary("DEPRECATED: Updates the segment configuration (validation and retention) for a table")
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
