package com.linkedin.pinot.controller.api.restlet.resources;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import org.apache.commons.io.FileUtils;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.resource.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;


public class PinotTableTenantConfigs extends BasePinotControllerRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableTenantConfigs.class);
  private final File baseDataDir;
  private final File tempDir;

  public PinotTableTenantConfigs() throws IOException {
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
    try {
      return updateTenantConfig("dummy");
    } catch (Exception e) {
      LOGGER.error("Caught exception while updating tenant config " , e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_TENANT_UPDATE_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("put")
  @Summary("Updates the tenant configuration for a table")
  @Tags({"table"})
  @Paths({
      "/tables/{tableName}/tenantConfigs"
  })
  private Representation updateTenantConfig(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to update the tenant configuration", required = true)
      String tableName) {
    throw new UnsupportedOperationException("current tenant config updates are not supported");
  }
}
