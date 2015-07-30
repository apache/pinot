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
import org.restlet.resource.Put;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;


public class PinotTableMetadataConfigs extends ServerResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableMetadataConfigs.class);
  private final ControllerConf conf;
  private final PinotHelixResourceManager manager;
  private final File baseDataDir;
  private final File tempDir;

  public PinotTableMetadataConfigs() throws IOException {
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
  @Put("json")
  public Representation put(Representation entity) {
    final String tableName = (String) getRequest().getAttributes().get("tableName");
    if (tableName == null) {
      return new StringRepresentation("tableName is not present");
    }

    try {
      return updateTableMetadata(tableName, entity);
    } catch (final Exception e) {
      LOGGER.error("Caught exception while updating metadata for table {}", tableName, e);
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("put")
  @Summary("Updates the metadata configuration for a table")
  @Tags({"table"})
  @Paths({
      "/tables/{tableName}/metadataConfigs"
  })
  private Representation updateTableMetadata(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to update the metadata configuration", required = true)
      String tableName, Representation entity)
      throws Exception {
    AbstractTableConfig config = AbstractTableConfig.init(entity.getText());
    manager.updateMetadataConfigFor(config.getTableName(), TableType.valueOf(config.getTableType().toUpperCase()),
        config.getCustomConfigs());
    return new StringRepresentation("done");
  }
}
