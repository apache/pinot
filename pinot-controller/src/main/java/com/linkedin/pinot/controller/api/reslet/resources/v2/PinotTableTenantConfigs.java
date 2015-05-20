package com.linkedin.pinot.controller.api.reslet.resources.v2;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Put;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TenantConfig;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;


public class PinotTableTenantConfigs extends ServerResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableTenantConfigs.class);
  private final ControllerConf conf;
  private final PinotHelixResourceManager manager;
  private final File baseDataDir;
  private final File tempDir;

  public PinotTableTenantConfigs() throws IOException {
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
    StringRepresentation presentation = null;
    final String tableName = (String) getRequest().getAttributes().get("tableName");
    if (tableName == null) {
      return new StringRepresentation("tableName is not present");
    }

    TenantConfig config = null;

    try {
      JSONObject o = new JSONObject(entity.getText().toString());
      config =
          AbstractTableConfig.loadTenantsConfig(new ObjectMapper().readTree(o.getJSONObject("tenants").toString()));
    } catch (final Exception e) {

    }
    return presentation;
  }
}
