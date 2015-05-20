package com.linkedin.pinot.controller.api.reslet.resources.v2;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;


public class PinotTableInstances extends ServerResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableInstances.class);
  private final ControllerConf conf;
  private final PinotHelixResourceManager manager;
  private final File baseDataDir;
  private final File tempDir;

  public PinotTableInstances() throws IOException {
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
    StringRepresentation presentation = null;

    final String tableName = (String) getRequest().getAttributes().get("tableName");
    final String type = getQueryValue("type");
    if (tableName == null) {
      return new StringRepresentation("tableName is not present");
    }

    if (type == null) {
      // send back all instances
    }

    if (type.toLowerCase().equals("broker")) {
      // send back all broker instances
    }

    if (type.toLowerCase().equals("server")) {
      // send back all server instances
    }

    // get all instances for this tableName
    return presentation;
  }
}
