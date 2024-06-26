package org.apache.pinot.controller.helix.core;

import org.apache.pinot.controller.ControllerConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotHelixResourceManagerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotHelixResourceManagerFactory.class);

  public static PinotHelixResourceManager create(ControllerConf controllerConf) {
    PinotHelixResourceManager pinotHelixResourceManager = null;
    try {
      Class<?> clazz = Class.forName(controllerConf.getPinotHelixResourceManagerClass());
      pinotHelixResourceManager =
          (PinotHelixResourceManager) clazz.getConstructor(ControllerConf.class).newInstance(controllerConf);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while constructing TableUpsertMetadataManager with class: %s for table: %s",
              controllerConf.getPinotHelixResourceManagerClass()), e);
    }
    return pinotHelixResourceManager;
  }
}
