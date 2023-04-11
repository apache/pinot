package org.apache.pinot.common;

import java.util.Map;


public class Versions {

  /**
   * Obtains the version numbers of the Pinot components.
   *
   * @return A map of component name to component version.
   */
  public Map<String, String> getComponentVersions() {
    return Utils.getComponentVersions();
  }
}
