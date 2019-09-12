package org.apache.pinot.benchmark;

import org.apache.commons.configuration.PropertiesConfiguration;


public class PinotBenchConf extends PropertiesConfiguration {

  private static final String PERF_CONTROLLER_HOST = "perf.controller.host";
  private static final String PERF_CONTROLLER_PORT = "perf.controller.port";

  private static final String PROD_CONTROLLER_HOST = "prod.controller.host";
  private static final String PROD_CONTROLLER_PORT = "prod.controller.port";

  private static final String LOCAL_TEMP_DIR = "controller.local.temp.dir";

  public void setPerfControllerHost(String perfControllerHost) {
    setProperty(PERF_CONTROLLER_HOST, perfControllerHost);
  }

  public String getPerfControllerHost() {
    return (String) getProperty(PERF_CONTROLLER_HOST);
  }

  public void setPerfControllerPort(int perfControllerPort) {
    setProperty(PERF_CONTROLLER_PORT, perfControllerPort);
  }

  public int getPerfControllerPort() {
    return getInt(PERF_CONTROLLER_PORT);
  }

  public void setProdControllerHost(String prodControllerHost) {
    setProperty(PROD_CONTROLLER_HOST, prodControllerHost);
  }

  public String getProdControllerHost() {
    return (String) getProperty(PROD_CONTROLLER_HOST);
  }

  public void setProdControllerPort(int prodControllerPort) {
    setProperty(PROD_CONTROLLER_PORT, prodControllerPort);
  }

  public int getProdControllerPort() {
    return getInt(PROD_CONTROLLER_PORT, -1);
  }
}
