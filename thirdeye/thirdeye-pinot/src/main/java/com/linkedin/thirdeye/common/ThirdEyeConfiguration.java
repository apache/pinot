package com.linkedin.thirdeye.common;

import io.dropwizard.Configuration;

public abstract class ThirdEyeConfiguration extends Configuration {
  /**
   * Root directory for all other configuration
   */
  private String rootDir = "";

  private String smtpHost = "";
  private int smtpPort = 0;

  private String phantomJsPath = "";

  /**
   * allow cross request for local development
   */
  private boolean cors = false;

  public String getDataSourcesPath() {
    return getRootDir() + "/data-sources/data-sources-config.yml";
  }

  public String getRootDir() {
    return rootDir;
  }

  public void setRootDir(String rootDir) {
    this.rootDir = rootDir;
  }

  public boolean isCors() {
    return cors;
  }

  public void setCors(boolean cors) {
    this.cors = cors;
  }

  public String getFunctionConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/functions.properties";
  }

  //alertFilter.properties format: {alert filter type} = {path to alert filter implementation}
  public String getAlertFilterConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/alertFilter.properties";
  }

  //alertFilterAutotune.properties format: {auto tune type} = {path to auto tune implementation}
  public String getFilterAutotuneConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/alertFilterAutotune.properties";
  }

  public String getAlertGroupRecipientProviderConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/alertGroupRecipientProvider.properties";
  }

  public String getAnomalyClassifierConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/anomalyClassifier.properties";
  }

  public String getSmtpHost() {
    return smtpHost;
  }

  public void setSmtpHost(String smtpHost) {
    this.smtpHost = smtpHost;
  }

  public int getSmtpPort() {
    return smtpPort;
  }

  public void setSmtpPort(int smtpPort) {
    this.smtpPort = smtpPort;
  }

  public String getPhantomJsPath() {
    return phantomJsPath;
  }

  public void setPhantomJsPath(String phantomJsPath) {
    this.phantomJsPath = phantomJsPath;
  }

}
