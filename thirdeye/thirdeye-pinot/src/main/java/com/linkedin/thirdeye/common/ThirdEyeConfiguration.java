package com.linkedin.thirdeye.common;

import io.dropwizard.Configuration;

public abstract class ThirdEyeConfiguration extends Configuration {
  /**
   * Root directory for all other configuration
   */
  private String rootDir = "";
  /**
   * pinot/mysql etc. Impl specific file will be in
   * <configRootDir>/dataSources e.g
   * <configRootDir>/dataSources/pinot.yml
   */
  private String client = "pinot";
  /**
   * file, mysql etc
   * <configRootDir>/configStores/
   * <configRootDir>/configStores/file.yml
   */
  private String configStoreType = "FILE";
  private String implMode = "hibernate";

  private String whitelistCollections = "";
  private String blacklistCollections = "";

  private String smtpHost = "";
  private int smtpPort = 0;

  public String getRootDir() {
    return rootDir;
  }

  public void setRootDir(String rootDir) {
    this.rootDir = rootDir;
  }

  public String getClient() {
    return client;
  }

  public void setClient(String client) {
    this.client = client;
  }

  public String getConfigStoreType() {
    return configStoreType;
  }

  public void setConfigStoreType(String configStoreType) {
    this.configStoreType = configStoreType;
  }

  public String getImplMode() {
    return implMode;
  }

  public void setImplMode(String implMode) {
    this.implMode = implMode;
  }

  public String getWhitelistCollections() {
    return whitelistCollections;
  }

  public void setWhitelistCollections(String whitelistCollections) {
    this.whitelistCollections = whitelistCollections;
  }

  public String getBlacklistCollections() {
    return blacklistCollections;
  }

  public void setBlacklistCollections(String blacklistCollections) {
    this.blacklistCollections = blacklistCollections;
  }

  public String getFunctionConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/functions.properties";
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

}
