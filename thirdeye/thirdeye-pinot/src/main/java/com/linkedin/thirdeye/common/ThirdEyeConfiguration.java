package com.linkedin.thirdeye.common;

import io.dropwizard.Configuration;
import java.util.ArrayList;
import java.util.List;

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

  private List<String> whitelistCollections = new ArrayList<>();
  private List<String> blacklistCollections = new ArrayList<>();

  private String smtpHost = "";
  private int smtpPort = 0;

  private String phantomJsPath = "";

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

  public List<String> getWhitelistCollections() {
    return whitelistCollections;
  }

  public void setWhitelistCollections(List<String> whitelistCollections) {
    this.whitelistCollections = whitelistCollections;
  }

  public List<String> getBlacklistCollections() {
    return blacklistCollections;
  }

  public void setBlacklistCollections(List<String> blacklistCollections) {
    this.blacklistCollections = blacklistCollections;
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
