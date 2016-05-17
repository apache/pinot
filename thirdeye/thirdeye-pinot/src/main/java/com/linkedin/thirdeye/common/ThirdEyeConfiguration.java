package com.linkedin.thirdeye.common;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;

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

  private String whitelistCollections = "";
  private String blacklistCollections = "";

  @Valid
  @NotNull
  private final DataSourceFactory database = new DataSourceFactory();

  public ThirdEyeConfiguration() {

  }

  @JsonProperty("database")
  public DataSourceFactory getDatabase() {
    return database;
  }

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

}
