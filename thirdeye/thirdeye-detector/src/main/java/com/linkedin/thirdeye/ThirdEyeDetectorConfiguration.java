package com.linkedin.thirdeye;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;

public class ThirdEyeDetectorConfiguration extends Configuration {
  @Valid
  @NotNull
  private String clientConfigRoot;

  @Valid
  @NotNull
  private String functionConfigPath;

  @Valid
  @NotNull
  private final DataSourceFactory database = new DataSourceFactory();

  @JsonProperty("database")
  public DataSourceFactory getDatabase() {
    return database;
  }

  public String getClientConfigRoot() {
    return clientConfigRoot;
  }

  public void setClientConfigRoot(String clientConfigRoot) {
    this.clientConfigRoot = clientConfigRoot;
  }

  public String getFunctionConfigPath() {
    return functionConfigPath;
  }

  public void setFunctionConfigPath(String functionConfigPath) {
    this.functionConfigPath = functionConfigPath;
  }
}
