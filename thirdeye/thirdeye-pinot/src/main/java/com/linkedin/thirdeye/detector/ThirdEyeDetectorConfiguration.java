package com.linkedin.thirdeye.detector;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;

import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;

public class ThirdEyeDetectorConfiguration extends ThirdEyeConfiguration {

  @Valid
  @NotNull
  private final DataSourceFactory database = new DataSourceFactory();

  @JsonProperty("database")
  public DataSourceFactory getDatabase() {
    return database;
  }

  public String getFunctionConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/functions.properties";
  }
}
