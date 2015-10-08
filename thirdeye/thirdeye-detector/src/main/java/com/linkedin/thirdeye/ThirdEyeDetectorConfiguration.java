package com.linkedin.thirdeye;

import io.dropwizard.Configuration;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.db.DataSourceFactory;
import org.hibernate.validator.constraints.*;

import javax.validation.Valid;
import javax.validation.constraints.*;

public class ThirdEyeDetectorConfiguration extends Configuration {
  @Valid
  @NotNull
  private String thirdEyeHost;

  @Valid
  @NotNull
  private int thirdEyePort;

  @Valid
  @NotNull
  private DataSourceFactory database = new DataSourceFactory();

  @JsonProperty("database")
  public DataSourceFactory getDatabase() {
    return database;
  }

  public int getThirdEyePort() {
    return thirdEyePort;
  }

  public void setThirdEyePort(int thirdEyePort) {
    this.thirdEyePort = thirdEyePort;
  }

  public String getThirdEyeHost() {
    return thirdEyeHost;
  }

  public void setThirdEyeHost(String thirdEyeHost) {
    this.thirdEyeHost = thirdEyeHost;
  }
}
