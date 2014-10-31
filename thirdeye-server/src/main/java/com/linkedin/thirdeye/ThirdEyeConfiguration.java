package com.linkedin.thirdeye;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

public class ThirdEyeConfiguration extends Configuration
{
  @NotEmpty
  private String dataRoot;

  @JsonProperty
  public String getDataRoot()
  {
    return dataRoot;
  }

  @JsonProperty
  public void setDataRoot(String dataRoot)
  {
    this.dataRoot = dataRoot;
  }
}
