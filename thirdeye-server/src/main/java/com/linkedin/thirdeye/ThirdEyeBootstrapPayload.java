package com.linkedin.thirdeye;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

public class ThirdEyeBootstrapPayload
{
  @NotEmpty
  private String collection;

  @NotEmpty
  private String uri;

  @JsonProperty
  public String getCollection()
  {
    return collection;
  }

  public void setCollection(String collection)
  {
    this.collection = collection;
  }

  @JsonProperty
  public String getUri()
  {
    return uri;
  }

  public void setUri(String uri)
  {
    this.uri = uri;
  }
}
