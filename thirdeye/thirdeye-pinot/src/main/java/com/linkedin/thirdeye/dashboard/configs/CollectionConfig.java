package com.linkedin.thirdeye.dashboard.configs;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class CollectionConfig extends AbstractConfig {
  String collectionName;

  public CollectionConfig() {

  }

  @Override
  public String toJSON() throws Exception{
    return OBJECT_MAPPER.defaultPrettyPrintingWriter().writeValueAsString(this);

  }

}
