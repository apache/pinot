package com.linkedin.thirdeye.dashboard.configs;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class WidgetConfig extends AbstractConfig {

  public WidgetConfig() {
  }

  @Override
  public String toJSON() throws Exception {
    return OBJECT_MAPPER.defaultPrettyPrintingWriter().writeValueAsString(this);

  }

  @Override
  public int getConfigId() {
    // TODO Auto-generated method stub
    return 0;
  }


}
