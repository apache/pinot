package com.linkedin.thirdeye.dashboard.api.funnel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;


public class CustomFunnelSpec {

  private String collection;
  private Map<String, FunnelSpec> funnelSpecMap;

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public Map<String, FunnelSpec> getFunnels() {
    return funnelSpecMap;
  }

  public void setFunnels(List<FunnelSpec> funnels) {
    this.funnelSpecMap = new HashMap<String, FunnelSpec>();
    for (FunnelSpec spec : funnels) {
      funnelSpecMap.put(spec.getName(), spec);
    }
  }


  public String toString() {
    String ret = null;

    try {
      ret = new ObjectMapper().writeValueAsString(this);
    } catch (Exception e) {

    }

    return ret;
  }
}
