package com.linkedin.thirdeye.dashboard.api.funnel;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;


public class FunnelSpec {

  private String name;
  private String visulizationType;
  private List<String> metrics;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getVisulizationType() {
    return visulizationType;
  }

  public void setVisulizationType(String visulizationType) {
    this.visulizationType = visulizationType;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<String> metrics) {
    this.metrics = metrics;
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
