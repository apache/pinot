package com.linkedin.thirdeye.dashboard.api.funnel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class FunnelSpec {

  private String name;
  private String visulizationType;
  private Map<String, String> aliasToMetricsMap;

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

  public Map<String, String> getAliasToActualMetrics() {
    return aliasToMetricsMap;
  }

  public void setMetrics(LinkedList<String> metrics) {
    aliasToMetricsMap = new LinkedHashMap<String, String>();
    for (String metric : metrics) {
      String alias = metric.split("=")[0];
      String actual = metric.split("=")[1];
      aliasToMetricsMap.put(alias, actual);
    }
  }

  public List<String> getActualMetricNames() {
    List<String> ret = new ArrayList<String>();
    ret.addAll(aliasToMetricsMap.values());
    return ret;
  }
}
