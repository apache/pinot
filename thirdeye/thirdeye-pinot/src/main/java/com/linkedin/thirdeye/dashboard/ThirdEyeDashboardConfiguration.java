package com.linkedin.thirdeye.dashboard;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import java.util.List;


public class ThirdEyeDashboardConfiguration extends ThirdEyeConfiguration {

  String informedApiUrl;

  String rcaConfigPath;
  int rcaParallelism;
  List<String> rcaFormatters;

  String rcaRootCauseFramework;
  String rcaRelatedMetricsFramework;

  public String getInformedApiUrl() {
    return informedApiUrl;
  }

  public void setInformedApiUrl(String informedApiUrl) {
    this.informedApiUrl = informedApiUrl;
  }

  public String getRcaConfigPath() {
    return rcaConfigPath;
  }

  public void setRcaConfigPath(String rcaConfigPath) {
    this.rcaConfigPath = rcaConfigPath;
  }

  public int getRcaParallelism() {
    return rcaParallelism;
  }

  public void setRcaParallelism(int rcaParallelism) {
    this.rcaParallelism = rcaParallelism;
  }

  public List<String> getRcaFormatters() {
    return rcaFormatters;
  }

  public void setRcaFormatters(List<String> rcaFormatters) {
    this.rcaFormatters = rcaFormatters;
  }

  public String getRcaRootCauseFramework() {
    return rcaRootCauseFramework;
  }

  public void setRcaRootCauseFramework(String rcaRootCauseFramework) {
    this.rcaRootCauseFramework = rcaRootCauseFramework;
  }

  public String getRcaRelatedMetricsFramework() {
    return rcaRelatedMetricsFramework;
  }

  public void setRcaRelatedMetricsFramework(String rcaRelatedMetricsFramework) {
    this.rcaRelatedMetricsFramework = rcaRelatedMetricsFramework;
  }
}
