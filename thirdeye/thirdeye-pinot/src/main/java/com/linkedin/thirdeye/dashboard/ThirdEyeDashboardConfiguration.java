package com.linkedin.thirdeye.dashboard;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.Yaml;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;

public class ThirdEyeDashboardConfiguration extends ThirdEyeConfiguration {

  String informedApiUrl;

  public String getInformedApiUrl() {
    return informedApiUrl;
  }

  public void setInformedApiUrl(String informedApiUrl) {
    this.informedApiUrl = informedApiUrl;
  }

  public static void main(String[] args) {
    System.out.println(new Yaml().dump(new ThirdEyeDashboardConfiguration()));
  }

}
