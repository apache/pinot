package com.linkedin.thirdeye.dashboard;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.Yaml;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;

public class ThirdEyeDashboardConfiguration extends ThirdEyeConfiguration {

  public ThirdEyeDashboardConfiguration() {
    super();
  }

  public static void main(String[] args) {
    System.out.println(new Yaml().dump(new ThirdEyeDashboardConfiguration()));
  }

}
