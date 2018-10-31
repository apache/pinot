package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;


public class FormatterLoader {
  public static RootCauseEntityFormatter fromClassName(String className) throws Exception {
    return (RootCauseEntityFormatter)Class.forName(className).getConstructor().newInstance();
  }
}
