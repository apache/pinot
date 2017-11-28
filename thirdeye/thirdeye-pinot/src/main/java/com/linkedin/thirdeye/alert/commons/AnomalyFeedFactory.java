package com.linkedin.thirdeye.alert.commons;

import com.linkedin.thirdeye.alert.feed.AnomalyFeed;
import com.linkedin.thirdeye.common.BaseFactory;


public class AnomalyFeedFactory extends BaseFactory<AnomalyFeed> {
  public static final String PACKAGE_PATH = "com.linkedin.thirdeye.alert.feed";

  public static AnomalyFeed fromClassName(String className)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException{
    String classPath = PACKAGE_PATH + "." + className;
    return (new BaseFactory<AnomalyFeed>()).getInstance(classPath);
  }
}
