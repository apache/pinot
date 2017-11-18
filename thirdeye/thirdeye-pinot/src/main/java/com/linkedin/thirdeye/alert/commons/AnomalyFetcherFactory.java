package com.linkedin.thirdeye.alert.commons;

import com.linkedin.thirdeye.alert.fetcher.AnomalyFetcher;
import com.linkedin.thirdeye.common.BaseFactory;


public class AnomalyFetcherFactory extends BaseFactory<AnomalyFetcher>{
  public static final String PACKAGE_PATH = "com.linkedin.thirdeye.alert.fetcher";

  public static AnomalyFetcher fromClassName(String className)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException{
    String classPath = PACKAGE_PATH + "." + className;
    return (new BaseFactory<AnomalyFetcher>()).getInstance(classPath);
  }
}
