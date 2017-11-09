package com.linkedin.thirdeye.alert.commons;

import com.linkedin.thirdeye.alert.fetcher.AnomalyFetcher;
import com.linkedin.thirdeye.common.BaseFactory;
import com.linkedin.thirdeye.common.FactorySource;


public class AnomalyFetcherFactory extends BaseFactory<AnomalyFetcher>{
  private AnomalyFetcherFactory(){
    super(FactorySource.ALERT_FETCHER);
  }

  public static AnomalyFetcher fromClassName(String className)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException{
    return (new AnomalyFetcherFactory()).getInstance(className);
  }
}
