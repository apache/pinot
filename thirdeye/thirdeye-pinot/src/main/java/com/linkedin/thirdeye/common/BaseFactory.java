package com.linkedin.thirdeye.common;

public class BaseFactory<T> {
  protected FactorySource factorySource;
  public BaseFactory(FactorySource factorySource) {
    this.factorySource = factorySource;
  }
  public <T> T getInstance(String classType)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException{
    T typeInstance = (T) Class.forName(factorySource.getPackagePath() + "." + classType).newInstance();
    return typeInstance;
  }
}
