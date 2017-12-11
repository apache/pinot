package com.linkedin.thirdeye.alert.commons;

import com.linkedin.thirdeye.alert.content.EmailContentFormatter;
import com.linkedin.thirdeye.common.BaseFactory;


public class EmailContentFormatterFactory extends BaseFactory<EmailContentFormatter> {
  public static final String PACKAGE_PATH = "com.linkedin.thirdeye.alert.content";

  public static EmailContentFormatter fromClassName(String className)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException{
    String classPath = PACKAGE_PATH + "." + className;
    return (new BaseFactory<EmailContentFormatter>()).getInstance(classPath);
  }
}
