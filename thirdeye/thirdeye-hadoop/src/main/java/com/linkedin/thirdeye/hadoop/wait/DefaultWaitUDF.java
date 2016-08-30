package com.linkedin.thirdeye.hadoop.wait;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultWaitUDF implements WaitUDF {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultWaitUDF.class);

  private Properties inputConfig;

  public DefaultWaitUDF() {

  }

  @Override
  public void init(Properties inputConfig) {
    this.inputConfig = inputConfig;
  }

  @Override
  // default implementation always returns complete
  public boolean checkCompleteness() {
    return true;
  }

}
