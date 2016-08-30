package com.linkedin.thirdeye.hadoop.wait;

import java.lang.reflect.Constructor;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.hadoop.wait.WaitPhaseJobConstants.*;

public class WaitPhaseJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(WaitPhaseJob.class);

  private String name;
  private Properties props;

  public WaitPhaseJob(String name, Properties props) {
    this.name = name;
    this.props = props;
  }

  public void run() {

    try {
      String thirdeyeWaitClass = getAndCheck(WAIT_UDF_CLASS.toString());

      if (thirdeyeWaitClass != null) {
        LOGGER.info("Initializing class {}", thirdeyeWaitClass);
        Constructor<?> constructor = Class.forName(thirdeyeWaitClass).getConstructor();
        WaitUDF waitUdf = (WaitUDF) constructor.newInstance();
        waitUdf.init(props);

        boolean complete = waitUdf.checkCompleteness();
        if (!complete) {
          throw new RuntimeException("Input folder {} has not received all records");
        }
      }
    }catch (Exception e) {
      LOGGER.error("Exception in waiting for inputs", e);
    }
  }

  private String getAndCheck(String propName) {
    String propValue = props.getProperty(propName);
    if (propValue == null) {
      throw new IllegalArgumentException(propName + " required property");
    }
    return propValue;
  }

}
