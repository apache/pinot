package com.linkedin.thirdeye.anomaly.generic;

import java.net.URL;
import java.net.URLClassLoader;

import org.testng.annotations.Test;

import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction;

/**
 *
 */
public class GenericFunctionFactoryTest {

  @Test
  public void loadFunction() throws Exception {
    URL oddJarUrl = ClassLoader.getSystemResource("generic/jars/OddFunction.jar");

    URLClassLoader loader = new URLClassLoader(new URL[]{oddJarUrl});
    Class functionClass = Class.forName("reflection.OddAnomalyDetectionFunction", true, loader);

    AnomalyDetectionFunction function = (AnomalyDetectionFunction) functionClass.newInstance();

    // null pointer exception is expected since StarTreeConfig is null
    try {
      function.init(null, new FunctionProperties());
    } catch (NullPointerException e) {

    }
  }
}
