package com.linkedin.thirdeye.function;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.AnomalyFunctionSpec;

public class AnomalyFunctionFactory {
  private static Logger LOGGER = LoggerFactory.getLogger(AnomalyFunctionFactory.class);
  private final Properties props;

  public AnomalyFunctionFactory(String functionConfigPath) {
    props = new Properties();
    InputStream input = null;
    try {
      input = new FileInputStream(functionConfigPath);
      props.load(input);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(input);
    }

    LOGGER.info("Found {} entries in anomaly function configuration file {}", props.size(),
        functionConfigPath);
    for (Entry<Object, Object> entry : props.entrySet()) {
      LOGGER.info("{}: {}", entry.getKey(), entry.getValue());
    }
  }

  public AnomalyFunction fromSpec(AnomalyFunctionSpec functionSpec) throws Exception {
    AnomalyFunction anomalyFunction = null;
    String type = functionSpec.getType();
    if (!props.containsKey(type)) {
      throw new IllegalArgumentException("Unsupported type " + type);
    }
    String className = props.getProperty(type);
    anomalyFunction = (AnomalyFunction) Class.forName(className).newInstance();

    anomalyFunction.init(functionSpec);

    return anomalyFunction;
  }
}
