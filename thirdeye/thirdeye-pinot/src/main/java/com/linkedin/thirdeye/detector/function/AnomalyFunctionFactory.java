package com.linkedin.thirdeye.detector.function;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;

public class AnomalyFunctionFactory {
  private static Logger LOGGER = LoggerFactory.getLogger(AnomalyFunctionFactory.class);
  private final Properties props;

  public AnomalyFunctionFactory(String functionConfigPath) {
    props = new Properties();

    try {
      InputStream input = new FileInputStream(functionConfigPath);
      loadPropertiesFromInputStream(input);
    } catch (FileNotFoundException e) {
      LOGGER.error("File {} not found", functionConfigPath, e);
    }

  }

  public AnomalyFunctionFactory(InputStream input) {
    props = new Properties();
    loadPropertiesFromInputStream(input);
  }

  private void loadPropertiesFromInputStream(InputStream input) {
    try {
      props.load(input);
    } catch (IOException e) {
      LOGGER.error("Error loading the functions from config", e);
    } finally {
      IOUtils.closeQuietly(input);
    }

    LOGGER.info("Found {} entries in anomaly function configuration file {}", props.size());
    for (Entry<Object, Object> entry : props.entrySet()) {
      LOGGER.info("{}: {}", entry.getKey(), entry.getValue());
    }
  }

  public BaseAnomalyFunction fromSpec(AnomalyFunctionDTO functionSpec) throws Exception {
    BaseAnomalyFunction anomalyFunction = null;
    String type = functionSpec.getType();
    if (!props.containsKey(type)) {
      throw new IllegalArgumentException("Unsupported type " + type);
    }
    String className = props.getProperty(type);
    anomalyFunction = (BaseAnomalyFunction) Class.forName(className).newInstance();

    anomalyFunction.init(functionSpec);
    return anomalyFunction;
  }
}
