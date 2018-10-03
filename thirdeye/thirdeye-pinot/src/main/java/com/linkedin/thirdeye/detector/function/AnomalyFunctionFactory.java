/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detector.function;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
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

  public List<BaseAnomalyFunction> getSecondaryAnomalyFunctions(AnomalyFunctionDTO functionSpec) throws Exception {
    List<String> secondaryAnomalyFunctionsType = functionSpec.getSecondaryAnomalyFunctionsType();

    if (secondaryAnomalyFunctionsType == null) {
      LOGGER.info("null secondary anomaly function");
      return null;
    }

    List<BaseAnomalyFunction> baseAnomalyFunctions = new ArrayList<>();
    for (String secondaryAnomalyFunctionType : secondaryAnomalyFunctionsType) {
      if (secondaryAnomalyFunctionType == null || !props.containsKey(secondaryAnomalyFunctionType)) {
        LOGGER.error("Unsupported secondary anomaly function type " + secondaryAnomalyFunctionType);
        continue;
      }
      String className = props.getProperty(secondaryAnomalyFunctionType);
      BaseAnomalyFunction anomalyFunction = (BaseAnomalyFunction) Class.forName(className).newInstance();
      anomalyFunction.init(functionSpec);
      baseAnomalyFunctions.add(anomalyFunction);
    }

    return baseAnomalyFunctions;
  }

  public String getClassNameForFunctionType(String functionType) {
    return props.getProperty(functionType);
  }
}
