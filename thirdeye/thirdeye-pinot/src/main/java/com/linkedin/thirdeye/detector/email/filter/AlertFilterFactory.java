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

package com.linkedin.thirdeye.detector.email.filter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class AlertFilterFactory {
  private static Logger LOGGER = LoggerFactory.getLogger(AlertFilterFactory.class);
  public static final String FILTER_TYPE_KEY = "type";
  private final Properties props;


  public AlertFilterFactory(String AlertFilterConfigPath) {
    props = new Properties();
    try {
      InputStream input = new FileInputStream(AlertFilterConfigPath);
      loadPropertiesFromInputStream(input);
    } catch (FileNotFoundException e) {
      LOGGER.error("Alert Filter Property File {} not found", AlertFilterConfigPath, e);
    }
  }

  public AlertFilterFactory(InputStream input) {
    props = new Properties();
    loadPropertiesFromInputStream(input);
  }

  private void loadPropertiesFromInputStream(InputStream input) {
    try {
      props.load(input);
    } catch (IOException e) {
      LOGGER.error("Error loading the alert filters from config", e);
    } finally {
      IOUtils.closeQuietly(input);
    }

    LOGGER.info("Found {} entries in alert filter configuration file {}", props.size());
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      LOGGER.info("{}: {}", entry.getKey(), entry.getValue());
    }
  }


  /**
   * Get an alert filter for the given alert filter spec.
   *
   * @param alertFilterSpec the anomaly function that contains the alert filter spec.
   *        alertFilterSpec can be obtained by using AnomalyFunctionDTO getAlertFilter()
   * @return the alert filter specified by the alert filter spec or a dummy filter if the function
   * does not have an alert filter spec or this method fails to initiates an alert filter from the
   * spec.
   */
  public BaseAlertFilter fromSpec(Map<String, String> alertFilterSpec) {
    if (alertFilterSpec == null) {
      alertFilterSpec = Collections.emptyMap();
    }
    // the default alert filter is DummyAlertFilter
    BaseAlertFilter alertFilter = new DummyAlertFilter();
    if (alertFilterSpec.containsKey(FILTER_TYPE_KEY)) {
      String alertFilterType = alertFilterSpec.get(FILTER_TYPE_KEY);
      if(props.containsKey(alertFilterType.toUpperCase())) {
        String className = props.getProperty(alertFilterType.toUpperCase());
        try {
          alertFilter = (BaseAlertFilter) Class.forName(className).newInstance();
        } catch (Exception e) {
          LOGGER.warn(e.getMessage());
        }
      }
    }
    alertFilter.setParameters(alertFilterSpec);
    return alertFilter;
  }

  public String getClassNameForAlertFilterType(String type) {
    return props.getProperty(type);
  }
}
