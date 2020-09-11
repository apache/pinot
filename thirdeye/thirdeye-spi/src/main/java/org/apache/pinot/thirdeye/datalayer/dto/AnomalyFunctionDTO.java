/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.pinot.thirdeye.datalayer.dto;

import org.apache.pinot.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import org.apache.pinot.thirdeye.datalayer.util.ThirdEyeStringUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnomalyFunctionDTO extends AnomalyFunctionBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyFunctionDTO.class);

  public String getTopicMetric() {
    return getMetric();
  }

  public void setTopicMetric(String topicMetric) {
    setMetric(topicMetric);
  }

  // TODO: Remove this method and update the value in DB instead
  /**
   * Returns the list of metrics to be retrieved for anomaly detection. If the information is not
   * provided in the DB, then it returns a list that contains only topic metric.
   *
   * @return a list of metrics to be retrieved for anomaly detection.
   */
  @Override
  public List<String> getMetrics() {
    if (super.getMetrics() == null || super.getMetrics().isEmpty()) {
      return Arrays.asList(getMetric());
    } else {
      return super.getMetrics();
    }
  }

  /**
   * Return if this function should get total metric for anomaly calculation
   * @return
   * true if this function should get total metric
   */
  public boolean isToCalculateGlobalMetric() {
    return org.apache.commons.lang3.StringUtils.isNotEmpty(this.getGlobalMetric());
  }

  /**
   * Parses the properties of String and returns the corresponding Properties object.
   *
   * @return a Properties object corresponds to the properties String of this anomaly function.
   */
  public Properties toProperties() {
    return toProperties(getProperties());
  }

  /**
   * Parses the properties of String and returns the corresponding Properties object.
   *
   * @return a Properties object corresponds to the properties String of this anomaly function.
   */
  public static Properties toProperties(String properties) {
    if (properties == null || properties.isEmpty()) {
      return new Properties();
    }
    return ThirdEyeStringUtils.decodeCompactedProperties(properties);
  }

  /**
   * Update values of entries in Properties
   * @param config
   * the configuration need to be updated in the function properties
   */
  public void updateProperties(Map<String, String> config) {
    Properties properties = toProperties();
    for (Map.Entry<String, String> entry : config.entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }
    setProperties(ThirdEyeStringUtils.encodeCompactedProperties(properties));
  }
}
