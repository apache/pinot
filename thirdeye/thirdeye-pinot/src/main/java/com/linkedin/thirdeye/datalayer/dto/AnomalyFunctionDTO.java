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

package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.util.StringUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.collections.CollectionUtils;
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
    if (CollectionUtils.isEmpty((super.getMetrics()))) {
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
    Properties props = null;
    try {
      props = toProperties(getProperties());
    } catch (IOException e) {
      LOGGER.warn("Failed to parse property string ({}) for anomaly function: {}", getProperties(), getId());
    }
    return props;
  }

  /**
   * Parses the properties of String and returns the corresponding Properties object.
   *
   * @return a Properties object corresponds to the properties String of this anomaly function.
   */
  public static Properties toProperties(String properties) throws IOException {
    if (properties == null || properties.isEmpty()) {
      return new Properties();
    }
    return StringUtils.decodeCompactedProperties(properties);
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
    setProperties(StringUtils.encodeCompactedProperties(properties));
  }
}
