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
 */

package org.apache.pinot.thirdeye.detection;

import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.algorithm.LegacyAlertFilterWrapper;
import org.apache.pinot.thirdeye.detector.email.filter.AlertFilterFactory;
import org.apache.pinot.thirdeye.detector.function.AnomalyFunctionFactory;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Legacy Anomaly function translator.
 */
@Deprecated
public class LegacyAnomalyFunctionTranslator {
  private static final Logger LOGGER = LoggerFactory.getLogger(LegacyAnomalyFunctionTranslator.class);

  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_ANOMALY_FUNCTION_CLASS_NAME = "anomalyFunctionClassName";
  private static final String PROP_SPECS = "specs";
  private static final String PROP_ALERT_FILTER_LOOK_BACK = "alertFilterLookBack";
  private static final String PROP_LEGACY_ALERT_FILTER_CLASS_NAME = "legacyAlertFilterClassName";

  private final MetricConfigManager metricConfigDAO;
  private final AnomalyFunctionFactory anomalyFunctionFactory;
  private final AlertFilterFactory alertFilterFactory;

  /**
   * Instantiates a new Legacy Anomaly function translator.
   */
  public LegacyAnomalyFunctionTranslator(MetricConfigManager metricConfigDAO,
      AnomalyFunctionFactory anomalyFunctionFactory, AlertFilterFactory alertFilterFactory) {
    this.metricConfigDAO = metricConfigDAO;
    this.anomalyFunctionFactory = anomalyFunctionFactory;
    this.alertFilterFactory = alertFilterFactory;
  }

  /**
   * Translate a legacy anomaly function to a new detection config.
   */
  public DetectionConfigDTO translate(AnomalyFunctionDTO anomalyFunctionDTO) throws Exception {
    if (anomalyFunctionDTO.getMetricId() <= 0) {
      MetricConfigDTO metricDTO = this.metricConfigDAO.findByMetricAndDataset(anomalyFunctionDTO.getMetric(), anomalyFunctionDTO.getCollection());
      if (metricDTO == null) {
        LOGGER.error("Cannot find metric {} for anomaly function {}", anomalyFunctionDTO.getMetric(), anomalyFunctionDTO.getFunctionName());
        throw new Exception("Cannot find metric");
      }
      anomalyFunctionDTO.setMetricId(metricDTO.getId());
    }

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, LegacyAlertFilterWrapper.class);
    properties.put(PROP_ANOMALY_FUNCTION_CLASS_NAME, anomalyFunctionFactory.getClassNameForFunctionType(anomalyFunctionDTO.getType().toUpperCase()));
    properties.put(PROP_SPECS, anomalyFunctionDTO);

    if (anomalyFunctionDTO.getAlertFilter() != null) {
      String type = anomalyFunctionDTO.getAlertFilter().get("type");
      if (type != null) {
        properties.put(PROP_LEGACY_ALERT_FILTER_CLASS_NAME, this.alertFilterFactory.getClassNameForAlertFilterType(type.toUpperCase()));
        properties.put(PROP_ALERT_FILTER_LOOK_BACK, "1d");
      }
    }

    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setName(anomalyFunctionDTO.getFunctionName());
    config.setCron(anomalyFunctionDTO.getCron());
    config.setProperties(properties);
    config.setActive(false);

    return config;
  }
}
