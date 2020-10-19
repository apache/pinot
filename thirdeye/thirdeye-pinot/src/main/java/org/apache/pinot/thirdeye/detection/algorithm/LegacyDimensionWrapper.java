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

package org.apache.pinot.thirdeye.detection.algorithm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.util.ThirdEyeDataUtils;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detector.function.BaseAnomalyFunction;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;


/**
 * The Legacy dimension wrapper. Do dimension exploration for existing anomaly functions.
 */
public class LegacyDimensionWrapper extends DimensionWrapper {
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_LOOKBACK = "lookback";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_SPEC = "specs";
  private static final String PROP_ANOMALY_FUNCTION_CLASS = "anomalyFunctionClassName";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String SPEC_METRIC_ID = "metricId";
  private static final String SPEC_FILTERS = "filters";

  private final BaseAnomalyFunction anomalyFunction;
  private final Map<String, Object> anomalyFunctionSpecs;
  private final String anomalyFunctionClassName;

  /**
   * Instantiates a new Legacy dimension wrapper.
   *
   * @param provider the provider
   * @param config the config
   * @param startTime the start time
   * @param endTime the end time
   * @throws Exception the exception
   */
  public LegacyDimensionWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) throws Exception {
    super(provider, augmentConfig(config), startTime, endTime);

    this.anomalyFunctionClassName = MapUtils.getString(config.getProperties(), PROP_ANOMALY_FUNCTION_CLASS);
    this.anomalyFunctionSpecs = ConfigUtils.getMap(config.getProperties().get(PROP_SPEC));
    this.anomalyFunction = (BaseAnomalyFunction) Class.forName(this.anomalyFunctionClassName).newInstance();

    String specs = OBJECT_MAPPER.writeValueAsString(this.anomalyFunctionSpecs);
    this.anomalyFunction.init(OBJECT_MAPPER.readValue(specs, AnomalyFunctionDTO.class));
    if (!StringUtils.isBlank(this.anomalyFunction.getSpec().getExploreDimensions())) {
      this.dimensions.add(this.anomalyFunction.getSpec().getExploreDimensions());
    }

    if (this.nestedProperties.isEmpty()) {
      this.nestedProperties.add(Collections.singletonMap(PROP_CLASS_NAME, (Object) LegacyAnomalyFunctionAlgorithm.class.getName()));
    }
  }

  @Override
  protected DetectionPipelineResult runNested(
      Map<String, Object> nestedProps, final long startTime, final long endTime) throws Exception {
    if (!nestedProps.containsKey(PROP_SPEC)) {
      nestedProps.put(PROP_SPEC, this.anomalyFunctionSpecs);
    }
    if (!nestedProps.containsKey(PROP_ANOMALY_FUNCTION_CLASS)) {
      nestedProps.put(PROP_ANOMALY_FUNCTION_CLASS, this.anomalyFunctionClassName);
    }
    return super.runNested(nestedProps, startTime, endTime);
  }

  private static DetectionConfigDTO augmentConfig(DetectionConfigDTO config) {
    config.setProperties(augmentProperties(config.getProperties()));
    return config;
  }

  private static Map<String, Object> augmentProperties(Map<String, Object> properties) {
    Map<String, Object> spec = ConfigUtils.getMap(properties.get(PROP_SPEC));

    if (!properties.containsKey(PROP_METRIC_URN)) {
      long metricId = MapUtils.getLongValue(spec, SPEC_METRIC_ID);
      Multimap<String, String> filters = ThirdEyeDataUtils
          .getFilterSet(MapUtils.getString(spec, SPEC_FILTERS));
      properties.put(PROP_METRIC_URN,MetricEntity.fromMetric(1.0, metricId, filters).getUrn());
    }

    if (!properties.containsKey(PROP_LOOKBACK)) {
      properties.put(PROP_LOOKBACK, "0");
    }

    return properties;
  }
}
