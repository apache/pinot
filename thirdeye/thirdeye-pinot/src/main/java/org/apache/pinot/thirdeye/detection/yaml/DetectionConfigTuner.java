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

package org.apache.pinot.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.spec.AbstractSpec;
import org.apache.pinot.thirdeye.detection.spi.components.Tunable;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.detection.DetectionUtils.*;


/**
 * This class focuses on tuning the detection config for the provided tuning window
 *
 * This wraps and calls the appropriate tunable implementation
 */
public class DetectionConfigTuner {
  protected static final Logger LOG = LoggerFactory.getLogger(DetectionConfigTuner.class);

  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String DEFAULT_TIMEZONE = "America/Los_Angeles";

  private static final DetectionRegistry DETECTION_REGISTRY = DetectionRegistry.getInstance();
  static {
    // do not tune for alerts migrated from legacy anomaly function.
    DetectionRegistry.registerComponent("com.linkedin.thirdeye.detection.components.AdLibAlertFilter",
        "MIGRATED_ALGORITHM_FILTER");
    DetectionRegistry.registerComponent("com.linkedin.thirdeye.detection.components.AdLibAnomalyDetector",
        "MIGRATED_ALGORITHM");
  }

  public static final String PROP_YAML_PARAMS = "yamlParams";
  public static final Set<String> TURNOFF_TUNING_COMPONENTS =
      ImmutableSet.of("MIGRATED_ALGORITHM_FILTER", "MIGRATED_ALGORITHM", "MIGRATED_ALGORITHM_BASELINE");

  private final DetectionConfigDTO detectionConfig;
  private final DataProvider dataProvider;

  public DetectionConfigTuner(DetectionConfigDTO config, DataProvider dataProvider) {
    Preconditions.checkNotNull(config);
    this.detectionConfig = config;
    this.dataProvider = dataProvider;
  }

  /**
   * Returns the default(for new alert) or tuned configuration for the specified component
   *
   * @param componentProps previously tuned configuration along with user supplied yaml config
   * @param startTime start time of the tuning window
   * @param endTime end time of the tuning window
   */
  private Map<String, Object> getTunedSpecs(Map<String, Object> componentProps, long startTime, long endTime)
      throws Exception {
    Map<String, Object> tunedSpec = new HashMap<>();

    // Instantiate tunable component
    long configId = detectionConfig.getId() == null ? 0 : detectionConfig.getId();
    String componentClassName = componentProps.get(PROP_CLASS_NAME).toString();
    Map<String, Object> yamlParams = ConfigUtils.getMap(componentProps.get(PROP_YAML_PARAMS));
    InputDataFetcher dataFetcher = new DefaultInputDataFetcher(dataProvider, configId);
    Tunable tunable = instantiateTunable(componentClassName, yamlParams, dataFetcher);

    // round to daily boundary
    Preconditions.checkNotNull(componentProps.get(PROP_METRIC_URN));
    String metricUrn = componentProps.get(PROP_METRIC_URN).toString();
    MetricEntity metricEntity = MetricEntity.fromURN(metricUrn);
    Map<Long, MetricConfigDTO> metricConfigMap = dataProvider.fetchMetrics(Collections.singleton(metricEntity.getId()));
    Preconditions.checkArgument(metricConfigMap.size() == 1, "Unable to find the metric to tune");

    DatasetConfigDTO datasetConfig = metricConfigMap.values().iterator().next().getDatasetConfig();
    DateTimeZone timezone = DateTimeZone.forID(datasetConfig.getTimezone() == null ? DEFAULT_TIMEZONE : datasetConfig.getTimezone());
    DateTime start = new DateTime(startTime, timezone).withTimeAtStartOfDay();
    DateTime end =  new DateTime(endTime, timezone).withTimeAtStartOfDay();
    Interval window = new Interval(start, end);

    // TODO: if dimension drill down applied, pass in the metric urn of top dimension
    tunedSpec.putAll(tunable.tune(componentProps, window, metricUrn));

    return tunedSpec;
  }

  private Tunable instantiateTunable(String componentClassName, Map<String, Object> yamlParams, InputDataFetcher dataFetcher)
      throws Exception {
    String tunableClassName = DETECTION_REGISTRY.lookupTunable(componentClassName);
    Class clazz = Class.forName(tunableClassName);
    Class<AbstractSpec> specClazz = (Class<AbstractSpec>) Class.forName(getSpecClassName(clazz));
    AbstractSpec spec = AbstractSpec.fromProperties(yamlParams, specClazz);
    Tunable tunable = (Tunable) clazz.newInstance();
    tunable.init(spec, dataFetcher);
    return tunable;
  }

  /**
   * Scans through all the tunable components and injects tuned model params into the detection config
   */
  public DetectionConfigDTO tune(long tuningWindowStart, long tuningWindowEnd) {
    Map<String, Object> tunedComponentSpecs = new HashMap<>();

    // Tune each tunable component in the detection componentSpecs
    Map<String, Object> allComponentSpecs = this.detectionConfig.getComponentSpecs();
    for (Map.Entry<String, Object> componentSpec : allComponentSpecs.entrySet()) {
      Map<String, Object> tunedComponentProps = new HashMap<>();

      String componentKey = componentSpec.getKey();
      Map<String, Object> existingComponentProps = ConfigUtils.getMap(componentSpec.getValue());

      // For tunable components, the model params are computed from user supplied yaml params and previous model params.
      String componentClassName = existingComponentProps.get(PROP_CLASS_NAME).toString();
      String type = DetectionUtils.getComponentType(componentKey);
      if (!TURNOFF_TUNING_COMPONENTS.contains(type) && DETECTION_REGISTRY.isTunable(componentClassName)) {
        try {
          tunedComponentProps.putAll(getTunedSpecs(existingComponentProps, tuningWindowStart, tuningWindowEnd));
        } catch (Exception e) {
          LOG.error("Tuning failed for component " + type, e);
        }
      }

      tunedComponentProps.putAll(existingComponentProps);
      tunedComponentSpecs.put(componentKey, tunedComponentProps);
    }

    detectionConfig.setComponentSpecs(tunedComponentSpecs);
    return detectionConfig;
  }
}
