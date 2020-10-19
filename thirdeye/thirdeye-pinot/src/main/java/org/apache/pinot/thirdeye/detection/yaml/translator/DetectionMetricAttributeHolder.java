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

package org.apache.pinot.thirdeye.detection.yaml.translator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;

import static org.apache.pinot.thirdeye.detection.ConfigUtils.*;


/**
 * A data holder to store the processed information per metric
 */
public class DetectionMetricAttributeHolder {

  private static final String PROP_METRIC = "metric";
  private static final String PROP_DATASET = "dataset";
  private static final String PROP_CRON = "cron";

  private final Map<String, DetectionMetricProperties> metricAttributesMap = new HashMap<>();
  private final DataProvider dataProvider;
  private final List<DatasetConfigDTO> datasetConfigs = new ArrayList<>();
  private final Map<String, Object> components = new HashMap<>();

  DetectionMetricAttributeHolder(DataProvider provider) {
    this.dataProvider = provider;
  }

  private String loadMetricCache(Map<String, Object> metricAlertConfigMap) {
    String metricName = MapUtils.getString(metricAlertConfigMap, PROP_METRIC);
    String datasetName = MapUtils.getString(metricAlertConfigMap, PROP_DATASET);
    String cron = MapUtils.getString(metricAlertConfigMap, PROP_CRON);
    String metricAliasKey = ThirdEyeUtils.constructMetricAlias(datasetName, metricName);
    if (metricAttributesMap.containsKey(metricAliasKey)) {
      return metricAliasKey;
    }

    DatasetConfigDTO datasetConfig = fetchDatasetConfigDTO(this.dataProvider, datasetName);
    datasetConfigs.add(datasetConfig);

    MetricConfigDTO metricConfig = this.dataProvider.fetchMetric(metricName, datasetConfig.getDataset());

    cron = cron == null ? buildCron(datasetConfig.bucketTimeGranularity()) : cron;

    metricAttributesMap.put(metricAliasKey, new DetectionMetricProperties(cron, metricConfig, datasetConfig));

    return metricAliasKey;
  }

  public List<DatasetConfigDTO> getAllDatasets() {
    return datasetConfigs;
  }

  public DatasetConfigDTO fetchDataset(Map<String, Object> metricAlertConfigMap) {
    return metricAttributesMap.get(loadMetricCache(metricAlertConfigMap)).getDatasetConfigDTO();
  }

  public MetricConfigDTO fetchMetric(Map<String, Object> metricAlertConfigMap) {
    return metricAttributesMap.get(loadMetricCache(metricAlertConfigMap)).getMetricConfigDTO();
  }

  public String fetchCron(Map<String, Object> metricAlertConfigMap) {
    return metricAttributesMap.get(loadMetricCache(metricAlertConfigMap)).getCron();
  }

  //  Default schedule:
  //  minute granularity: every 15 minutes, starts at 0 minute
  //  hourly: every hour, starts at 0 minute
  //  daily: every day, starts at 2 pm UTC
  //  others: every day, start at 12 am UTC
  private String buildCron(TimeGranularity timegranularity) {
    switch (timegranularity.getUnit()) {
      case MINUTES:
        return "0 0/15 * * * ? *";
      case HOURS:
        return "0 0 * * * ? *";
      case DAYS:
        return "0 0 14 * * ? *";
      default:
        return "0 0 0 * * ?";
    }
  }

  public void addComponent(String componentKey, Map<String, Object> componentSpecs) {
    components.put(componentKey, componentSpecs);
  }

  public Map<String, Object> getAllComponents() {
    return components;
  }

}
