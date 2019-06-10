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

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.quartz.CronExpression;


/**
 * A data holder to store the processed information per metric
 */
class DetectionConfigMetricCache {

  private static final String PROP_METRIC = "metric";
  private static final String PROP_DATASET = "dataset";

  private final Map<String, DetectionMetricProperties> metricCache = new HashMap<>();
  private final DataProvider dataProvider;

  DetectionConfigMetricCache(DataProvider provider) {
    this.dataProvider = provider;
  }

  private void loadMetricCache(Map<String, Object> metricAlertConfigMap) {
    String metricName = MapUtils.getString(metricAlertConfigMap, PROP_METRIC);
    String datasetName = MapUtils.getString(metricAlertConfigMap, PROP_DATASET);

    MetricConfigDTO metricConfig = this.dataProvider.fetchMetric(metricName, datasetName);
    DatasetConfigDTO datasetConfig = this.dataProvider.fetchDatasets(Collections.singletonList(metricConfig.getDataset()))
        .get(metricConfig.getDataset());
    String cron = buildCron(datasetConfig.bucketTimeGranularity());

    metricCache.put(metricName, new DetectionMetricProperties(cron, metricConfig, datasetConfig));
  }

  DatasetConfigDTO fetchDataset(Map<String, Object> metricAlertConfigMap) {
    String metricName = MapUtils.getString(metricAlertConfigMap, PROP_METRIC);
    if (!metricCache.containsKey(metricName)) {
      loadMetricCache(metricAlertConfigMap);
    }

    return metricCache.get(metricName).getDatasetConfigDTO();
  }

  MetricConfigDTO fetchMetric(Map<String, Object> metricAlertConfigMap) {
    String metricName = MapUtils.getString(metricAlertConfigMap, PROP_METRIC);
    if (!metricCache.containsKey(metricName)) {
      loadMetricCache(metricAlertConfigMap);
    }

    return metricCache.get(metricName).getMetricConfigDTO();
  }

  String fetchCron(Map<String, Object> metricAlertConfigMap) {
    String metricName = MapUtils.getString(metricAlertConfigMap, PROP_METRIC);
    if (!metricCache.containsKey(metricName)) {
      loadMetricCache(metricAlertConfigMap);
    }

    return metricCache.get(metricName).getCron();
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
}
