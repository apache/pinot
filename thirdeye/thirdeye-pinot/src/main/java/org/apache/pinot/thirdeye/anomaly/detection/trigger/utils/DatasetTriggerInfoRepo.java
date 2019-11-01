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

package org.apache.pinot.thirdeye.anomaly.detection.trigger.utils;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.MetricExpression;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.formatter.DetectionConfigFormatter;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to refresh a list of active dataset and its latest timestamp in memory so that
 * it can be used for event-driven scheduling.
 */
public class DatasetTriggerInfoRepo {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetTriggerInfoRepo.class);
  private static DatasetTriggerInfoRepo _instance = null;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static Set<String> dataSourceWhitelist = new HashSet<>();
  private static int refreshFreqInMin = 30;
  private Map<String, Long> datasetRefreshTimeMap;
  private ScheduledThreadPoolExecutor executorService;

  private DatasetTriggerInfoRepo() {
    this.datasetRefreshTimeMap = new ConcurrentHashMap<>();
    this.updateFreshTimeMap(); // initial refresh
    this.executorService = new ScheduledThreadPoolExecutor(1, r -> {
      Thread t = Executors.defaultThreadFactory().newThread(r);
      t.setDaemon(true);
      return t;
    });
    this.executorService.scheduleAtFixedRate(
        this::updateFreshTimeMap, refreshFreqInMin, refreshFreqInMin, TimeUnit.MINUTES);
  }

  public static void init(int refreshFreqInMin, Collection<String> dataSourceWhitelist) {
    DatasetTriggerInfoRepo.refreshFreqInMin = refreshFreqInMin;
    DatasetTriggerInfoRepo.dataSourceWhitelist = new HashSet<>(dataSourceWhitelist);
  }

  public static DatasetTriggerInfoRepo getInstance() {
    if (_instance == null) {
      synchronized (DatasetTriggerInfoRepo.class) {
        if (_instance == null) {
          _instance = new DatasetTriggerInfoRepo();
        }
      }
    }
    return _instance;
  }

  public boolean isDatasetActive(String dataset) {
    return datasetRefreshTimeMap.containsKey(dataset);
  }

  public long getLastUpdateTimestamp(String dataset) {
    return datasetRefreshTimeMap.getOrDefault(dataset, 0L);
  }

  public void setLastUpdateTimestamp(String dataset, long timestamp) {
    datasetRefreshTimeMap.put(dataset, timestamp);
  }

  public void close() {
    executorService.shutdown();
    _instance = null;
  }

  private void updateFreshTimeMap() {
    Set<Long> visitedMetrics = new HashSet<>(); // reduce the number of DB read
    DetectionConfigManager detectionConfigDAO = DAO_REGISTRY.getDetectionConfigManager();
    MetricConfigManager metricsConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    List<DetectionConfigDTO> detectionConfigs = detectionConfigDAO.findAllActive();
    LOG.info(String.format("Found %d active detection configs", detectionConfigs.size()));
    for (DetectionConfigDTO detectionConfig : detectionConfigs) {
      List<String> metricUrns = DetectionConfigFormatter
          .extractMetricUrnsFromProperties(detectionConfig.getProperties());
      for (String urn : metricUrns) {
        MetricEntity me = MetricEntity.fromURN(urn);
        if (visitedMetrics.contains(me.getId())) {
          // the metric is already visited before, so skipping.
          continue;
        }
        MetricConfigDTO metricConfig = metricsConfigDAO.findById(me.getId());
        if (metricConfig == null) {
          LOG.warn("Found null value for metric: " + me.getId());
          continue;
        }
        if (metricConfig.isDerived()) {
          MetricExpression metricExpression = ThirdEyeUtils.getMetricExpressionFromMetricConfig(metricConfig);
          List<MetricFunction> functions = metricExpression.computeMetricFunctions();
          functions.forEach(f -> add2CacheIfNeeded(f.getDataset()));
        } else {
          add2CacheIfNeeded(metricConfig.getDataset());
        }
        visitedMetrics.add(me.getId());
      }
    }
    LOG.info("Finished updating the list of dataset with size: " + datasetRefreshTimeMap.size());
  }

  private void add2CacheIfNeeded(String dataset) {
    DatasetConfigManager datasetConfigDAO = DAO_REGISTRY.getDatasetConfigDAO();
    if (!datasetRefreshTimeMap.containsKey(dataset)) {
      DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
      if (!dataSourceWhitelist.contains(datasetConfig.getDataSource())) {
        return;
      }
      datasetRefreshTimeMap.put(dataset, datasetConfig.getLastRefreshTime());
    }
  }
}
