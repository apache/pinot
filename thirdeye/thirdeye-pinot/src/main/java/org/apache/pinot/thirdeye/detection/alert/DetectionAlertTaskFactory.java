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

package org.apache.pinot.thirdeye.detection.alert;

import com.google.common.base.Preconditions;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.detection.alert.scheme.DetectionAlertScheme;
import org.apache.pinot.thirdeye.detection.alert.suppress.DetectionAlertSuppressor;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DetectionAlertTaskFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionAlertTaskFactory.class);

  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_EMAIL_SCHEME = "emailScheme";
  private static final String DEFAULT_ALERT_SCHEME = "org.apache.pinot.thirdeye.detection.alert.scheme.DetectionEmailAlerter";
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private final DataProvider provider;

  public DetectionAlertTaskFactory() {
    EventManager eventDAO = DAO_REGISTRY.getEventDAO();
    MetricConfigManager metricDAO = DAO_REGISTRY.getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAO_REGISTRY.getDatasetConfigDAO();
    MergedAnomalyResultManager anomalyMergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    EvaluationManager evaluationDAO = DAO_REGISTRY.getEvaluationManager();
    TimeSeriesLoader timeseriesLoader = new DefaultTimeSeriesLoader(metricDAO, datasetDAO,
        ThirdEyeCacheRegistry.getInstance().getQueryCache(), ThirdEyeCacheRegistry.getInstance().getTimeSeriesCache());
    AggregationLoader aggregationLoader = new DefaultAggregationLoader(metricDAO, datasetDAO,
        ThirdEyeCacheRegistry.getInstance().getQueryCache(),
        ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());
    this.provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, anomalyMergedResultDAO, evaluationDAO,
        timeseriesLoader, aggregationLoader, new DetectionPipelineLoader());
  }

  public DetectionAlertFilter loadAlertFilter(DetectionAlertConfigDTO alertConfig, long endTime)
      throws Exception {
    Preconditions.checkNotNull(alertConfig);
    String className = alertConfig.getProperties().get(PROP_CLASS_NAME).toString();
    LOG.debug("Loading Alert Filter : {}", className);
    Constructor<?> constructor = Class.forName(className)
        .getConstructor(DataProvider.class, DetectionAlertConfigDTO.class, long.class);
    return (DetectionAlertFilter) constructor.newInstance(provider, alertConfig, endTime);

  }

  public Set<DetectionAlertScheme> loadAlertSchemes(DetectionAlertConfigDTO alertConfig,
      ThirdEyeAnomalyConfiguration thirdeyeConfig, DetectionAlertFilterResult result) throws Exception {
    Preconditions.checkNotNull(alertConfig);
    Map<String, Map<String, Object>> alertSchemes = alertConfig.getAlertSchemes();
    if (alertSchemes == null || alertSchemes.isEmpty()) {
      Map<String, Object> emailScheme = new HashMap<>();
      emailScheme.put(PROP_CLASS_NAME, DEFAULT_ALERT_SCHEME);
      alertSchemes = Collections.singletonMap(PROP_EMAIL_SCHEME, emailScheme);
    }
    Set<DetectionAlertScheme> detectionAlertSchemeSet = new HashSet<>();
    for (String alertSchemeType : alertSchemes.keySet()) {
      LOG.debug("Loading Alert Scheme : {}", alertSchemeType);
      Preconditions.checkNotNull(alertSchemes.get(alertSchemeType));
      Preconditions.checkNotNull(alertSchemes.get(alertSchemeType).get(PROP_CLASS_NAME));
      Constructor<?> constructor = Class.forName(alertSchemes.get(alertSchemeType).get(PROP_CLASS_NAME).toString().trim())
          .getConstructor(DetectionAlertConfigDTO.class, ThirdEyeAnomalyConfiguration.class, DetectionAlertFilterResult.class);
      detectionAlertSchemeSet.add((DetectionAlertScheme) constructor.newInstance(alertConfig, thirdeyeConfig, result));
    }
    return detectionAlertSchemeSet;
  }

  public Set<DetectionAlertSuppressor> loadAlertSuppressors(DetectionAlertConfigDTO alertConfig) throws Exception {
    Preconditions.checkNotNull(alertConfig);
    Set<DetectionAlertSuppressor> detectionAlertSuppressors = new HashSet<>();
    Map<String, Map<String, Object>> alertSuppressors = alertConfig.getAlertSuppressors();
    if (alertSuppressors == null || alertSuppressors.isEmpty()) {
      return detectionAlertSuppressors;
    }

    for (String alertSuppressor : alertSuppressors.keySet()) {
      LOG.debug("Loading Alert Suppressor : {}", alertSuppressor);
      Preconditions.checkNotNull(alertSuppressors.get(alertSuppressor));
      Preconditions.checkNotNull(alertSuppressors.get(alertSuppressor).get(PROP_CLASS_NAME));
      Constructor<?> constructor = Class.forName(alertSuppressors.get(alertSuppressor).get(PROP_CLASS_NAME).toString().trim())
          .getConstructor(DetectionAlertConfigDTO.class);
      detectionAlertSuppressors.add((DetectionAlertSuppressor) constructor.newInstance(alertConfig));
    }

    return detectionAlertSuppressors;
  }
}
