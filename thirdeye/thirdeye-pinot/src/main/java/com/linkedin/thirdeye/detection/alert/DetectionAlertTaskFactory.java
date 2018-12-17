package com.linkedin.thirdeye.detection.alert;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.loader.AggregationLoader;
import com.linkedin.thirdeye.datasource.loader.DefaultAggregationLoader;
import com.linkedin.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import com.linkedin.thirdeye.datasource.loader.TimeSeriesLoader;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DefaultDataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineLoader;
import com.linkedin.thirdeye.detection.alert.scheme.DetectionAlertScheme;
import com.linkedin.thirdeye.detection.alert.suppress.DetectionAlertSuppressor;
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
  private static final String DEFAULT_ALERT_SCHEME = "com.linkedin.thirdeye.detection.alert.scheme.DetectionEmailAlerter";
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private final DataProvider provider;

  public DetectionAlertTaskFactory() {
    EventManager eventDAO = DAO_REGISTRY.getEventDAO();
    MetricConfigManager metricDAO = DAO_REGISTRY.getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAO_REGISTRY.getDatasetConfigDAO();
    MergedAnomalyResultManager anomalyMergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();

    TimeSeriesLoader timeseriesLoader = new DefaultTimeSeriesLoader(metricDAO, datasetDAO,
        ThirdEyeCacheRegistry.getInstance().getQueryCache());
    AggregationLoader aggregationLoader = new DefaultAggregationLoader(metricDAO, datasetDAO,
        ThirdEyeCacheRegistry.getInstance().getQueryCache(),
        ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());
    this.provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, anomalyMergedResultDAO,
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
      emailScheme.put("className", DEFAULT_ALERT_SCHEME);
      alertSchemes = Collections.singletonMap("EmailScheme", emailScheme);
    }
    Set<DetectionAlertScheme> detectionAlertSchemeSet = new HashSet<>();
    for (String alertSchemeType : alertSchemes.keySet()) {
      LOG.debug("Loading Alert Scheme : {}", alertSchemeType);
      Preconditions.checkNotNull(alertSchemes.get(alertSchemeType));
      Preconditions.checkNotNull(alertSchemes.get(alertSchemeType).get("className"));
      Constructor<?> constructor = Class.forName(alertSchemes.get(alertSchemeType).get("className").toString().trim())
          .getConstructor(DetectionAlertConfigDTO.class, ThirdEyeAnomalyConfiguration.class, DetectionAlertFilterResult.class);
      detectionAlertSchemeSet.add((DetectionAlertScheme) constructor.newInstance(alertConfig,
          thirdeyeConfig, result));
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
      Preconditions.checkNotNull(alertSuppressors.get(alertSuppressor).get("className"));
      Constructor<?> constructor = Class.forName(alertSuppressors.get(alertSuppressor).get("className").toString().trim())
          .getConstructor(DetectionAlertConfigDTO.class);
      detectionAlertSuppressors.add((DetectionAlertSuppressor) constructor.newInstance(alertConfig));
    }

    return detectionAlertSuppressors;
  }
}
