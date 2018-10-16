package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
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
import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DetectionAlertTaskFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionAlertTaskFactory.class);

  private static final String PROP_CLASS_NAME = "className";
  private static final String DEFAULT_ALERT_SCHEME = "com.linkedin.thirdeye.detection.alert.scheme.DetectionEmailAlerter";
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private final DataProvider provider;
  private DetectionAlertConfigManager alertConfigDAO;

  public DetectionAlertTaskFactory() {
    this.alertConfigDAO = DAO_REGISTRY.getDetectionAlertConfigManager();

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

  public DetectionAlertFilter loadAlertFilter(long detectionAlertConfigId, long endTime)
      throws Exception {
    DetectionAlertConfigDTO alertConfig = loadDetectionAlertConfig(detectionAlertConfigId);
    String className = alertConfig.getProperties().get(PROP_CLASS_NAME).toString();
    Constructor<?> constructor = Class.forName(className)
        .getConstructor(DataProvider.class, DetectionAlertConfigDTO.class, long.class);
    return (DetectionAlertFilter) constructor.newInstance(provider, alertConfig, endTime);

  }

  public Set<DetectionAlertScheme> loadAlertSchemes(long detectionAlertConfigId, TaskContext taskContext,
      DetectionAlertFilterResult result) throws Exception {
    DetectionAlertConfigDTO alertConfig = loadDetectionAlertConfig(detectionAlertConfigId);

    String alertSchemes = alertConfig.getAlertSchemes();
    if (StringUtils.isEmpty(alertSchemes)) {
      alertSchemes = DEFAULT_ALERT_SCHEME;
    }
    String[] alertSchemeClasses = alertSchemes.split(",");
    Set<DetectionAlertScheme> detectionAlertSchemeSet = new HashSet<>();
    for (String alertSchemeClass : alertSchemeClasses) {
      Constructor<?> constructor = Class.forName(alertSchemeClass.trim())
          .getConstructor(DetectionAlertConfigDTO.class, TaskContext.class, DetectionAlertFilterResult.class);
      detectionAlertSchemeSet.add((DetectionAlertScheme) constructor.newInstance(alertConfig, taskContext, result));
    }
    return detectionAlertSchemeSet;
  }

  private DetectionAlertConfigDTO loadDetectionAlertConfig(long detectionAlertConfigId) {
    DetectionAlertConfigDTO detectionAlertConfig = this.alertConfigDAO.findById(detectionAlertConfigId);
    if (detectionAlertConfig == null) {
      throw new RuntimeException("Cannot find detection alert config id " + detectionAlertConfigId);
    }

    if (detectionAlertConfig.getProperties() == null) {
      LOG.warn(String.format("Detection alert %d contains no properties", detectionAlertConfigId));
    }
    return detectionAlertConfig;
  }
}
