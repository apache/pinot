package com.linkedin.thirdeye.detection;

import com.google.common.base.Strings;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.algorithm.LegacyAlertFilterWrapper;
import com.linkedin.thirdeye.detection.algorithm.LegacyAnomalyFunctionAlgorithm;
import com.linkedin.thirdeye.detection.algorithm.LegacyMergeWrapper;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Legacy Anomaly function translator.
 */
public class LegacyAnomalyFunctionTranslator {
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final Logger LOGGER = LoggerFactory.getLogger(LegacyAnomalyFunctionTranslator.class);

  private static final String PROP_LEGACY_ALERT_FILTER_CLASS_NAME = "legacyAlertFilterClassName";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_NESTED = "nested";

  private MetricConfigManager metricConfigDAO;
  private final AnomalyFunctionFactory anomalyFunctionFactory;
  private final AlertFilterFactory alertFilterFactory;

  /**
   * Instantiates a new Legacy Anomaly function translator.
   */
  public LegacyAnomalyFunctionTranslator(AnomalyFunctionFactory anomalyFunctionFactory, AlertFilterFactory alertFilterFactory) {
    this.metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    this.anomalyFunctionFactory = anomalyFunctionFactory;
    this.alertFilterFactory = alertFilterFactory;
  }

  /**
   * Translate a legacy anomaly function to a new detection config.
   */
  public DetectionConfigDTO translate(AnomalyFunctionDTO anomalyFunctionDTO) throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, LegacyMergeWrapper.class);
    properties.put("anomalyFunctionClassName", anomalyFunctionFactory.getClassNameForFunctionType(anomalyFunctionDTO.getType()));

    String filters = anomalyFunctionDTO.getFilters();
    MetricConfigDTO metricDTO = this.metricConfigDAO.findByMetricAndDataset(anomalyFunctionDTO.getMetric(), anomalyFunctionDTO.getCollection());
    if (metricDTO == null) {
      LOGGER.error("Cannot find metric {} for anomaly function {}", anomalyFunctionDTO.getMetric(), anomalyFunctionDTO.getFunctionName());
      throw new Exception("Cannot find metric");
    }
    anomalyFunctionDTO.setMetricId(metricDTO.getId());

    MetricEntity me = MetricEntity.fromMetric(1.0, metricDTO.getId()).withFilters(ThirdEyeUtils.getFilterSet(filters));
    String metricUrn = me.getUrn();

    Map<String, Object> legacyAnomalyFunctionProperties = new HashMap<>();
    legacyAnomalyFunctionProperties.put(PROP_CLASS_NAME, LegacyAnomalyFunctionAlgorithm.class);
    Map<String, Object> nestedProperties = new HashMap<>();

    if (!Strings.isNullOrEmpty(anomalyFunctionDTO.getExploreDimensions())) {
      // if anomaly function does dimension exploration, then plug in the dimension wrapper
      nestedProperties.put(PROP_CLASS_NAME, LegacyAnomalyFunctionAlgorithm.class);
      nestedProperties.put(PROP_METRIC_URN, metricUrn);
      nestedProperties.put(PROP_NESTED, Collections.singletonList(legacyAnomalyFunctionProperties));
      properties.put(PROP_NESTED, Collections.singletonList(nestedProperties));
    } else {
      legacyAnomalyFunctionProperties.put(PROP_METRIC_URN, metricUrn);
      properties.put(PROP_NESTED, Collections.singletonList(legacyAnomalyFunctionProperties));
    }

    // plug in alert filter if exists
    Map<String, String> alertFilter = anomalyFunctionDTO.getAlertFilter();
    if (alertFilter != null) {
      Map<String, Object> alertFilterProp = new HashMap<>();
      alertFilterProp.put(PROP_CLASS_NAME, LegacyAlertFilterWrapper.class);
      String alertFilterType = alertFilter.get("type");

      if (alertFilterType != null) {
        String alertFilterClassName = this.alertFilterFactory.getClassNameForAlertFilterType(alertFilterType.toUpperCase());
        if (alertFilterClassName != null) {
          alertFilterProp.put(PROP_LEGACY_ALERT_FILTER_CLASS_NAME, alertFilterClassName);
        }
      }

      alertFilterProp.put(PROP_NESTED, Collections.singletonList(properties));
      properties = alertFilterProp;
    }

    properties.put("specs", anomalyFunctionDTO);
    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setName(anomalyFunctionDTO.getFunctionName());
    config.setCron(anomalyFunctionDTO.getCron());
    config.setProperties(properties);
    config.setActive(true);
    return config;
  }
}
