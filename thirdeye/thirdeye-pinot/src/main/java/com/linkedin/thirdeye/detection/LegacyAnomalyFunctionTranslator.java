package com.linkedin.thirdeye.detection;

import com.google.common.base.Strings;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * The Legacy Anomaly function translator.
 */
public class LegacyAnomalyFunctionTranslator {
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private DetectionConfigManager detectionConfigDAO;
  private final AnomalyFunctionFactory anomalyFunctionFactory;

  /**
   * Instantiates a new Legacy Anomaly function translator.
   */
  public LegacyAnomalyFunctionTranslator(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.detectionConfigDAO = DAO_REGISTRY.getDetectionConfigManager();
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  /**
   * Translate legacy anomaly function to generate a new detection config and persist it into the database.
   */
  public void translate(AnomalyFunctionDTO anomalyFunctionDTO) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("specs", anomalyFunctionDTO);
    properties.put("className", "com.linkedin.thirdeye.detection.algorithm.LegacyMergeWrapper");
    properties.put("anomalyFunctionClassName",
        anomalyFunctionFactory.getClassNameForFunctionType(anomalyFunctionDTO.getType()));

    String filters = anomalyFunctionDTO.getFilters();
    long metricId = anomalyFunctionDTO.getMetricId();
    MetricEntity me = MetricEntity.fromMetric(1.0, metricId).withFilters(ThirdEyeUtils.getFilterSet(filters));
    String metricUrn = me.getUrn();

    Map<String, Object> legacyAnomalyFunctionProperties = new HashMap<>();
    legacyAnomalyFunctionProperties.put("className",
        "com.linkedin.thirdeye.detection.algorithm.LegacyAnomalyFunctionAlgorithm");
    Map<String, Object> nestedProperties = new HashMap<>();

    if (!Strings.isNullOrEmpty(anomalyFunctionDTO.getExploreDimensions())) {
      // if anomaly function does dimension exploration, then plug in the dimension wrapper
      nestedProperties.put("className", "com.linkedin.thirdeye.detection.algorithm.LegacyDimensionWrapper");
      nestedProperties.put("metricUrn", metricUrn);
      nestedProperties.put("nested", Collections.singletonList(legacyAnomalyFunctionProperties));
      properties.put("nested", Collections.singletonList(nestedProperties));
    } else {
      legacyAnomalyFunctionProperties.put("metricUrn", metricUrn);
      properties.put("nested", Collections.singletonList(legacyAnomalyFunctionProperties));
    }

    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setName(anomalyFunctionDTO.getFunctionName());
    config.setCron(anomalyFunctionDTO.getCron());
    config.setProperties(properties);
    this.detectionConfigDAO.save(config);
  }
}
