package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.algorithm.LegacyAlertFilterWrapper;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
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
  private static final String PROP_CLASS_NAME = "className";

  private MetricConfigManager metricConfigDAO;
  private final AnomalyFunctionFactory anomalyFunctionFactory;

  /**
   * Instantiates a new Legacy Anomaly function translator.
   */
  public LegacyAnomalyFunctionTranslator(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  /**
   * Translate a legacy anomaly function to a new detection config.
   */
  public DetectionConfigDTO translate(AnomalyFunctionDTO anomalyFunctionDTO) throws Exception {
    if (anomalyFunctionDTO.getMetricId() <= 0) {
      MetricConfigDTO metricDTO = this.metricConfigDAO.findByMetricAndDataset(anomalyFunctionDTO.getMetric(), anomalyFunctionDTO.getCollection());
      if (metricDTO == null) {
        LOGGER.error("Cannot find metric {} for anomaly function {}", anomalyFunctionDTO.getMetric(), anomalyFunctionDTO.getFunctionName());
        throw new Exception("Cannot find metric");
      }
      anomalyFunctionDTO.setMetricId(metricDTO.getId());
    }

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, LegacyAlertFilterWrapper.class);
    properties.put("anomalyFunctionClassName", anomalyFunctionFactory.getClassNameForFunctionType(anomalyFunctionDTO.getType()));
    properties.put("specs", anomalyFunctionDTO);
    properties.put("alertFilterLookBack", "1d");

    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setName(anomalyFunctionDTO.getFunctionName());
    config.setCron(anomalyFunctionDTO.getCron());
    config.setProperties(properties);
    config.setActive(false);

    return config;
  }
}
