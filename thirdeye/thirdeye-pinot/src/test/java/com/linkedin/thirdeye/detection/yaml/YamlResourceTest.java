package com.linkedin.thirdeye.detection.yaml;

import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;

import static org.testng.Assert.*;


public class YamlResourceTest {
  private DetectionConfigManager detectionConfigDAO;
  private DetectionAlertConfigManager detectionAlertConfigDAO;
  private MetricConfigManager metricDAO;

  private static final Yaml YAML_MAPPER = new Yaml();

  @Test
  public void testSetUpDetectionPipeline() {
    Map<String, Object> yamlConfigs = new HashMap<>();
    yamlConfigs.put("name", "testPipeline");
    yamlConfigs.put("metric", "testMetric");

  }

  @BeforeMethod
  public void setUp() {
    DAOTestBase testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    this.detectionAlertConfigDAO = daoRegistry.getDetectionAlertConfigManager();
    this.detectionConfigDAO = daoRegistry.getDetectionConfigManager();
  }
}
