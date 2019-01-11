package org.apache.pinot.thirdeye.detection.yaml;

import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class YamlDetectionConfigTranslatorTest {
  private MetricConfigManager metricDAO;
  private DAOTestBase testDAOProvider;

  @Test
  public void testGenerateDetectionConfig() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("className", "test.linkedin.thirdeye");

    Map<String, Object> yamlConfigs = new HashMap<>();
    yamlConfigs.put("detectionName", "testPipeline");
    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setName("a_daily_metric");
    metricConfigDTO.setDataset("a_test_dataset");
    metricConfigDTO.setAlias("alias");
    this.metricDAO.save(metricConfigDTO);

    YamlDetectionConfigTranslator translator = new MockYamlDetectionConfigTranslator(yamlConfigs, new MockDataProvider());
    DetectionConfigDTO detectionConfigDTO = translator.generateDetectionConfig();
    Assert.assertEquals(detectionConfigDTO.getName(), "testPipeline");
    Assert.assertEquals(detectionConfigDTO.getCron(), "0 0 14 * * ? *");
    Assert.assertEquals(detectionConfigDTO.getProperties().get("yamlConfigs"), yamlConfigs);
  }

  @BeforeMethod
  public void setUp() {
    this.testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    this.metricDAO = daoRegistry.getMetricConfigDAO();
  }

  @AfterMethod
  public void tearDown() {
    this.testDAOProvider.cleanup();
  }
}
