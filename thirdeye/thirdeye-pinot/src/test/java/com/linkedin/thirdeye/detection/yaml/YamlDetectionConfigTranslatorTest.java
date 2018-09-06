package com.linkedin.thirdeye.detection.yaml;

import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class YamlDetectionConfigTranslatorTest {
  private MetricConfigManager metricDAO;

  @Test
  public void testGenerateDetectionConfig() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("className", "test.linkedin.thirdeye");
    YamlDetectionConfigTranslator translator = new MockYamlDetectionConfigTranslator();

    Map<String, Object> yamlConfigs = new HashMap<>();
    yamlConfigs.put("name", "testPipeline");
    yamlConfigs.put("metric", "a_daily_metric");
    yamlConfigs.put("dataset", "a_test_dataset");
    Map<String, Collection<String>> filters = new HashMap();
    filters.put("D1", Arrays.asList("v1", "v2"));
    filters.put("D2", Collections.singletonList("v3"));
    yamlConfigs.put("filters", filters);

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setName("a_daily_metric");
    metricConfigDTO.setDataset("a_test_dataset");
    metricConfigDTO.setAlias("alias");
    Long id = this.metricDAO.save(metricConfigDTO);

    DetectionConfigDTO detectionConfigDTO = translator.generateDetectionConfig(yamlConfigs);
    Assert.assertEquals(detectionConfigDTO.getName(), "testPipeline");
    Assert.assertEquals(detectionConfigDTO.getCron(), "0 0 14 * * ? *");
    Assert.assertEquals(detectionConfigDTO.getProperties().get("yamlConfigs"), yamlConfigs);
    Assert.assertEquals(detectionConfigDTO.getProperties().get("metricUrn"), "thirdeye:metric:"+id+":D1%3Dv1:D1%3Dv2:D2%3Dv3");
  }

  @BeforeMethod
  public void setUp() {
    DAOTestBase testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    this.metricDAO = daoRegistry.getMetricConfigDAO();
  }

}