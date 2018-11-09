package com.linkedin.thirdeye.detection.yaml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.MockDataProvider;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;


public class CompositePipelineConfigTranslatorTest {

  private Long metricId;
  private Yaml yaml;
  private Map<String, Object> yamlConfig;
  private DataProvider provider;
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @BeforeMethod
  public void setUp() {
    MetricConfigDTO metricConfig = new MetricConfigDTO();
    metricConfig.setAlias("alias");
    metricConfig.setName("test_metric");
    metricConfig.setDataset("test_dataset");
    this.metricId = 1L;
    metricConfig.setId(metricId);
    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset("test_dataset");
    datasetConfigDTO.setTimeUnit(TimeUnit.DAYS);
    datasetConfigDTO.setTimeDuration(1);
    this.yaml = new Yaml();
    this.provider = new MockDataProvider().setMetrics(Collections.singletonList(metricConfig)).setDatasets(Collections.singletonList(datasetConfigDTO));
  }


  @Test
  public void testBuildDetectionPropertiesMultipleRules() throws Exception {
    this.yamlConfig = (Map<String, Object>) this.yaml.load(this.getClass().getResourceAsStream("pipeline-config-1.yaml"));
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator(this.yamlConfig, this.provider);
    YamlTranslationResult result = translator.translateYaml();
    YamlTranslationResult expected = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("compositePipelineTranslatorTestResult-1.json"), YamlTranslationResult.class);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testBuildDetectionPropertiesNoFilter() throws Exception {
    this.yamlConfig = (Map<String, Object>) this.yaml.load(this.getClass().getResourceAsStream("pipeline-config-2.yaml"));
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator(this.yamlConfig, this.provider);
    YamlTranslationResult result = translator.translateYaml();
    YamlTranslationResult expected = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("compositePipelineTranslatorTestResult-2.json"), YamlTranslationResult.class);
    Assert.assertEquals(expected, result);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuildDetectionPipelineMissModuleType() {
    this.yamlConfig.put("anomalyDetection", Collections.singletonList(
        ImmutableMap.of("detection", Collections.singletonList(ImmutableMap.of("change", 0.3)))));
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator(this.yamlConfig, this.provider);

    translator.generateDetectionConfig();
  }
}
