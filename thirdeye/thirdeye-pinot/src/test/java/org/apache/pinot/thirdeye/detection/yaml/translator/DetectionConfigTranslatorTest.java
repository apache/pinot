package org.apache.pinot.thirdeye.detection.yaml.translator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.pinot.PinotThirdEyeDataSource;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.components.MockGrouper;
import org.apache.pinot.thirdeye.detection.components.RuleBaselineProvider;
import org.apache.pinot.thirdeye.detection.components.ThresholdRuleAnomalyFilter;
import org.apache.pinot.thirdeye.detection.components.ThresholdRuleDetector;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.detection.validators.ConfigValidationException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;


public class DetectionConfigTranslatorTest {

  private Long metricId;
  private Yaml yaml;
  private Map<String, Object> yamlConfig;
  private DataProvider provider;
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private DAOTestBase testDAOProvider;
  private DAORegistry daoRegistry;

  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    daoRegistry = DAORegistry.getInstance();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @BeforeMethod
  public void setUp() {
    MetricConfigDTO metricConfig = new MetricConfigDTO();
    metricConfig.setAlias("alias");
    metricConfig.setName("test_metric");
    metricConfig.setDataset("test_dataset");
    this.metricId = 1L;
    metricConfig.setId(metricId);
    daoRegistry.getMetricConfigDAO().save(metricConfig);

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset("test_dataset");
    datasetConfigDTO.setTimeUnit(TimeUnit.DAYS);
    datasetConfigDTO.setTimeDuration(1);
    datasetConfigDTO.setDataSource(PinotThirdEyeDataSource.class.getSimpleName());
    daoRegistry.getDatasetConfigDAO().save(datasetConfigDTO);

    this.yaml = new Yaml();
    DetectionRegistry.registerComponent(ThresholdRuleDetector.class.getName(), "THRESHOLD");
    DetectionRegistry.registerComponent(ThresholdRuleAnomalyFilter.class.getName(), "THRESHOLD_RULE_FILTER");
    DetectionRegistry.registerComponent(RuleBaselineProvider.class.getName(), "RULE_BASELINE");
    DetectionRegistry.registerComponent(MockGrouper.class.getName(), "MOCK_GROUPER");
    this.provider = new MockDataProvider().setMetrics(Collections.singletonList(metricConfig)).setDatasets(Collections.singletonList(datasetConfigDTO));
  }

  @Test
  public void testBuildPropertiesFull() throws Exception {
    String yamlConfig = IOUtils.toString(this.getClass().getResourceAsStream("pipeline-config-1.yaml"), "UTF-8");
    DetectionConfigTranslator translator = new DetectionConfigTranslator(yamlConfig, this.provider);
    DetectionConfigDTO result = translator.translate();
    YamlTranslationResult expected = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("compositePipelineTranslatorTestResult-1.json"), YamlTranslationResult.class);
    Assert.assertEquals(result.getProperties(), expected.getProperties());
    Assert.assertTrue(result.isDataAvailabilitySchedule());
  }

  @Test
  public void testBuildDetectionPropertiesNoFilter() throws Exception {
    String yamlConfig = IOUtils.toString(this.getClass().getResourceAsStream("pipeline-config-2.yaml"), "UTF-8");
    DetectionConfigTranslator translator = new DetectionConfigTranslator(yamlConfig, this.provider);
    DetectionConfigDTO result = translator.translate();
    YamlTranslationResult expected = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("compositePipelineTranslatorTestResult-2.json"), YamlTranslationResult.class);
    Assert.assertEquals(result.getProperties(), expected.getProperties());
    Assert.assertTrue(result.isDataAvailabilitySchedule());
  }

  @Test(expectedExceptions = ConfigValidationException.class)
  public void testBuildDetectionPipelineMissModuleType() throws Exception {
    this.yamlConfig = (Map<String, Object>) this.yaml.load(this.getClass().getResourceAsStream("pipeline-config-1.yaml"));
    this.yamlConfig.put("rules", Collections.singletonList(
        ImmutableMap.of("name", "rule2","detection", Collections.singletonList(ImmutableMap.of("change", 0.3)))));
    DetectionConfigTranslator translator = new DetectionConfigTranslator(yaml.dump(this.yamlConfig), this.provider);
    translator.translate();
  }

  @Test(expectedExceptions = ConfigValidationException.class)
  public void testMultipleGrouperLogic() throws Exception {
    String yamlConfig = IOUtils.toString(this.getClass().getResourceAsStream("pipeline-config-3.yaml"), "UTF-8");
    DetectionConfigTranslator translator = new DetectionConfigTranslator(yamlConfig, this.provider);
    translator.translate();
  }

  @Test
  public void testBuildEntityTranslationWithOneMetric() throws Exception {
    String yamlConfig = IOUtils.toString(this.getClass().getResourceAsStream("pipeline-config-4.yaml"), "UTF-8");
    DetectionConfigTranslator translator = new DetectionConfigTranslator(yamlConfig, this.provider);
    DetectionConfigDTO result = translator.translate();
    YamlTranslationResult expected = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("compositePipelineTranslatorTestResult-4.json"), YamlTranslationResult.class);
    Assert.assertEquals(result.getProperties(), expected.getProperties());
    Assert.assertFalse(result.isDataAvailabilitySchedule());
  }

  @Test
  public void testBuildEntityTranslationWithMultipleMetrics() throws Exception {
    String yamlConfig = IOUtils.toString(this.getClass().getResourceAsStream("pipeline-config-5.yaml"), "UTF-8");
    DetectionConfigTranslator translator = new DetectionConfigTranslator(yamlConfig, this.provider);
    DetectionConfigDTO result = translator.translate();
    YamlTranslationResult expected = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("compositePipelineTranslatorTestResult-5.json"), YamlTranslationResult.class);
    Assert.assertEquals(result.getProperties(), expected.getProperties());
    Assert.assertFalse(result.isDataAvailabilitySchedule());
  }
}
