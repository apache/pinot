package com.linkedin.thirdeye.detection.yaml;

import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.algorithm.BaselineAlgorithm;
import com.linkedin.thirdeye.detection.algorithm.BaselineRuleFilterWrapper;
import com.linkedin.thirdeye.detection.algorithm.DimensionWrapper;
import com.linkedin.thirdeye.detection.algorithm.LegacyAlertFilterWrapper;
import com.linkedin.thirdeye.detection.algorithm.LegacyMergeWrapper;
import com.linkedin.thirdeye.detection.algorithm.MergeWrapper;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;


public class CompositePipelineConfigTranslatorTest {

  private DAOTestBase testDAOProvider;
  private MetricConfigManager metricConfigDAO;
  private Long metricId;
  private Yaml yaml;
  Map<String, Object> yamlConfig;

  @BeforeMethod
  public void setUp() {
    this.testDAOProvider = DAOTestBase.getInstance();
    this.metricConfigDAO = DAORegistry.getInstance().getMetricConfigDAO();
    MetricConfigDTO metricConfig = new MetricConfigDTO();
    metricConfig.setAlias("alias");
    metricConfig.setName("test_metric");
    metricConfig.setDataset("test_dataset");
    this.metricId = this.metricConfigDAO.save(metricConfig);
    this.yaml = new Yaml();
    this.yamlConfig = (Map<String, Object>) this.yaml.load(this.getClass().getResourceAsStream("pipeline-config.yaml"));
  }

  @AfterMethod
  public void tearDown() {
    this.testDAOProvider.cleanup();
  }

  @Test
  public void testBuildDetectionProperties() {
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();
    Map<String, Object> properties = translator.buildDetectionProperties(this.yamlConfig);
    List<Map<String, Object>> nestedProperties = ConfigUtils.getList(properties.get("nested"));
    Map<String, Object> mergerProperties = nestedProperties.get(0);
    List<Map<String, Object>> mergerNestedProperties = ConfigUtils.getList(mergerProperties.get("nested"));
    Map<String, Object> algorithmPipelineProperties = mergerNestedProperties.get(0);
    Map<String, Object> ruleDetectionPipelineProperties = mergerNestedProperties.get(1);
    List<Map<String, Object>> ruleNestedProperties = ConfigUtils.getList(ruleDetectionPipelineProperties.get("nested"));
    Map<String, Object> baselineAlgorithmProperties = ruleNestedProperties.get(0);

    Assert.assertEquals(properties.get("siteWideImpactThreshold"), 0.05);
    Assert.assertEquals(properties.get("className"), BaselineRuleFilterWrapper.class.getName());
    Assert.assertEquals(nestedProperties.size(), 1);
    Assert.assertEquals(mergerProperties.get("className"), MergeWrapper.class.getName());
    Assert.assertEquals(mergerNestedProperties.size(), 2);
    Assert.assertEquals(algorithmPipelineProperties.get("anomalyFunctionClassName"),
        "com.linkedin.anomalydetection.function.RegressionGaussianScanFunction");
    Assert.assertEquals(algorithmPipelineProperties.get("legacyAlertFilterClassName"),
        "com.linkedin.filter.AlphaBetaLogisticAlertFilterTwoSide");
    Assert.assertEquals(algorithmPipelineProperties.get("className"), LegacyAlertFilterWrapper.class.getName());
    Assert.assertEquals(MapUtils.getMap(algorithmPipelineProperties, "specs").size(), 5);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("className"), DimensionWrapper.class.getName());
    Assert.assertEquals(ruleDetectionPipelineProperties.get("metricUrn"), "thirdeye:metric:" + this.metricId +":D1%3Dv1:D1%3Dv2:D2%3Dv3");
    Assert.assertEquals(ruleDetectionPipelineProperties.get("minContribution"), 0.05);
    Assert.assertEquals(ruleNestedProperties.size(), 1);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("dimensions"), Arrays.asList("D1", "D2"));
    Assert.assertEquals(baselineAlgorithmProperties.get("change"), 0.1);
    Assert.assertEquals(baselineAlgorithmProperties.get("className"), BaselineAlgorithm.class.getName());
  }

  @Test
  public void testBuildDetectionPropertiesRuleOnly() {
    this.yamlConfig.remove("algorithmDetection");
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();
    Map<String, Object> properties = translator.buildDetectionProperties(this.yamlConfig);
    List<Map<String, Object>> nestedProperties = ConfigUtils.getList(properties.get("nested"));
    Map<String, Object> mergerProperties = nestedProperties.get(0);
    List<Map<String, Object>> mergerNestedProperties = ConfigUtils.getList(mergerProperties.get("nested"));
    Map<String, Object> ruleDetectionPipelineProperties = mergerNestedProperties.get(0);
    List<Map<String, Object>> ruleNestedProperties = ConfigUtils.getList(ruleDetectionPipelineProperties.get("nested"));
    Map<String, Object> baselineAlgorithmProperties = ruleNestedProperties.get(0);

    Assert.assertEquals(properties.get("siteWideImpactThreshold"), 0.05);
    Assert.assertEquals(properties.get("className"), BaselineRuleFilterWrapper.class.getName());
    Assert.assertEquals(nestedProperties.size(), 1);
    Assert.assertEquals(mergerProperties.get("className"), MergeWrapper.class.getName());
    Assert.assertEquals(mergerNestedProperties.size(), 1);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("className"), DimensionWrapper.class.getName());
    Assert.assertEquals(ruleDetectionPipelineProperties.get("metricUrn"), "thirdeye:metric:" + this.metricId + ":D1%3Dv1:D1%3Dv2:D2%3Dv3");
    Assert.assertEquals(ruleDetectionPipelineProperties.get("minContribution"), 0.05);
    Assert.assertEquals(ruleNestedProperties.size(), 1);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("dimensions"), Arrays.asList("D1", "D2"));
    Assert.assertEquals(baselineAlgorithmProperties.get("change"), 0.1);
    Assert.assertEquals(baselineAlgorithmProperties.get("className"), BaselineAlgorithm.class.getName());
  }

  @Test
  public void testBuildDetectionPropertiesAlgorithmOnly() {
    this.yamlConfig.remove("ruleDetection");
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();

    Map<String, Object> properties = translator.buildDetectionProperties(this.yamlConfig);
    List<Map<String, Object>> nestedProperties = ConfigUtils.getList(properties.get("nested"));
    Map<String, Object> mergerProperties = nestedProperties.get(0);
    List<Map<String, Object>> mergerNestedProperties = ConfigUtils.getList(mergerProperties.get("nested"));
    Map<String, Object> algorithmPipelineProperties = mergerNestedProperties.get(0);

    Assert.assertEquals(properties.get("siteWideImpactThreshold"), 0.05);
    Assert.assertEquals(properties.get("className"), BaselineRuleFilterWrapper.class.getName());
    Assert.assertEquals(nestedProperties.size(), 1);
    Assert.assertEquals(mergerProperties.get("className"), MergeWrapper.class.getName());
    Assert.assertEquals(mergerNestedProperties.size(), 1);
    Assert.assertEquals(algorithmPipelineProperties.get("anomalyFunctionClassName"),
        "com.linkedin.anomalydetection.function.RegressionGaussianScanFunction");
    Assert.assertEquals(algorithmPipelineProperties.get("legacyAlertFilterClassName"),
        "com.linkedin.filter.AlphaBetaLogisticAlertFilterTwoSide");
    Assert.assertEquals(algorithmPipelineProperties.get("className"), LegacyAlertFilterWrapper.class.getName());
    Assert.assertEquals(MapUtils.getMap(algorithmPipelineProperties, "specs").size(), 5);
  }

  @Test
  public void testBuildDetectionPropertiesNoRuleFilter() {
    this.yamlConfig.remove("ruleFilter");
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();

    Map<String, Object> mergerProperties = translator.buildDetectionProperties(this.yamlConfig);
    List<Map<String, Object>> mergerNestedProperties = ConfigUtils.getList(mergerProperties.get("nested"));
    Map<String, Object> algorithmPipelineProperties = mergerNestedProperties.get(0);
    Map<String, Object> ruleDetectionPipelineProperties = mergerNestedProperties.get(1);
    List<Map<String, Object>> ruleNestedProperties = ConfigUtils.getList(ruleDetectionPipelineProperties.get("nested"));
    Map<String, Object> baselineAlgorithmProperties = ruleNestedProperties.get(0);

    Assert.assertEquals(mergerProperties.get("className"), MergeWrapper.class.getName());
    Assert.assertEquals(mergerNestedProperties.size(), 2);
    Assert.assertEquals(algorithmPipelineProperties.get("anomalyFunctionClassName"),
        "com.linkedin.anomalydetection.function.RegressionGaussianScanFunction");
    Assert.assertEquals(algorithmPipelineProperties.get("legacyAlertFilterClassName"),
        "com.linkedin.filter.AlphaBetaLogisticAlertFilterTwoSide");
    Assert.assertEquals(algorithmPipelineProperties.get("className"), LegacyAlertFilterWrapper.class.getName());
    Assert.assertEquals(MapUtils.getMap(algorithmPipelineProperties, "specs").size(), 5);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("className"), DimensionWrapper.class.getName());
    Assert.assertEquals(ruleDetectionPipelineProperties.get("metricUrn"), "thirdeye:metric:" + this.metricId + ":D1%3Dv1:D1%3Dv2:D2%3Dv3");
    Assert.assertEquals(ruleDetectionPipelineProperties.get("minContribution"), 0.05);
    Assert.assertEquals(ruleNestedProperties.size(), 1);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("dimensions"), Arrays.asList("D1", "D2"));
    Assert.assertEquals(baselineAlgorithmProperties.get("change"), 0.1);
    Assert.assertEquals(baselineAlgorithmProperties.get("className"), BaselineAlgorithm.class.getName());
  }

  @Test
  public void testBuildDetectionPropertiesNoAlgorithmFilter() {
    this.yamlConfig.remove("algorithmFilter");
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();

    Map<String, Object> properties = translator.buildDetectionProperties(this.yamlConfig);
    List<Map<String, Object>> nestedProperties = ConfigUtils.getList(properties.get("nested"));
    Map<String, Object> mergerProperties = nestedProperties.get(0);
    List<Map<String, Object>> mergerNestedProperties = ConfigUtils.getList(mergerProperties.get("nested"));
    Map<String, Object> algorithmPipelineProperties = mergerNestedProperties.get(0);
    Map<String, Object> ruleDetectionPipelineProperties = mergerNestedProperties.get(1);
    List<Map<String, Object>> ruleNestedProperties = ConfigUtils.getList(ruleDetectionPipelineProperties.get("nested"));
    Map<String, Object> baselineAlgorithmProperties = ruleNestedProperties.get(0);

    Assert.assertEquals(properties.get("siteWideImpactThreshold"), 0.05);
    Assert.assertEquals(properties.get("className"), BaselineRuleFilterWrapper.class.getName());
    Assert.assertEquals(nestedProperties.size(), 1);
    Assert.assertEquals(mergerProperties.get("className"), MergeWrapper.class.getName());
    Assert.assertEquals(mergerNestedProperties.size(), 2);
    Assert.assertEquals(algorithmPipelineProperties.get("anomalyFunctionClassName"),
        "com.linkedin.anomalydetection.function.RegressionGaussianScanFunction");
    Assert.assertEquals(algorithmPipelineProperties.get("className"), LegacyMergeWrapper.class.getName());
    Assert.assertEquals(MapUtils.getMap(algorithmPipelineProperties, "specs").size(), 4);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("className"), DimensionWrapper.class.getName());
    Assert.assertEquals(ruleDetectionPipelineProperties.get("metricUrn"), "thirdeye:metric:" + this.metricId + ":D1%3Dv1:D1%3Dv2:D2%3Dv3");
    Assert.assertEquals(ruleDetectionPipelineProperties.get("minContribution"), 0.05);
    Assert.assertEquals(ruleNestedProperties.size(), 1);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("dimensions"), Arrays.asList("D1", "D2"));
    Assert.assertEquals(baselineAlgorithmProperties.get("change"), 0.1);
    Assert.assertEquals(baselineAlgorithmProperties.get("className"), BaselineAlgorithm.class.getName());
  }


  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuildDetectionPipelineMissModuleType() {
    Map<String, Object> ruleDetectionConfig = MapUtils.getMap(this.yamlConfig, "ruleDetection");
    ruleDetectionConfig.remove("type");
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();

    Map<String, Object> properties = translator.buildDetectionProperties(this.yamlConfig);
    List<Map<String, Object>> nestedProperties = ConfigUtils.getList(properties.get("nested"));
    Map<String, Object> mergerProperties = nestedProperties.get(0);
    List<Map<String, Object>> mergerNestedProperties = ConfigUtils.getList(mergerProperties.get("nested"));
    Map<String, Object> algorithmPipelineProperties = mergerNestedProperties.get(0);
    Map<String, Object> ruleDetectionPipelineProperties = mergerNestedProperties.get(1);
    List<Map<String, Object>> ruleNestedProperties = ConfigUtils.getList(ruleDetectionPipelineProperties.get("nested"));
    Map<String, Object> baselineAlgorithmProperties = ruleNestedProperties.get(0);

    Assert.assertEquals(properties.get("siteWideImpactThreshold"), 0.05);
    Assert.assertEquals(properties.get("className"), BaselineRuleFilterWrapper.class.getName());
    Assert.assertEquals(nestedProperties.size(), 1);
    Assert.assertEquals(mergerProperties.get("className"), MergeWrapper.class.getName());
    Assert.assertEquals(mergerNestedProperties.size(), 2);
    Assert.assertEquals(algorithmPipelineProperties.get("anomalyFunctionClassName"),
        "com.linkedin.anomalydetection.function.RegressionGaussianScanFunction");
    Assert.assertEquals(algorithmPipelineProperties.get("legacyAlertFilterClassName"),
        "com.linkedin.filter.AlphaBetaLogisticAlertFilterTwoSide");
    Assert.assertEquals(MapUtils.getMap(algorithmPipelineProperties, "specs").size(), 5);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("className"), DimensionWrapper.class.getName());
    Assert.assertEquals(ruleDetectionPipelineProperties.get("metricUrn"), "thirdeye:metric:" + this.metricId + ":D1%3Dv1:D1%3Dv2:D2%3Dv3");
    Assert.assertEquals(ruleDetectionPipelineProperties.get("minContribution"), 0.05);
    Assert.assertEquals(ruleNestedProperties.size(), 1);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("dimensions"), Arrays.asList("D1", "D2"));
    Assert.assertEquals(baselineAlgorithmProperties.get("change"), 0.1);
    Assert.assertEquals(baselineAlgorithmProperties.get("className"), BaselineAlgorithm.class.getName());
  }

}
