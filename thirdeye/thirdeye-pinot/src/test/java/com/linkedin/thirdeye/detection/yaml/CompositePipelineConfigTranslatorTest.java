package com.linkedin.thirdeye.detection.yaml;

import com.google.common.collect.ImmutableMap;
import com.linkedin.anomalydetection.function.RegressionGaussianScanFunction;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.algorithm.BaselineAlgorithm;
import com.linkedin.thirdeye.detection.algorithm.BaselineRuleFilterWrapper;
import com.linkedin.thirdeye.detection.algorithm.DimensionWrapper;
import com.linkedin.thirdeye.detection.algorithm.LegacyAlertFilterWrapper;
import com.linkedin.thirdeye.detection.algorithm.LegacyMergeWrapper;
import com.linkedin.thirdeye.detection.algorithm.MergeWrapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CompositePipelineConfigTranslatorTest {

  @BeforeClass
  public void setUp() {
    DAOTestBase testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
  }

  @Test
  public void testBuildDetectionProperties() {
    Map<String, Object> yamlConfig = new HashMap<>();
    yamlConfig.put("name", "testPipeline");
    yamlConfig.put("dimensionExploration", ImmutableMap.of("dimensions", Arrays.asList("D1", "D2")));
    yamlConfig.put("dimensionFilter", ImmutableMap.of("minContribution", 0.05));
    yamlConfig.put("ruleDetection", ImmutableMap.of("type", "BASELINE", "change", 0.1));
    yamlConfig.put("ruleFilter", ImmutableMap.of("type", "BUSINESS_RULE_FILTER", "siteWideImpactThreshold", 0.05));
    yamlConfig.put("algorithmDetection",
        ImmutableMap.of("type", "REGRESSION_GAUSSIAN_SCAN", "bucketSize", 1, "bucketUnit", "HOURS", "windowDelay", 0));
    yamlConfig.put("algorithmFilter", ImmutableMap.of("type", "alpha_beta_logistic_two_side", "pattern", "UP,DOWN"));
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();
    String metricUrn = "thirdeye:metric:12345";

    Map<String, Object> properties = translator.buildDetectionProperties(metricUrn, yamlConfig);
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
        RegressionGaussianScanFunction.class.getName());
    Assert.assertEquals(algorithmPipelineProperties.get("legacyAlertFilterClassName"),
        "com.linkedin.filter.AlphaBetaLogisticAlertFilterTwoSide");
    Assert.assertEquals(algorithmPipelineProperties.get("className"), LegacyAlertFilterWrapper.class.getName());
    Assert.assertEquals(MapUtils.getMap(algorithmPipelineProperties, "specs").size(), 5);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("className"), DimensionWrapper.class.getName());
    Assert.assertEquals(ruleDetectionPipelineProperties.get("metricUrn"), metricUrn);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("minContribution"), 0.05);
    Assert.assertEquals(ruleNestedProperties.size(), 1);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("dimensions"), Arrays.asList("D1", "D2"));
    Assert.assertEquals(baselineAlgorithmProperties.get("change"), 0.1);
    Assert.assertEquals(baselineAlgorithmProperties.get("className"), BaselineAlgorithm.class.getName());
  }

  @Test
  public void testBuildDetectionPropertiesRuleOnly() {
    Map<String, Object> yamlConfig = new HashMap<>();
    yamlConfig.put("name", "testPipeline");
    yamlConfig.put("dimensionExploration", ImmutableMap.of("dimensions", Arrays.asList("D1", "D2")));
    yamlConfig.put("dimensionFilter", ImmutableMap.of("minContribution", 0.05));
    yamlConfig.put("ruleDetection", ImmutableMap.of("type", "BASELINE", "change", 0.1));
    yamlConfig.put("ruleFilter", ImmutableMap.of("type", "BUSINESS_RULE_FILTER", "siteWideImpactThreshold", 0.05));
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();
    String metricUrn = "thirdeye:metric:12345";

    Map<String, Object> properties = translator.buildDetectionProperties(metricUrn, yamlConfig);
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
    Assert.assertEquals(ruleDetectionPipelineProperties.get("metricUrn"), metricUrn);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("minContribution"), 0.05);
    Assert.assertEquals(ruleNestedProperties.size(), 1);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("dimensions"), Arrays.asList("D1", "D2"));
    Assert.assertEquals(baselineAlgorithmProperties.get("change"), 0.1);
    Assert.assertEquals(baselineAlgorithmProperties.get("className"), BaselineAlgorithm.class.getName());
  }

  @Test
  public void testBuildDetectionPropertiesAlgorithmOnly() {
    Map<String, Object> yamlConfig = new HashMap<>();
    yamlConfig.put("name", "testPipeline");
    yamlConfig.put("dimensionExploration", ImmutableMap.of("dimensions", Arrays.asList("D1", "D2")));
    yamlConfig.put("dimensionFilter", ImmutableMap.of("minContribution", 0.05));
    yamlConfig.put("algorithmDetection",
        ImmutableMap.of("type", "REGRESSION_GAUSSIAN_SCAN", "bucketSize", 1, "bucketUnit", "HOURS", "windowDelay", 0));
    yamlConfig.put("algorithmFilter", ImmutableMap.of("type", "alpha_beta_logistic_two_side", "pattern", "UP,DOWN"));
    yamlConfig.put("ruleFilter", ImmutableMap.of("type", "BUSINESS_RULE_FILTER", "siteWideImpactThreshold", 0.05));

    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();
    String metricUrn = "thirdeye:metric:12345";

    Map<String, Object> properties = translator.buildDetectionProperties(metricUrn, yamlConfig);
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
        RegressionGaussianScanFunction.class.getName());
    Assert.assertEquals(algorithmPipelineProperties.get("legacyAlertFilterClassName"),
        "com.linkedin.filter.AlphaBetaLogisticAlertFilterTwoSide");
    Assert.assertEquals(algorithmPipelineProperties.get("className"), LegacyAlertFilterWrapper.class.getName());
    Assert.assertEquals(MapUtils.getMap(algorithmPipelineProperties, "specs").size(), 5);
  }

  @Test
  public void testBuildDetectionPropertiesNoRuleFilter() {
    Map<String, Object> yamlConfig = new HashMap<>();
    yamlConfig.put("name", "testPipeline");
    yamlConfig.put("dimensionExploration", ImmutableMap.of("dimensions", Arrays.asList("D1", "D2")));
    yamlConfig.put("dimensionFilter", ImmutableMap.of("minContribution", 0.05));
    yamlConfig.put("ruleDetection", ImmutableMap.of("type", "BASELINE", "change", 0.1));
    yamlConfig.put("algorithmDetection",
        ImmutableMap.of("type", "REGRESSION_GAUSSIAN_SCAN", "bucketSize", 1, "bucketUnit", "HOURS", "windowDelay", 0));
    yamlConfig.put("algorithmFilter", ImmutableMap.of("type", "alpha_beta_logistic_two_side", "pattern", "UP,DOWN"));
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();
    String metricUrn = "thirdeye:metric:12345";

    Map<String, Object> mergerProperties = translator.buildDetectionProperties(metricUrn, yamlConfig);
    List<Map<String, Object>> mergerNestedProperties = ConfigUtils.getList(mergerProperties.get("nested"));
    Map<String, Object> algorithmPipelineProperties = mergerNestedProperties.get(0);
    Map<String, Object> ruleDetectionPipelineProperties = mergerNestedProperties.get(1);
    List<Map<String, Object>> ruleNestedProperties = ConfigUtils.getList(ruleDetectionPipelineProperties.get("nested"));
    Map<String, Object> baselineAlgorithmProperties = ruleNestedProperties.get(0);

    Assert.assertEquals(mergerProperties.get("className"), MergeWrapper.class.getName());
    Assert.assertEquals(mergerNestedProperties.size(), 2);
    Assert.assertEquals(algorithmPipelineProperties.get("anomalyFunctionClassName"),
        RegressionGaussianScanFunction.class.getName());
    Assert.assertEquals(algorithmPipelineProperties.get("legacyAlertFilterClassName"),
        "com.linkedin.filter.AlphaBetaLogisticAlertFilterTwoSide");
    Assert.assertEquals(algorithmPipelineProperties.get("className"), LegacyAlertFilterWrapper.class.getName());
    Assert.assertEquals(MapUtils.getMap(algorithmPipelineProperties, "specs").size(), 5);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("className"), DimensionWrapper.class.getName());
    Assert.assertEquals(ruleDetectionPipelineProperties.get("metricUrn"), metricUrn);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("minContribution"), 0.05);
    Assert.assertEquals(ruleNestedProperties.size(), 1);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("dimensions"), Arrays.asList("D1", "D2"));
    Assert.assertEquals(baselineAlgorithmProperties.get("change"), 0.1);
    Assert.assertEquals(baselineAlgorithmProperties.get("className"), BaselineAlgorithm.class.getName());
  }

  @Test
  public void testBuildDetectionPropertiesNoAlgorithmFilter() {
    Map<String, Object> yamlConfig = new HashMap<>();
    yamlConfig.put("name", "testPipeline");
    yamlConfig.put("dimensionExploration", ImmutableMap.of("dimensions", Arrays.asList("D1", "D2")));
    yamlConfig.put("dimensionFilter", ImmutableMap.of("minContribution", 0.05));
    yamlConfig.put("ruleDetection", ImmutableMap.of("type", "BASELINE", "change", 0.1));
    yamlConfig.put("ruleFilter", ImmutableMap.of("type", "BUSINESS_RULE_FILTER", "siteWideImpactThreshold", 0.05));
    yamlConfig.put("algorithmDetection",
        ImmutableMap.of("type", "REGRESSION_GAUSSIAN_SCAN", "bucketSize", 1, "bucketUnit", "HOURS", "windowDelay", 0));
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();
    String metricUrn = "thirdeye:metric:12345";

    Map<String, Object> properties = translator.buildDetectionProperties(metricUrn, yamlConfig);
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
        RegressionGaussianScanFunction.class.getName());
    Assert.assertEquals(algorithmPipelineProperties.get("className"), LegacyMergeWrapper.class.getName());
    Assert.assertEquals(MapUtils.getMap(algorithmPipelineProperties, "specs").size(), 4);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("className"), DimensionWrapper.class.getName());
    Assert.assertEquals(ruleDetectionPipelineProperties.get("metricUrn"), metricUrn);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("minContribution"), 0.05);
    Assert.assertEquals(ruleNestedProperties.size(), 1);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("dimensions"), Arrays.asList("D1", "D2"));
    Assert.assertEquals(baselineAlgorithmProperties.get("change"), 0.1);
    Assert.assertEquals(baselineAlgorithmProperties.get("className"), BaselineAlgorithm.class.getName());
  }


  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuildDetectionPipelineTypeMiss() {
    Map<String, Object> yamlConfig = new HashMap<>();
    yamlConfig.put("name", "testPipeline");
    yamlConfig.put("dimensionExploration", ImmutableMap.of("dimensions", Arrays.asList("D1", "D2")));
    yamlConfig.put("dimensionFilter", ImmutableMap.of("minContribution", 0.05));
    yamlConfig.put("ruleDetection", ImmutableMap.of("change", 0.1));
    yamlConfig.put("ruleFilter", ImmutableMap.of("type", "BUSINESS_RULE_FILTER", "siteWideImpactThreshold", 0.05));
    yamlConfig.put("algorithmDetection",
        ImmutableMap.of("type", "REGRESSION_GAUSSIAN_SCAN", "bucketSize", 1, "bucketUnit", "HOURS", "windowDelay", 0));
    yamlConfig.put("algorithmFilter", ImmutableMap.of("type", "alpha_beta_logistic_two_side", "pattern", "UP,DOWN"));
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();
    String metricUrn = "thirdeye:metric:12345";

    Map<String, Object> properties = translator.buildDetectionProperties(metricUrn, yamlConfig);
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
        RegressionGaussianScanFunction.class.getName());
    Assert.assertEquals(algorithmPipelineProperties.get("legacyAlertFilterClassName"),
        "com.linkedin.filter.AlphaBetaLogisticAlertFilterTwoSide");
    Assert.assertEquals(MapUtils.getMap(algorithmPipelineProperties, "specs").size(), 5);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("className"), DimensionWrapper.class.getName());
    Assert.assertEquals(ruleDetectionPipelineProperties.get("metricUrn"), metricUrn);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("minContribution"), 0.05);
    Assert.assertEquals(ruleNestedProperties.size(), 1);
    Assert.assertEquals(ruleDetectionPipelineProperties.get("dimensions"), Arrays.asList("D1", "D2"));
    Assert.assertEquals(baselineAlgorithmProperties.get("change"), 0.1);
    Assert.assertEquals(baselineAlgorithmProperties.get("className"), BaselineAlgorithm.class.getName());
  }

}