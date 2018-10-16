package com.linkedin.thirdeye.detection.yaml;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.algorithm.BaselineAlgorithm;
import com.linkedin.thirdeye.detection.algorithm.BaselineFillingMergeWrapper;
import com.linkedin.thirdeye.detection.algorithm.ChildKeepingMergeWrapper;
import com.linkedin.thirdeye.detection.algorithm.DimensionWrapper;
import com.linkedin.thirdeye.detection.algorithm.LegacyMergeWrapper;
import com.linkedin.thirdeye.detection.algorithm.MergeWrapper;
import com.linkedin.thirdeye.detection.algorithm.stage.AnomalyDetectionStageWrapper;
import com.linkedin.thirdeye.detection.algorithm.stage.AnomalyFilterStageWrapper;
import com.linkedin.thirdeye.detection.algorithm.stage.BaselineRuleDetectionStage;
import com.linkedin.thirdeye.detection.algorithm.stage.BaselineRuleFilterStage;
import java.util.Arrays;
import java.util.Collections;
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
  public void testBuildDetectionPropertiesMultipleRules() {
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();
    Map<String, Object> properties = translator.buildDetectionProperties(this.yamlConfig);
    List<Map<String, Object>> nestedProperties = ConfigUtils.getList(properties.get("nested"));
    Map<String, Object> dimensionProperties = nestedProperties.get(0);
    List<Map<String, Object>> dimensionNestedProperties = ConfigUtils.getList(dimensionProperties.get("nested"));
    Map<String, Object> ruleFilterWrapperProperties1 = dimensionNestedProperties.get(0);
    List<Map<String, Object>> ruleMergeWrapperProperties1 =
        ConfigUtils.getList(ruleFilterWrapperProperties1.get("nested"));
    List<Map<String, Object>> baselineAlgorithmProperties1 =
        ConfigUtils.getList(ruleMergeWrapperProperties1.get(0).get("nested"));
    Map<String, Object> ruleFilterWrapperProperties2 = dimensionNestedProperties.get(1);
    List<Map<String, Object>> ruleMergeWrapperProperties2 =
        ConfigUtils.getList(ruleFilterWrapperProperties2.get("nested"));
    List<Map<String, Object>> baselineAlgorithmProperties2 =
        ConfigUtils.getList(ruleMergeWrapperProperties2.get(0).get("nested"));

    Assert.assertEquals(properties.get("className"), ChildKeepingMergeWrapper.class.getName());
    Assert.assertEquals(nestedProperties.size(), 1);
    Assert.assertEquals(dimensionProperties.get("className"), DimensionWrapper.class.getName());
    Assert.assertEquals(dimensionProperties.get("metricUrn"),
        "thirdeye:metric:" + this.metricId + ":D1%3Dv1:D1%3Dv2:D2%3Dv3");
    Assert.assertEquals(dimensionProperties.get("minContribution"), 0.05);
    Assert.assertEquals(dimensionProperties.get("dimensions"), Arrays.asList("D1", "D2"));
    Assert.assertEquals(nestedProperties.size(), 1);
    Assert.assertEquals(dimensionNestedProperties.size(), 2);
    Assert.assertEquals(ruleFilterWrapperProperties1.get("className"), AnomalyFilterStageWrapper.class.getName());
    Assert.assertEquals(ruleFilterWrapperProperties1.get("stageClassName"), BaselineRuleFilterStage.class.getName());
    Assert.assertEquals(ruleFilterWrapperProperties1.get("specs"), ImmutableMap.of("siteWideImpactThreshold", 0.1));
    Assert.assertEquals(ruleMergeWrapperProperties1.size(), 1);
    Assert.assertEquals(ruleMergeWrapperProperties1.get(0).get("className"),
        BaselineFillingMergeWrapper.class.getName());
    Assert.assertEquals(baselineAlgorithmProperties1.size(), 1);
    Assert.assertEquals(baselineAlgorithmProperties1.get(0).get("stageClassName"),
        BaselineRuleDetectionStage.class.getName());
    Assert.assertEquals(baselineAlgorithmProperties1.get(0).get("specs"), ImmutableMap.of("change", 0.3));
    Assert.assertEquals(ruleFilterWrapperProperties2.get("className"), AnomalyFilterStageWrapper.class.getName());
    Assert.assertEquals(ruleFilterWrapperProperties2.get("stageClassName"), BaselineRuleFilterStage.class.getName());
    Assert.assertEquals(ruleFilterWrapperProperties2.get("specs"), ImmutableMap.of("siteWideImpactThreshold", 0.2));
    Assert.assertEquals(ruleMergeWrapperProperties2.size(), 1);
    Assert.assertEquals(ruleMergeWrapperProperties2.get(0).get("className"),
        BaselineFillingMergeWrapper.class.getName());
    Assert.assertEquals(baselineAlgorithmProperties2.size(), 1);
    Assert.assertEquals(baselineAlgorithmProperties2.get(0).get("stageClassName"),
        BaselineRuleDetectionStage.class.getName());
    Assert.assertEquals(baselineAlgorithmProperties2.get(0).get("specs"), ImmutableMap.of("change", 0.2));
  }

  @Test
  public void testBuildDetectionPropertiesSingleRule() {
    this.yamlConfig.put("anomalyDetection", Collections.singletonList(
        ImmutableMap.of("detection", Collections.singletonList(ImmutableMap.of("type", "BASELINE", "change", 0.3)),
            "filter", Collections.singletonList(ImmutableMap.of("type", "BUSINESS_RULE_FILTER", "siteWideImpactThreshold", 0.1)))));
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();
    Map<String, Object> properties = translator.buildDetectionProperties(this.yamlConfig);
    List<Map<String, Object>> nestedProperties = ConfigUtils.getList(properties.get("nested"));
    Map<String, Object> dimensionProperties = nestedProperties.get(0);
    List<Map<String, Object>> dimensionNestedProperties = ConfigUtils.getList(dimensionProperties.get("nested"));
    Map<String, Object> ruleFilterWrapperProperties1 = dimensionNestedProperties.get(0);
    List<Map<String, Object>> ruleMergeWrapperProperties1 =
        ConfigUtils.getList(ruleFilterWrapperProperties1.get("nested"));
    List<Map<String, Object>> baselineAlgorithmProperties1 =
        ConfigUtils.getList(ruleMergeWrapperProperties1.get(0).get("nested"));

    Assert.assertEquals(properties.get("className"), ChildKeepingMergeWrapper.class.getName());
    Assert.assertEquals(nestedProperties.size(), 1);
    Assert.assertEquals(dimensionProperties.get("className"), DimensionWrapper.class.getName());
    Assert.assertEquals(dimensionProperties.get("metricUrn"),
        "thirdeye:metric:" + this.metricId + ":D1%3Dv1:D1%3Dv2:D2%3Dv3");
    Assert.assertEquals(dimensionProperties.get("minContribution"), 0.05);
    Assert.assertEquals(dimensionProperties.get("dimensions"), Arrays.asList("D1", "D2"));
    Assert.assertEquals(nestedProperties.size(), 1);
    Assert.assertEquals(dimensionNestedProperties.size(), 1);
    Assert.assertEquals(ruleFilterWrapperProperties1.get("className"), AnomalyFilterStageWrapper.class.getName());
    Assert.assertEquals(ruleFilterWrapperProperties1.get("stageClassName"), BaselineRuleFilterStage.class.getName());
    Assert.assertEquals(ruleFilterWrapperProperties1.get("specs"), ImmutableMap.of("siteWideImpactThreshold", 0.1));
    Assert.assertEquals(ruleMergeWrapperProperties1.size(), 1);
    Assert.assertEquals(ruleMergeWrapperProperties1.get(0).get("className"),
        BaselineFillingMergeWrapper.class.getName());
    Assert.assertEquals(baselineAlgorithmProperties1.size(), 1);
    Assert.assertEquals(baselineAlgorithmProperties1.get(0).get("stageClassName"),
        BaselineRuleDetectionStage.class.getName());
    Assert.assertEquals(baselineAlgorithmProperties1.get(0).get("specs"), ImmutableMap.of("change", 0.3));
  }

  @Test
  public void testBuildDetectionPropertiesNoFilter() {
    this.yamlConfig.put("anomalyDetection", Collections.singletonList(
        ImmutableMap.of("detection", Collections.singletonList(ImmutableMap.of("type", "BASELINE", "change", 0.3)))));
    CompositePipelineConfigTranslator translator = new CompositePipelineConfigTranslator();
    Map<String, Object> properties = translator.buildDetectionProperties(this.yamlConfig);
    List<Map<String, Object>> nestedProperties = ConfigUtils.getList(properties.get("nested"));
    Map<String, Object> dimensionProperties = nestedProperties.get(0);
    List<Map<String, Object>> dimensionNestedProperties = ConfigUtils.getList(dimensionProperties.get("nested"));
    List<Map<String, Object>> ruleMergeWrapperProperties1 = ConfigUtils.getList(dimensionNestedProperties.get(0).get("nested"));
    Map<String, Object> baselineAlgorithmProperties1 =
        ruleMergeWrapperProperties1.get(0);

    Assert.assertEquals(properties.get("className"), ChildKeepingMergeWrapper.class.getName());
    Assert.assertEquals(nestedProperties.size(), 1);
    Assert.assertEquals(dimensionProperties.get("className"), DimensionWrapper.class.getName());
    Assert.assertEquals(dimensionProperties.get("metricUrn"),
        "thirdeye:metric:" + this.metricId + ":D1%3Dv1:D1%3Dv2:D2%3Dv3");
    Assert.assertEquals(dimensionProperties.get("minContribution"), 0.05);
    Assert.assertEquals(dimensionProperties.get("dimensions"), Arrays.asList("D1", "D2"));
    Assert.assertEquals(nestedProperties.size(), 1);
    Assert.assertEquals(dimensionNestedProperties.get(0).get("className"), BaselineFillingMergeWrapper.class.getName());
    Assert.assertEquals(dimensionNestedProperties.size(), 1);
    Assert.assertEquals(ruleMergeWrapperProperties1.size(), 1);
    Assert.assertEquals(baselineAlgorithmProperties1.get("className"), AnomalyDetectionStageWrapper.class.getName());
    Assert.assertEquals(baselineAlgorithmProperties1.get("stageClassName"),
        BaselineRuleDetectionStage.class.getName());
    Assert.assertEquals(baselineAlgorithmProperties1.get("specs"), ImmutableMap.of("change", 0.3));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuildDetectionPipelineMissModuleType() {
    this.yamlConfig.put("anomalyDetection", Collections.singletonList(
        ImmutableMap.of("detection", Collections.singletonList(ImmutableMap.of("change", 0.3)))));
    YamlDetectionConfigTranslator translator = new CompositePipelineConfigTranslator();

    translator.generateDetectionConfig(this.yamlConfig);
  }
}
