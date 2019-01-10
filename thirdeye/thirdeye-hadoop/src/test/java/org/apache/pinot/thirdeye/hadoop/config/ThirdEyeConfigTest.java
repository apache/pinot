/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.hadoop.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.hadoop.config.MetricType;
import com.linkedin.thirdeye.hadoop.config.TopKDimensionToMetricsSpec;
import com.linkedin.thirdeye.hadoop.config.TopkWhitelistSpec;

public class ThirdEyeConfigTest {

  private Properties props;
  private ThirdEyeConfig thirdeyeConfig;
  private ThirdEyeConfig config;

  @BeforeClass
  public void setup() {
    props = new Properties();
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TABLE_NAME.toString(), "collection");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_NAMES.toString(), "d1,d2,d3");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_TYPES.toString(), "STRING,LONG,STRING");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_NAMES.toString(), "m1,m2,m3");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_TYPES.toString(), "LONG,FLOAT,INT");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_NAME.toString(), "t1");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_TYPE.toString(), "DAYS");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_SIZE.toString(), "10");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_SPLIT_THRESHOLD.toString(), "1000");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_SPLIT_ORDER.toString(), "d1,d2,d3");

    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_THRESHOLD_METRIC_NAMES.toString(), "m1,m3");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_METRIC_THRESHOLD_VALUES.toString(), "0.02,0.1");

    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_DIMENSION_NAMES.toString(), "d2,d3");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_METRICS.toString() + ".d2", "m1,m2");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_KVALUES.toString() + ".d2", "20,30");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_METRICS.toString() + ".d3", "m1");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_KVALUES.toString() + ".d3", "50");

    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_WHITELIST_DIMENSION_NAMES.toString(), "d1,d2");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_WHITELIST_DIMENSION.toString() + ".d1", "x,y");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_WHITELIST_DIMENSION.toString() + ".d2", "500");

    thirdeyeConfig = ThirdEyeConfig.fromProperties(props);

  }



  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testTableNameConfig() throws IllegalArgumentException {
    Assert.assertEquals("collection", thirdeyeConfig.getCollection(), "Collection name not correctly set");
    try {
      props.remove(ThirdEyeConfigProperties.THIRDEYE_TABLE_NAME.toString());
      config = ThirdEyeConfig.fromProperties(props);
    } finally {
      props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TABLE_NAME.toString(), "collection");
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDimensionsConfig() throws IllegalArgumentException {
    Assert.assertEquals(3, thirdeyeConfig.getDimensionNames().size(), "Incorrect number of dimensions");
    Assert.assertEquals(new String[]{"d1", "d2", "d3"}, thirdeyeConfig.getDimensionNames().toArray(), "Incorrect dimensions");

    try {
      props.remove(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_NAMES.toString());
      config = ThirdEyeConfig.fromProperties(props);
    } finally {
      props.setProperty(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_NAMES.toString(), "d1,d2,d3");
    }
  }

  @Test
  public void testMetricsConfig() throws IllegalArgumentException {
    boolean failed = false;
    Assert.assertEquals(3, thirdeyeConfig.getMetricNames().size(), "Incorrect number of metrics");
    Assert.assertEquals(3, thirdeyeConfig.getMetrics().size(), "Incorrect number of metric specs");
    Assert.assertEquals(new String[]{"m1", "m2", "m3"}, thirdeyeConfig.getMetricNames().toArray(), "Incorrect metrics");
    MetricType[] actualMetricTypes = new MetricType[3];
    for (int i = 0; i < 3; i++) {
      actualMetricTypes[i] = thirdeyeConfig.getMetrics().get(i).getType();
    }
    Assert.assertEquals(actualMetricTypes, new MetricType[]{MetricType.LONG, MetricType.FLOAT, MetricType.INT}, "Incorrect metric specs");

    try {
      props.remove(ThirdEyeConfigProperties.THIRDEYE_METRIC_NAMES.toString());
      config = ThirdEyeConfig.fromProperties(props);
    } catch (IllegalArgumentException e) {
      failed = true;
      props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_NAMES.toString(), "m1,m2,m3");
    }
    Assert.assertTrue(failed, "Expected exception due to missing metric names property");

    failed = false;
    try {
      props.remove(ThirdEyeConfigProperties.THIRDEYE_METRIC_TYPES.toString());
      config = ThirdEyeConfig.fromProperties(props);
    } catch (IllegalArgumentException e) {
      failed = true;
      props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_TYPES.toString(), "LONG,FLOAT,INT");
    }
    Assert.assertTrue(failed, "Expected exception due to missing metric types property");

    failed = false;
    try {
      props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_NAMES.toString(), "m1,m2");
      config = ThirdEyeConfig.fromProperties(props);
    } catch (IllegalStateException e) {
      failed = true;
      props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_NAMES.toString(), "m1,m2,m3");
    }
    Assert.assertTrue(failed, "Expecetd exception due to inequal number of metric names and types in properties");
  }

  @Test
  public void testTimeConfig() throws IllegalArgumentException {
    boolean failed = false;
    Assert.assertEquals(thirdeyeConfig.getTime().getColumnName(), "t1", "Incorrect time column name");
    Assert.assertNull(thirdeyeConfig.getInputTime(), "Incorrect input time column name");
    Assert.assertEquals(thirdeyeConfig.getTime().getTimeGranularity().getSize(), 10, "Incorrect time size");
    Assert.assertEquals(thirdeyeConfig.getTime().getTimeGranularity().getUnit(), TimeUnit.DAYS, "Incorrect time unit");

    try {
      props.remove(ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_NAME.toString());
      config = ThirdEyeConfig.fromProperties(props);
    } catch (IllegalArgumentException e) {
      failed = true;
      props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_NAME.toString(), "t1");
    }
    Assert.assertTrue(failed, "Expected exception due to missing time column property");

    props.remove(ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_SIZE.toString());
    props.remove(ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_TYPE.toString());

    config = ThirdEyeConfig.fromProperties(props);
    Assert.assertEquals(config.getTime().getTimeGranularity().getSize(), 1, "Incorrect default time size");
    Assert.assertEquals(config.getTime().getTimeGranularity().getUnit(), TimeUnit.HOURS, "Incorrect default time unit");
  }

  @Test
  public void testSplitConfig() throws Exception {
    Assert.assertEquals(thirdeyeConfig.getSplit().getThreshold(), 1000, "Incorrect split threshold");
    Assert.assertEquals(thirdeyeConfig.getSplit().getOrder().toArray(), new String[]{"d1", "d2", "d3"}, "Incorrect split order");

    props.remove(ThirdEyeConfigProperties.THIRDEYE_SPLIT_THRESHOLD.toString());
    config = ThirdEyeConfig.fromProperties(props);
    Assert.assertEquals(config.getSplit(), null, "Default split should be null");
  }

  @Test
  public void testTopKWhitelistConfig() throws IllegalArgumentException {
    boolean failed = false;
    TopkWhitelistSpec topKWhitelistSpec = thirdeyeConfig.getTopKWhitelist();
    // others values
    Map<String, String> nonWhitelistValueMap = topKWhitelistSpec.getNonWhitelistValue();
    Map<String, String> expectedNonWhitelistMap = new HashMap<>();
    expectedNonWhitelistMap.put("d1", "other");
    expectedNonWhitelistMap.put("d2","0");
    Assert.assertEquals(nonWhitelistValueMap, expectedNonWhitelistMap);
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_NONWHITELIST_VALUE_DIMENSION.toString() + ".d1", "dummy");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_NONWHITELIST_VALUE_DIMENSION.toString() + ".d2", "-1");
    config = ThirdEyeConfig.fromProperties(props);
    topKWhitelistSpec = config.getTopKWhitelist();
    nonWhitelistValueMap = topKWhitelistSpec.getNonWhitelistValue();
    expectedNonWhitelistMap = new HashMap<>();
    expectedNonWhitelistMap.put("d1", "dummy");
    expectedNonWhitelistMap.put("d2", "-1");
    Assert.assertEquals(nonWhitelistValueMap, expectedNonWhitelistMap);

    // thresholds
    Map<String, Double> threshold = topKWhitelistSpec.getThreshold();
    Assert.assertEquals(threshold.size(), 2, "Incorrect metric thresholds size");
    Assert.assertEquals(threshold.get("m1") == 0.02 && threshold.get("m3") == 0.1, true, "Incorrect metric thresholds config");
    try {
      props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_METRIC_THRESHOLD_VALUES.toString(), "0.1");
      config = ThirdEyeConfig.fromProperties(props);
    } catch (IllegalStateException e) {
      failed = true;
    }
    Assert.assertTrue(failed, "Expected exception due to unequal number of metrics and threshold");
    props.remove(ThirdEyeConfigProperties.THIRDEYE_TOPK_METRIC_THRESHOLD_VALUES.toString());
    props.remove(ThirdEyeConfigProperties.THIRDEYE_TOPK_THRESHOLD_METRIC_NAMES.toString());
    config = ThirdEyeConfig.fromProperties(props);
    Assert.assertEquals(config.getTopKWhitelist().getThreshold(), null, "Default threshold config should be null");

    // whitelist
    Map<String, List<String>> whitelist = topKWhitelistSpec.getWhitelist();
    Assert.assertEquals(whitelist.size(), 2, "Incorrect size of whitelist dimensions");
    List<String> expectedWhitelistValues = new ArrayList<>();
    expectedWhitelistValues.add("x"); expectedWhitelistValues.add("y");
    Assert.assertEquals(whitelist.get("d1"), expectedWhitelistValues, "Incorrect whitelist config");
    expectedWhitelistValues = new ArrayList<>();
    expectedWhitelistValues.add("500");
    Assert.assertEquals(whitelist.get("d2"), expectedWhitelistValues, "Incorrect whitelist config");
    props.remove(ThirdEyeConfigProperties.THIRDEYE_WHITELIST_DIMENSION_NAMES.toString());
    config = ThirdEyeConfig.fromProperties(props);
    Assert.assertEquals(config.getTopKWhitelist().getWhitelist(), null, "Default whitelist config should be null");

    // topk
    List<TopKDimensionToMetricsSpec> topk = topKWhitelistSpec.getTopKDimensionToMetricsSpec();
    Assert.assertEquals(topk.size(), 2, "Incorrect topk dimensions config size");
    TopKDimensionToMetricsSpec topkSpec = topk.get(0);
    Assert.assertEquals(topkSpec.getDimensionName().equals("d2")
          && topkSpec.getTopk().size() == 2
          && topkSpec.getTopk().get("m1") == 20
          && topkSpec.getTopk().get("m2") == 30, true, "Incorrect topk config");
    topkSpec = topk.get(1);
    Assert.assertEquals(topkSpec.getDimensionName().equals("d3")
        && topkSpec.getTopk().size() == 1
        && topkSpec.getTopk().get("m1") == 50, true, "Incorrect topk config");
    failed = false;
    try {
      props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_METRICS.toString() + ".d3", "m1");
      props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_KVALUES.toString() + ".d3", "50,50");
      config = ThirdEyeConfig.fromProperties(props);
    } catch (IllegalStateException e) {
      failed = true;
    }
    Assert.assertTrue(failed, "Expecetd exception due to inequal number of metrics and kvalues for dimension");
    props.remove(ThirdEyeConfigProperties.THIRDEYE_TOPK_DIMENSION_NAMES.toString());
    config = ThirdEyeConfig.fromProperties(props);
    Assert.assertEquals(config.getTopKWhitelist(), null, "Default topk should be null");

  }
}
