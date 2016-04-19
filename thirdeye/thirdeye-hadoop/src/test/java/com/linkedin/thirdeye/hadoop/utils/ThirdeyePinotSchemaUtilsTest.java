/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.thirdeye.hadoop.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.thirdeye.hadoop.ThirdEyeConfig;
import com.linkedin.thirdeye.hadoop.ThirdEyeConfigConstants;
import com.linkedin.thirdeye.hadoop.util.ThirdeyePinotSchemaUtils;

public class ThirdeyePinotSchemaUtilsTest {

  ThirdEyeConfig thirdeyeConfig;
  Properties props;

  @BeforeTest
  public void setup() {
    props = new Properties();
    props.setProperty(ThirdEyeConfigConstants.THIRDEYE_TABLE_NAME.toString(), "collection");
    props.setProperty(ThirdEyeConfigConstants.THIRDEYE_DIMENSION_NAMES.toString(), "d1,d2,d3");
    props.setProperty(ThirdEyeConfigConstants.THIRDEYE_METRIC_NAMES.toString(), "m1,m2");
    props.setProperty(ThirdEyeConfigConstants.THIRDEYE_METRIC_TYPES.toString(), "INT,INT");
    props.setProperty(ThirdEyeConfigConstants.THIRDEYE_TIMECOLUMN_NAME.toString(), "hoursSinceEpoch");

    thirdeyeConfig = ThirdEyeConfig.fromProperties(props);
  }

  @Test
  public void testThirdeyeConfigToPinotSchemaGeneration() throws Exception {
    Schema schema = ThirdeyePinotSchemaUtils.createSchema(thirdeyeConfig);

    Assert.assertEquals(schema.getAllFieldSpecs().size(), 7, "Incorrect pinot schema fields list size");
    List<String> dimensions = Arrays.asList("d1", "d2", "d3");
    Assert.assertEquals(schema.getDimensionNames().containsAll(dimensions), true,
        "New schema dimensions " + schema.getDimensionNames() + " is missing dimensions");

    List<String> metrics = Arrays.asList("m1", "m2", "__COUNT");
    Assert.assertEquals(schema.getMetricNames().containsAll(metrics), true,
        "New schema metrics " + schema.getMetricNames() + "is missing metrics");

    Assert.assertEquals(schema.getTimeColumnName(), "hoursSinceEpoch");

  }

}
