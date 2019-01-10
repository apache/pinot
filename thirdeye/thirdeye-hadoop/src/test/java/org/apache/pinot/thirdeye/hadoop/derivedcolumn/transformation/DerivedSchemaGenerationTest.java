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
package com.linkedin.thirdeye.hadoop.derivedcolumn.transformation;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfig;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfigProperties;

public class DerivedSchemaGenerationTest {
  private static final String AVRO_SCHEMA = "schema.avsc";

  DerivedColumnTransformationPhaseJob job = new DerivedColumnTransformationPhaseJob("derived_column_transformation", null);
  Schema inputSchema;
  ThirdEyeConfig thirdeyeConfig;
  Properties props;

  @BeforeTest
  public void setup() throws IOException {
    inputSchema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_SCHEMA));

    props = new Properties();
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TABLE_NAME.toString(), "collection");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_NAMES.toString(), "d1,d2,d3");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_TYPES.toString(), "STRING,LONG,STRING");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_NAMES.toString(), "m1,m2");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_TYPES.toString(), "INT,INT");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_NAME.toString(), "hoursSinceEpoch");

  }

  @Test
  public void testDerivedColumnsSchemaGeneration() throws Exception{
    ThirdEyeConfig thirdeyeConfig = ThirdEyeConfig.fromProperties(props);
    Schema outputSchema = job.newSchema(thirdeyeConfig);
    Assert.assertEquals(inputSchema.getFields().size(), outputSchema.getFields().size(),
        "Input schema should be same as output schema if no topk/whitelist in config");

    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_DIMENSION_NAMES.toString(), "d2,");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_METRICS.toString() + ".d2", "m1");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_KVALUES.toString() + ".d2", "1");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_WHITELIST_DIMENSION_NAMES.toString(), "d2,d3");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_WHITELIST_DIMENSION.toString() + ".d2" , "10,20,30");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_WHITELIST_DIMENSION.toString() + ".d3", "x,y");

    thirdeyeConfig = ThirdEyeConfig.fromProperties(props);
    outputSchema = job.newSchema(thirdeyeConfig);
    Assert.assertEquals(inputSchema.getFields().size() + 1, outputSchema.getFields().size(),
        "Input schema should not be same as output schema if topk/whitelist in config");

    Assert.assertEquals(outputSchema.getField("d2_topk") != null, true,
        "Output schema should have _topk entries for columsn in topk");
  }

}
