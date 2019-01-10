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

package com.linkedin.thirdeye.hadoop.util;

import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;

public class ThirdeyeAvroUtilsTest {

  public Schema avroSchema;

  private static final String AVRO_SCHEMA = "schema.avsc";

  @BeforeClass
  public void setup() throws Exception {
    avroSchema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_SCHEMA));
  }

  @Test
  public void testGetDimensionTypes() throws Exception{
    String dimensionTypesProperty = ThirdeyeAvroUtils.getDimensionTypesProperty("d1,d2,d3", avroSchema);
    Assert.assertEquals(dimensionTypesProperty, "STRING,LONG,STRING", "Dimension property not extracted correctly");
  }

  @Test
  public void testGetDimensionTypesEmpty() throws Exception{
    String dimensionTypesProperty = ThirdeyeAvroUtils.getDimensionTypesProperty("", avroSchema);
    Assert.assertEquals(dimensionTypesProperty, "", "Dimension property not extracted correctly");
  }

  @Test
  public void testGetMetricTypes() throws Exception{
    String metricTypesProperty = ThirdeyeAvroUtils.getMetricTypesProperty("m1,m2", null, avroSchema);
    Assert.assertEquals(metricTypesProperty, "INT,INT", "Metric property not extracted correctly");

    metricTypesProperty = ThirdeyeAvroUtils.getMetricTypesProperty("m1,m2", "INT,LONG", avroSchema);
    Assert.assertEquals(metricTypesProperty, "INT,LONG", "Metric property not extracted correctly");
  }

  @Test
  public void testGetDataTypeForField() throws Exception {
    String type = ThirdeyeAvroUtils.getDataTypeForField("d1", avroSchema);
    Assert.assertEquals(type, "STRING", "Data type not extracted correctly for d1");
    type = ThirdeyeAvroUtils.getDataTypeForField("hoursSinceEpoch", avroSchema);
    Assert.assertEquals(type, "LONG", "Data type not extracted correctly for hoursSinceEpoch");
    type = ThirdeyeAvroUtils.getDataTypeForField("m1", avroSchema);
    Assert.assertEquals(type, "INT", "Data type not extracted correctly for m1");
  }

  @Test
  public void testConstructAvroSchemaFromPinotSchema() throws Exception {
    com.linkedin.pinot.common.data.Schema pinotSchema = new com.linkedin.pinot.common.data.Schema();

    pinotSchema.setSchemaName("test");
    FieldSpec spec = new DimensionFieldSpec("d1", DataType.STRING, true);
    pinotSchema.addField(spec);
    spec = new MetricFieldSpec("m1", DataType.DOUBLE);
    pinotSchema.addField(spec);
    spec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "t"));
    pinotSchema.addField(spec);

    Schema avroSchema = ThirdeyeAvroUtils.constructAvroSchemaFromPinotSchema(pinotSchema);
    String dType = ThirdeyeAvroUtils.getDataTypeForField("d1", avroSchema);
    Assert.assertEquals(dType, "STRING", "Avro schema constructed incorrectly");
    dType = ThirdeyeAvroUtils.getDataTypeForField("m1", avroSchema);
    Assert.assertEquals(dType, "DOUBLE", "Avro schema constructed incorrectly");
    dType = ThirdeyeAvroUtils.getDataTypeForField("t", avroSchema);
    Assert.assertEquals(dType, "LONG", "Avro schema constructed incorrectly");
  }

}
