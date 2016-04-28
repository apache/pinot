package com.linkedin.thirdeye.hadoop.util;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ThirdeyeAvroUtilsTest {

  public Schema avroSchema;

  private static final String AVRO_SCHEMA = "schema.avsc";

  @BeforeClass
  public void setup() throws Exception {
    avroSchema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_SCHEMA));
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

}
