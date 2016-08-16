/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.data;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SchemaTest {
  public static final Logger LOGGER = LoggerFactory.getLogger(SchemaTest.class);

  private Schema schema;

  @BeforeClass
  public void setUp()
      throws IOException {
    URL resourceUrl = getClass().getClassLoader().getResource("schemaTest.schema");
    Preconditions.checkNotNull(resourceUrl);
    schema = Schema.fromFile(new File(resourceUrl.getFile()));
  }

  @Test
  public void testValidation() throws Exception {
    Schema schemaToValidate;

    schemaToValidate = Schema.fromString(makeSchema(FieldSpec.DataType.LONG, FieldSpec.DataType.STRING, true));
    Assert.assertTrue(schemaToValidate.validate(LOGGER));

    schemaToValidate = Schema.fromString(makeSchema(FieldSpec.DataType.BOOLEAN, FieldSpec.DataType.STRING, true));
    Assert.assertFalse(schemaToValidate.validate(LOGGER));

    schemaToValidate = Schema.fromString(makeSchema(FieldSpec.DataType.STRING, FieldSpec.DataType.STRING, false));
    Assert.assertFalse(schemaToValidate.validate(LOGGER));

    schemaToValidate = Schema.fromString(makeSchema(FieldSpec.DataType.LONG, FieldSpec.DataType.BOOLEAN, false));
    Assert.assertTrue(schemaToValidate.validate(LOGGER));
  }

  private String makeSchema(FieldSpec.DataType metricType, FieldSpec.DataType dimensionType, boolean isSingleValue) {
    return "{"
        + "  \"schemaName\":\"SchemaTest\","
        + "  \"metricFieldSpecs\":["
        + "    {\"name\":\"m\",\"dataType\":\"" + metricType + "\"}"
        + "  ],"
        + "  \"dimensionFieldSpecs\":["
        + "    {\"name\":\"d\",\"dataType\":\"" + dimensionType + "\",\"singleValueField\":" + isSingleValue + "}"
        + "  ],"
        + "  \"timeFieldSpec\":{"
        + "    \"incomingGranularitySpec\":{\"dataType\":\"LONG\",\"timeType\":\"MILLISECONDS\",\"name\":\"time\"},"
        + "    \"defaultNullValue\":12345"
        + "  }"
        + "}";
  }

  @Test
  public void testSchemaBuilder() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("svDimension", FieldSpec.DataType.INT)
        .addMultiValueDimension("mvDimension", FieldSpec.DataType.STRING)
        .addMetric("metric", FieldSpec.DataType.INT)
        .addMetric("derivedMetric", FieldSpec.DataType.STRING, 10, MetricFieldSpec.DerivedMetricType.HLL)
        .addTime("time", TimeUnit.DAYS, FieldSpec.DataType.LONG)
        .build();

    FieldSpec fieldSpec;
    fieldSpec = schema.getDimensionSpec("svDimension");
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), true);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.INT);

    fieldSpec = schema.getDimensionSpec("mvDimension");
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), false);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.STRING);

    fieldSpec = schema.getMetricSpec("metric");
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), true);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.INT);

    fieldSpec = schema.getMetricSpec("derivedMetric");
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), true);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.STRING);

    fieldSpec = schema.getTimeFieldSpec();
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), true);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.LONG);
  }

  @Test
  public void testSerializeDeserialize()
      throws IOException, IllegalAccessException {
    Schema newSchema;

    newSchema = Schema.fromString(schema.getJSONSchema());
    Assert.assertEquals(newSchema, schema);
    Assert.assertEquals(newSchema.hashCode(), schema.hashCode());

    newSchema = Schema.fromZNRecord(Schema.toZNRecord(schema));
    Assert.assertEquals(newSchema, schema);
    Assert.assertEquals(newSchema.hashCode(), schema.hashCode());
  }
}
