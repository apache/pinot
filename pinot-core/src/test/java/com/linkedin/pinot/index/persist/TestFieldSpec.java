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
package com.linkedin.pinot.index.persist;

import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema.SchemaBuilder;


public class TestFieldSpec {

  @Test
  public void testFieldSpec() {
    FieldSpec fieldSpec = new DimensionFieldSpec();
    fieldSpec.setDataType(DataType.INT);
    fieldSpec.setFieldType(FieldType.dimension);
    fieldSpec.setSingleValueField(true);
    fieldSpec.setDelimeter(",");
    AssertJUnit.assertEquals("< data type : INT , field type : dimension, single value column, delimeter : , >",
        fieldSpec.toString());

    fieldSpec.setDataType(DataType.DOUBLE);
    fieldSpec.setFieldType(FieldType.metric);
    fieldSpec.setSingleValueField(true);
    fieldSpec.setDelimeter(":");
    AssertJUnit.assertEquals("< data type : DOUBLE , field type : metric, single value column, delimeter : : >",
        fieldSpec.toString());

    fieldSpec.setDataType(DataType.STRING);
    fieldSpec.setFieldType(FieldType.dimension);
    fieldSpec.setSingleValueField(false);
    fieldSpec.setDelimeter(";");
    AssertJUnit.assertEquals("< data type : STRING , field type : dimension, multi value column, delimeter : ; >",
        fieldSpec.toString());
  }

  @Test
  public void testSchemaBuilder() {
    Schema schema =
        new SchemaBuilder().addSingleValueDimension("svDimension", DataType.INT)
        .addMultiValueDimension("mvDimension", DataType.STRING, ",").addMetric("metric", DataType.INT)
        .addTime("incomingTime", TimeUnit.DAYS, DataType.LONG).build();

    Assert.assertEquals(schema.getFieldSpecFor("svDimension").isSingleValueField(), true);
    Assert.assertEquals(schema.getFieldSpecFor("svDimension").getDataType(), DataType.INT);

    Assert.assertEquals(schema.getFieldSpecFor("mvDimension").isSingleValueField(), false);
    Assert.assertEquals(schema.getFieldSpecFor("mvDimension").getDataType(), DataType.STRING);
    Assert.assertEquals(schema.getFieldSpecFor("mvDimension").getDelimiter(), ",");

    Assert.assertEquals(schema.getFieldSpecFor("metric").isSingleValueField(), true);
    Assert.assertEquals(schema.getFieldSpecFor("metric").getDataType(), DataType.INT);

    Assert.assertEquals(schema.getFieldSpecFor("incomingTime").isSingleValueField(), true);
    Assert.assertEquals(schema.getFieldSpecFor("incomingTime").getDataType(), DataType.LONG);

  }
}
