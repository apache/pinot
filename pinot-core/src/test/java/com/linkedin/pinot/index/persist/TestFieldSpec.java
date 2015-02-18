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
import com.linkedin.pinot.common.data.Schema.Builder;


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
        new Builder().addSingleValueDimension("svDimension", DataType.INT)
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
