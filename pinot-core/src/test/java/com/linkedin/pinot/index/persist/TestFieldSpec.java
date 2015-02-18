package com.linkedin.pinot.index.persist;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;


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
}
