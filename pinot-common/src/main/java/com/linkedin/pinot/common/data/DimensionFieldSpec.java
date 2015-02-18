package com.linkedin.pinot.common.data;

public class DimensionFieldSpec extends FieldSpec {

  public DimensionFieldSpec() {
    super();
  }

  public DimensionFieldSpec(String name, DataType dType, boolean singleValue, String delimeter) {
    super(name, FieldType.dimension, dType, singleValue, delimeter);
  }

  public DimensionFieldSpec(String name, DataType dType, boolean singleValue) {
    super(name, FieldType.dimension, dType, singleValue);
  }

}
