package com.linkedin.pinot.common.data;

public class MetricFieldSpec extends FieldSpec {

  public MetricFieldSpec() {
    super();
  }

  public MetricFieldSpec(String name, DataType dType) {
    super(name, FieldType.metric, dType, true);
  }

}
