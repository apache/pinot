package com.linkedin.pinot.common.data;

import java.util.concurrent.TimeUnit;


public class TimeFieldSpec extends FieldSpec {

  private GranularitySpec incomingGranularitySpec;
  private GranularitySpec outgoingGranularitySpec;

  public TimeFieldSpec() {
    super();
    this.incomingGranularitySpec = null;
    this.outgoingGranularitySpec = null;
  }

  public TimeFieldSpec(String name, DataType dType, TimeUnit timeType) {
    super(name, FieldType.time, dType, true);
    this.incomingGranularitySpec = new GranularitySpec(dType, timeType, name);
    this.outgoingGranularitySpec = incomingGranularitySpec;
  }

  public TimeFieldSpec(GranularitySpec incominGranularitySpec) {
    super(incominGranularitySpec.getColumnName(), FieldType.time, incominGranularitySpec.getdType(), true);
    this.incomingGranularitySpec = incominGranularitySpec;
    this.outgoingGranularitySpec = incomingGranularitySpec;
  }

  public TimeFieldSpec(GranularitySpec incominGranularitySpec, GranularitySpec outgoingGranularitySpec) {
    super(incominGranularitySpec.getColumnName(), FieldType.time, incominGranularitySpec.getdType(), true);
    this.incomingGranularitySpec = incominGranularitySpec;
    this.outgoingGranularitySpec = outgoingGranularitySpec;
  }

  public boolean isSame() {
    if (this.outgoingGranularitySpec == null && this.incomingGranularitySpec != null) {
      return true;
    }
    return incomingGranularitySpec.equals(outgoingGranularitySpec);
  }


  public String getIncomingTimeColumnName() {
    return incomingGranularitySpec.getColumnName();
  }

  public String getOutGoingTimeColumnName() {
    return outgoingGranularitySpec.getColumnName();
  }

  public GranularitySpec getIncominGranularutySpec() {
    return incomingGranularitySpec;
  }

  public GranularitySpec getOutgoingGranularitySpec() {
    return outgoingGranularitySpec;
  }
}
