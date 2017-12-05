package com.linkedin.thirdeye.datasource;

import java.util.List;

import com.linkedin.thirdeye.api.TimeSpec;

public abstract class BaseThirdEyeResponse implements ThirdEyeResponse {
  protected final List<MetricFunction> metricFunctions;
  protected final ThirdEyeRequest request;
  protected final TimeSpec dataTimeSpec;

  public BaseThirdEyeResponse(ThirdEyeRequest request, TimeSpec dataTimeSpec) {
    this.request = request;
    this.dataTimeSpec = dataTimeSpec;
    this.metricFunctions = request.getMetricFunctions();
  }

  @Override
  public List<MetricFunction> getMetricFunctions() {
    return metricFunctions;
  }

  @Override
  public abstract int getNumRows();

  @Override
  public abstract ThirdEyeResponseRow getRow(int rowId);

  @Override
  public ThirdEyeRequest getRequest() {
    return request;
  }

  @Override
  public TimeSpec getDataTimeSpec() {
    return dataTimeSpec;
  }

  @Override
  public String toString() {
    return super.toString();
  }

}
