package com.linkedin.thirdeye;

import com.linkedin.thirdeye.api.ThirdEyeMetrics;
import com.linkedin.thirdeye.api.ThirdEyeTimeSeries;

import java.util.List;

public interface ThirdEyeClient
{
  List<ThirdEyeMetrics> getMetrics(ThirdEyeQuery query);

  List<ThirdEyeTimeSeries> getTimeSeries(ThirdEyeQuery query);
}
