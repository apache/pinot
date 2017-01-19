package com.linkedin.thirdeye.anomalydetection.model.transform;

import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;

public class MovingAverageSmoothingFunction extends AbstractTransformationFunction {
  public static final String BUCKET_SIZE = "bucketSize";
  public static final String BUCKET_UNIT = "bucketUnit";

  @Override public TimeSeries transform(TimeSeries timeSeries) {
    return null;
  }
}
