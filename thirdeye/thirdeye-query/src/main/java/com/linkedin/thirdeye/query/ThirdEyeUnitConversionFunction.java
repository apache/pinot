package com.linkedin.thirdeye.query;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;

import java.util.concurrent.TimeUnit;

public class ThirdEyeUnitConversionFunction implements ThirdEyeFunction {
  private final long outputSize;
  private final TimeUnit outputUnit;

  public ThirdEyeUnitConversionFunction(long outputSize, TimeUnit outputUnit) {
    this.outputSize = outputSize;
    this.outputUnit = outputUnit;
  }

  @Override
  public MetricTimeSeries apply(StarTreeConfig config, MetricTimeSeries timeSeries) {
    MetricSchema schema = timeSeries.getSchema();
    MetricTimeSeries converted = ThirdEyeFunctionUtils.copyBlankSeriesSame(schema.getNames(), schema);
    long inputSize = config.getTime().getBucket().getSize();
    TimeUnit inputUnit = config.getTime().getBucket().getUnit();

    for (Long time : timeSeries.getTimeWindowSet()) {
      long convertedTime = outputUnit.convert(time * inputSize, inputUnit) / outputSize;

      for (int i = 0; i < schema.getNumMetrics(); i++) {
        String name = schema.getMetricName(i);
        converted.increment(convertedTime, name, timeSeries.get(time, name));
      }
    }

    return converted;
  }
}
