package com.linkedin.thirdeye.query;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ThirdEyeUnitConversionFunction implements ThirdEyeFunction {
  private final long outputSize;
  private final TimeUnit outputUnit;
  private final List<String> metricNames;

  public ThirdEyeUnitConversionFunction(long outputSize, TimeUnit outputUnit, List<String> metricNames) {
    this.outputSize = outputSize;
    this.outputUnit = outputUnit;
    this.metricNames = metricNames;
  }

  @Override
  public MetricTimeSeries apply(StarTreeConfig config, ThirdEyeQuery query, MetricTimeSeries timeSeries) {
    List<MetricType> types = new ArrayList<>(metricNames.size());
    for (String name : metricNames) {
      types.add(timeSeries.getSchema().getMetricType(name));
    }
    MetricSchema schema = new MetricSchema(metricNames, types);
    MetricTimeSeries converted = ThirdEyeFunctionUtils.copyBlankSeriesSame(schema.getNames(), schema);

    long inputSize = config.getTime().getBucket().getSize();
    TimeUnit inputUnit = config.getTime().getBucket().getUnit();

    for (Long time : timeSeries.getTimeWindowSet()) {
      long convertedTime = outputUnit.convert(time * inputSize, inputUnit) / outputSize;
      for (String name : metricNames) {
        converted.increment(convertedTime, name, timeSeries.get(time, name));
      }
    }

    return converted;
  }
}
