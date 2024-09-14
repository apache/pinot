package org.apache.pinot.tsdb.spi.series;

import java.util.List;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.builders.MaxTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.builders.MinTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.builders.SummingTimeSeriesBuilder;


public class SimpleTimeSeriesBuilderFactory extends TimeSeriesBuilderFactory {
  public static final SimpleTimeSeriesBuilderFactory INSTANCE = new SimpleTimeSeriesBuilderFactory();

  private SimpleTimeSeriesBuilderFactory() {
    super();
  }

  @Override
  public BaseTimeSeriesBuilder newTimeSeriesBuilder(AggInfo aggInfo, String id, TimeBuckets timeBuckets,
      List<String> tagNames, Object[] tagValues) {
    switch (aggInfo.getAggFunction().toUpperCase()) {
      case "SUM":
        return new SummingTimeSeriesBuilder(id, timeBuckets, tagNames, tagValues);
      case "MIN":
        return new MinTimeSeriesBuilder(id, timeBuckets, tagNames, tagValues);
      case "MAX":
        return new MaxTimeSeriesBuilder(id, timeBuckets, tagNames, tagValues);
      default:
        throw new UnsupportedOperationException("Unsupported aggregation: " + aggInfo.getAggFunction());
    }
  }

  @Override
  public void init(PinotConfiguration pinotConfiguration) {
  }
}
