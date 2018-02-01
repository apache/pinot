package com.linkedin.thirdeye.rootcause.timeseries;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class BaselineOffset implements Baseline {
  final int offset;

  public BaselineOffset(int offset) {
    this.offset = offset;
  }

  @Override
  public List<MetricSlice> from(MetricSlice slice) {
    return Collections.singletonList(slice
        .withStart(slice.getStart() + offset)
        .withEnd(slice.getEnd() + offset));
  }

  @Override
  public DataFrame compute(MetricSlice slice, Map<MetricSlice, DataFrame> data) {
    Preconditions.checkArgument(data.size() == 1);

    MetricSlice dataSlice = data.entrySet().iterator().next().getKey();
    DataFrame input = new DataFrame(data.entrySet().iterator().next().getValue());

    long offset = dataSlice.getStart() - slice.getStart();
    if (offset != this.offset) {
      throw new IllegalArgumentException(String.format("Found slice with invalid offset %d", offset));
    }

    input.addSeries(COL_TIME, input.getLongs(COL_TIME).subtract(this.offset));

    return new DataFrame(COL_TIME, BaselineUtil.makeTimestamps(slice)).addSeries(input, COL_VALUE);
  }
}
