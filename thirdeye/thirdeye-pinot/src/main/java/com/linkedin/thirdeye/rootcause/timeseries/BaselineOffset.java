package com.linkedin.thirdeye.rootcause.timeseries;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class BaselineOffset implements Baseline {
  final long offset;

  private BaselineOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public List<MetricSlice> from(MetricSlice slice) {
    return Collections.singletonList(slice
        .withStart(slice.getStart() + offset)
        .withEnd(slice.getEnd() + offset));
  }

  @Override
  public Map<MetricSlice, DataFrame> filter(MetricSlice slice, Map<MetricSlice, DataFrame> data) {
    MetricSlice pattern = from(slice).get(0);
    DataFrame value = data.get(pattern);

    if (!data.containsKey(pattern)) {
      return Collections.emptyMap();
    }

    return Collections.singletonMap(pattern, value);
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

    DataFrame output = new DataFrame(input);
    output.addSeries(COL_TIME, output.getLongs(COL_TIME).subtract(this.offset));

    return output;
  }

  public static BaselineOffset fromOffset(long offset) {
    return new BaselineOffset(offset);
  }
}
