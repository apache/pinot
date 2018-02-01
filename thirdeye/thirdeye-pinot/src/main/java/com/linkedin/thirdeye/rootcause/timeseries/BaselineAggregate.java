package com.linkedin.thirdeye.rootcause.timeseries;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class BaselineAggregate implements Baseline {
  enum Type {
    SUM(DoubleSeries.SUM),
    PRODUCT(DoubleSeries.PRODUCT),
    MEAN(DoubleSeries.MEAN),
    MEDIAN(DoubleSeries.MEDIAN),
    MIN(DoubleSeries.MIN),
    MAX(DoubleSeries.MAX),
    STD(DoubleSeries.STD);

    final Series.DoubleFunction function;

    Type(Series.DoubleFunction function) {
      this.function = function;
    }

    public Series.DoubleFunction getFunction() {
      return function;
    }
  }

  final Type type;
  final List<Long> offsets;

  public BaselineAggregate(Type type, List<Long> offsets) {
    this.type = type;
    this.offsets = offsets;
  }

  @Override
  public List<MetricSlice> from(MetricSlice slice) {
    List<MetricSlice> slices = new ArrayList<>();
    for (long offset : this.offsets) {
      slices.add(slice
          .withStart(slice.getStart() + offset)
          .withEnd(slice.getEnd()+ offset));
    }
    return slices;
  }

  @Override
  public DataFrame compute(MetricSlice slice, Map<MetricSlice, DataFrame> data) {
    DataFrame joined = new DataFrame(COL_TIME, BaselineUtil.makeTimestamps(slice));

    List<String> colNames = new ArrayList<>();
    for (Map.Entry<MetricSlice, DataFrame> entry : data.entrySet()) {
      MetricSlice s = entry.getKey();

      long offset = s.getStart() - slice.getStart();
      if (!offsets.contains(offset)) {
        throw new IllegalArgumentException(String.format("Found slice with invalid offset %d", offset));
      }

      String colName = String.valueOf(s.getStart());

      DataFrame df = new DataFrame(entry.getValue());
      df.addSeries(COL_TIME, df.getLongs(COL_TIME).subtract(offset));
      df.renameSeries(COL_VALUE, colName);

      joined.addSeries(df, colName);
      colNames.add(colName);

      // TODO fill null forward?
    }

    String[] arrNames = colNames.toArray(new String[colNames.size()]);

    joined.addSeries(COL_VALUE, joined.map(this.type.function, arrNames));

    return joined;
  }
}
