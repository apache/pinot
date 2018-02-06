package com.linkedin.thirdeye.rootcause.timeseries;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class BaselineAggregate implements Baseline {
  final BaselineType type;
  final List<Long> offsets;

  private BaselineAggregate(BaselineType type, List<Long> offsets) {
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
  public Map<MetricSlice, DataFrame> filter(MetricSlice slice, Map<MetricSlice, DataFrame> data) {
    Map<MetricSlice, DataFrame> output = new HashMap<>();
    Set<MetricSlice> patterns = new HashSet<>(from(slice));

    for (Map.Entry<MetricSlice, DataFrame> entry : data.entrySet()) {
      if (patterns.contains(entry.getKey())) {
        output.put(entry.getKey(), entry.getValue());
      }
    }

    return output;
  }

  @Override
  public DataFrame compute(MetricSlice slice, Map<MetricSlice, DataFrame> data) {
    DataFrame joined = new DataFrame(COL_TIME, BaselineUtil.makeTimestamps(slice));

    final Set<Long> timestamps = new HashSet<>(joined.getLongs(COL_TIME).toList());
    boolean isInitialized = false;

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
      df = df.filter(new Series.LongConditional() {
        @Override
        public boolean apply(long... values) {
          return timestamps.contains(values[0]);
        }
      }, COL_TIME).dropNull();

      if (!isInitialized) {
        // handle multi-index via prototyping
        DataFrame prototype = new DataFrame();
        for (String name : df.getIndexNames()) {
          prototype.addSeries(name, df.get(name));
        }

        joined = joined.joinOuter(prototype, COL_TIME);
        joined.setIndex(df.getIndexNames());
        isInitialized = true;
      }

      joined = joined.joinOuter(df);
      colNames.add(colName);

      // TODO fill null forward?
    }

    String[] arrNames = colNames.toArray(new String[colNames.size()]);

    joined.addSeries(COL_VALUE, joined.map(this.type.function, arrNames));
    joined.sortedBy(joined.getIndexNames());

    return joined;
  }

  public static BaselineAggregate fromOffsets(BaselineType type, List<Long> offsets) {
    return new BaselineAggregate(type, offsets);
  }

  public static BaselineAggregate fromWeekOverWeek(BaselineType type, int numWeeks) {
    return fromWeekOverWeek(type, numWeeks, 0);
  }

  public static BaselineAggregate fromWeekOverWeek(BaselineType type, int numWeeks, int offsetWeeks) {
    List<Long> offsets = new ArrayList<>();

    for (int i = 0; i < numWeeks; i++) {
      long offset = -1 * (i + offsetWeeks) * TimeUnit.DAYS.toMillis(7);
      offsets.add(offset);
    }

    return new BaselineAggregate(type, offsets);
  }
}
