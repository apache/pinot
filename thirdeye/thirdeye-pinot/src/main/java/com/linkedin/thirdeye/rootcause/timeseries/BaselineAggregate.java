package com.linkedin.thirdeye.rootcause.timeseries;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;


/**
 * Synthetic baseline from a list of time offsets, aggregated with a user-specified function.
 *
 * @see BaselineAggregateType
 */
public class BaselineAggregate implements Baseline {
  private final BaselineAggregateType type;
  private final List<Long> offsets;
  private final boolean isDailyData;

  private BaselineAggregate(BaselineAggregateType type, List<Long> offsets, boolean isDailyData) {
    this.type = type;
    this.offsets = offsets;
    this.isDailyData = isDailyData;
  }

  public BaselineAggregate withType(BaselineAggregateType type) {
    return new BaselineAggregate(type, this.offsets, this.isDailyData);
  }

  public BaselineAggregate withOffsets(List<Long> offsets) {
    return new BaselineAggregate(this.type, offsets, this.isDailyData);
  }

  public BaselineAggregate withDailyData(boolean isDailyData) {
    return new BaselineAggregate(this.type, this.offsets, isDailyData);
  }

  @Override
  public List<MetricSlice> scatter(MetricSlice slice) {
    List<MetricSlice> slices = new ArrayList<>();
    for (long offset : this.offsets) {
      slices.add(slice
          .withStart(slice.getStart() + offset)
          .withEnd(slice.getEnd()+ offset));
    }
    return slices;
  }

  private Map<MetricSlice, DataFrame> filter(MetricSlice slice, Map<MetricSlice, DataFrame> data) {
    Map<MetricSlice, DataFrame> output = new HashMap<>();
    Set<MetricSlice> patterns = new HashSet<>(scatter(slice));

    for (Map.Entry<MetricSlice, DataFrame> entry : data.entrySet()) {
      if (patterns.contains(entry.getKey())) {
        output.put(entry.getKey(), entry.getValue());
      }
    }

    return output;
  }

  @Override
  public DataFrame gather(MetricSlice slice, Map<MetricSlice, DataFrame> data) {
    Map<MetricSlice, DataFrame> filtered = this.filter(slice, data);

    // probe for daily data
    LongSeries timestamps = makeTimestamps(slice, data);
    Set<Long> timestampSet = new HashSet<>(timestamps.toList());

    DataFrame output = new DataFrame(COL_TIME, BaselineUtil.makeTimestamps(slice));

    boolean isInitialized = false;

    List<String> colNames = new ArrayList<>();
    for (Map.Entry<MetricSlice, DataFrame> entry : filtered.entrySet()) {
      MetricSlice s = entry.getKey();

      long offset = s.getStart() - slice.getStart();
      if (!offsets.contains(offset)) {
        throw new IllegalArgumentException(String.format("Found slice with invalid offset %d", offset));
      }

      String colName = String.valueOf(s.getStart());
      DataFrame df = new DataFrame(entry.getValue());

      df.addSeries(COL_TIME, df.getLongs(COL_TIME).subtract(offset));
      df.renameSeries(COL_VALUE, colName);

      if (isDailyData) {
        df = applyDSTCorrection(df, timestampSet);
      }

      if (!isInitialized) {
        // handle multi-index via prototyping
        DataFrame prototype = new DataFrame();
        for (String name : df.getIndexNames()) {
          prototype.addSeries(name, df.get(name));
        }

        output = output.joinLeft(prototype, COL_TIME);
        output.setIndex(df.getIndexNames());
        isInitialized = true;
      }

      output = output.joinOuter(df);
      colNames.add(colName);

      // TODO fill null forward?
    }

    String[] arrNames = colNames.toArray(new String[colNames.size()]);

    // aggregation
    output.addSeries(COL_VALUE, output.map(this.type.function, arrNames));
    output = output.dropNull();

    // alignment
    List<String> indexNames = output.getIndexNames();
    output = output.setIndex(COL_TIME).joinRight(new DataFrame(COL_TIME, timestamps)).setIndex(indexNames);
    output.sortedBy(output.getIndexNames());

    return output;
  }

  /**
   * Returns an instance of BaselineAggregate for the specified type and offsets
   *
   * @see BaselineAggregateType
   *
   * @param type aggregation type
   * @param offsets time offsets
   * @param isDailyData daily time series flag
   * @return BaselineAggregate with given type and offsets
   */
  public static BaselineAggregate fromOffsets(BaselineAggregateType type, List<Long> offsets, boolean isDailyData) {
    return new BaselineAggregate(type, offsets, isDailyData);
  }

  /**
   * Returns an instance of BaselineAggregate for the specified type and {@code numWeeks} offsets
   * computed on a consecutive week-over-week basis (in UTC time) starting with a lag of {@code offsetWeeks}.
   * A lag of {@code 0} corresponds to the current week.
   *
   * @see BaselineAggregateType
   *
   * @param type aggregation type
   * @param numWeeks number of consecutive weeks
   * @param offsetWeeks lag for starting consecutive weeks
   * @param isDailyData daily time series flag
   * @return BaselineAggregate with given type and weekly offsets
   */
  public static BaselineAggregate fromWeekOverWeek(BaselineAggregateType type, int numWeeks, int offsetWeeks, boolean isDailyData) {
    List<Long> offsets = new ArrayList<>();

    for (int i = 0; i < numWeeks; i++) {
      long offset = -1 * (i + offsetWeeks) * TimeUnit.DAYS.toMillis(7);
      offsets.add(offset);
    }

    return new BaselineAggregate(type, offsets, isDailyData);
  }

  /**
   * Returns an instance of BaselineAggregate for the specified type and {@code numWeeks} offsets
   * computed on a consecutive week-over-week basis starting with a lag of {@code offsetWeeks}.
   * Additionally corrects for DST changes assuming a start date of {@code timestamp} in {@code timezone}.
   * <br/><b>NOTE:</b> As offsets are pre-computed, the DST correction will produce incorrect offsets
   * if used to scatter a slice that does not start at {@code timestamp}.
   *
   * @see BaselineAggregate#fromWeekOverWeek(BaselineAggregateType, int, int, boolean)
   * @see BaselineAggregateType
   *
   * @param type aggregation type
   * @param numWeeks number of consecutive weeks
   * @param offsetWeeks lag for starting consecutive weeks
   * @param timestamp assumed slice start timestamp
   * @param timezone time zone (long form)
   * @param isDailyData daily time series flag
   * @return BaselineAggregate with given type and weekly offsets corrected for DST
   */
  public static BaselineAggregate fromWeekOverWeek(BaselineAggregateType type, int numWeeks, int offsetWeeks, long timestamp, String timezone, boolean isDailyData) {
    DateTime baseDate = new DateTime(timestamp, DateTimeZone.forID(timezone));

    List<Long> offsets = new ArrayList<>();

    for (int i = 0; i < numWeeks; i++) {
      long offset = baseDate.minusWeeks(i + offsetWeeks).getMillis() - timestamp;
      offsets.add(offset);
    }

    return new BaselineAggregate(type, offsets, isDailyData);
  }

  /**
   * Returns acceptable timestamps given the input data set. Uses {@code slice}
   * timestamps if available, otherwise constructs artificial time series.
   *
   * @param slice base metric slice
   * @param data timeseries data
   * @return timestamp series
   */
  private LongSeries makeTimestamps(MetricSlice slice, Map<MetricSlice, DataFrame> data) {
    if (data.containsKey(slice)) {
      return data.get(slice).dropNull().getLongs(COL_TIME);
    }

    if (this.isDailyData) {
      // construct daily timestamps
      return BaselineUtil.makeTimestamps(slice.withGranularity(new TimeGranularity(1, TimeUnit.DAYS)));
    } else {
      return BaselineUtil.makeTimestamps(slice);
    }
  }

  /**
   * Returns a data time series aligned to a set of acceptable timestamps. Only operates
   * on data series identified as daily data. This method is required to correct for
   * incorrect DST adjustment of daily data at the data source level or below.
   *
   * @param data data series
   * @param timestamps set of acceptable timestamps
   * @return aligned data series
   */
  private DataFrame applyDSTCorrection(DataFrame data, final Set<Long> timestamps) {
    if (!this.isDailyData) {
      return data;
    }

    DataFrame dataPlusOneHour = new DataFrame(data).addSeries(COL_TIME, data.getLongs(COL_TIME).add(TimeUnit.HOURS.toMillis(1)));
    DataFrame dataMinusOneHour = new DataFrame(data).addSeries(COL_TIME, data.getLongs(COL_TIME).subtract(TimeUnit.HOURS.toMillis(1)));

    return new DataFrame(data)
        .append(dataPlusOneHour, dataMinusOneHour)
        .filter(new Series.LongConditional() {
          @Override
          public boolean apply(long... values) {
            return timestamps.contains(values[0]);
          }
        }, COL_TIME)
        .dropNull()
        .sortedBy(COL_TIME);
  }
}
