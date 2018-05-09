package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.dataframe.BooleanSeries;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.StaticDetectionPipeline;
import com.linkedin.thirdeye.detection.StaticDetectionPipelineData;
import com.linkedin.thirdeye.detection.StaticDetectionPipelineModel;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.PeriodType;


public class MovingWindowAlgorithm extends StaticDetectionPipeline {
  private static final String COL_CURR = "current";
  private static final String COL_BASE = "baseline";
  private static final String COL_STD = "std";
  private static final String COL_MEAN = "mean";
  private static final String COL_QUANTILE = "quantile";
  private static final String COL_ZSCORE = "zscore";
  private static final String COL_QUANTILE_VIOLATION = "quantile_violation";
  private static final String COL_ZSCORE_VIOLATION = "zscore_violation";
  private static final String COL_ANOMALY = "anomaly";
  private static final String COL_TIME = DataFrameUtils.COL_TIME;
  private static final String COL_VALUE = DataFrameUtils.COL_VALUE;

  private static final String PROP_METRIC_URN = "metricUrn";

  private static final String PROP_LOOKBACK = "lookback";
  private static final Period PROP_LOOKBACK_DEFAULT = new Period(7, PeriodType.days());

  private static final String PROP_QUANTILE = "quantile";
  private static final double PROP_QUANTILE_DEFAULT = Double.NaN;

  private static final String PROP_ZSCORE = "zscore";
  private static final double PROP_ZSCORE_DEFAULT = Double.NaN;

  private static final String PROP_WEEK_OVER_WEEK = "weekOverWeek";
  private static final boolean PROP_WEEK_OVER_WEEK_DEFAULT = false;

  private static final String PROP_TIMEZONE = "timezone";
  private static final String PROP_TIMEZONE_DEFAULT = "UTC";

  private static final Pattern PATTERN_LOOKBACK = Pattern.compile("([0-9]+)\\s*(\\S*)");

  private final MetricSlice slice;
  private final Period lookback;
  private final double zscore;
  private final double quantile;
  private final boolean weekOverWeek;
  private final DateTimeZone timezone;

  public MovingWindowAlgorithm(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    Preconditions.checkArgument(config.getProperties().containsKey(PROP_METRIC_URN));

    String metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    MetricEntity me = MetricEntity.fromURN(metricUrn, 1.0);

    this.weekOverWeek = MapUtils.getBooleanValue(config.getProperties(), PROP_WEEK_OVER_WEEK, PROP_WEEK_OVER_WEEK_DEFAULT);
    this.quantile = MapUtils.getDoubleValue(config.getProperties(), PROP_QUANTILE, PROP_QUANTILE_DEFAULT);
    this.zscore = MapUtils.getDoubleValue(config.getProperties(), PROP_ZSCORE, PROP_ZSCORE_DEFAULT);
    this.lookback = parseLookback(config.getProperties().get(PROP_LOOKBACK).toString());
    this.timezone = DateTimeZone.forID(MapUtils.getString(config.getProperties(), PROP_TIMEZONE, PROP_TIMEZONE_DEFAULT));

    DateTime trainStart = new DateTime(startTime, this.timezone).minus(this.lookback);
    if (this.weekOverWeek) {
      trainStart.minus(new Period(1, PeriodType.weeks()));
    }

    this.slice = MetricSlice.from(me.getId(), trainStart.getMillis(), endTime, me.getFilters());
  }

  @Override
  public StaticDetectionPipelineModel getModel() {
    return new StaticDetectionPipelineModel()
        .withTimeseriesSlices(Collections.singleton(this.slice));
  }

  @Override
  public DetectionPipelineResult run(StaticDetectionPipelineData data) throws Exception {
    DataFrame df = data.getTimeseries().get(this.slice);

    if (this.weekOverWeek) {
      df = this.differentiateWeekOverWeek(df);
    }

    // potentially variable window size. manual iteration required.
    df = applyMovingWindow(df);

    df.addSeries(COL_ZSCORE, df.getDoubles(COL_VALUE).subtract(df.get(COL_MEAN)).divide(df.get(COL_STD)));

    // zscore check
    df.addSeries(COL_ZSCORE_VIOLATION, BooleanSeries.fillValues(df.size(), false));
    if (!Double.isNaN(this.zscore)) {
      df.mapInPlace(new Series.DoubleConditional() {
        @Override
        public boolean apply(double... values) {
          return Math.abs(values[0]) > MovingWindowAlgorithm.this.zscore;
        }
      }, COL_ZSCORE_VIOLATION, COL_ZSCORE).fillNull(COL_ZSCORE_VIOLATION);
    }

    // quantile check
    df.addSeries(COL_QUANTILE_VIOLATION, BooleanSeries.fillValues(df.size(), false));
    if (!Double.isNaN(this.quantile)) {
      df.mapInPlace(new Series.DoubleConditional() {
        @Override
        public boolean apply(double... values) {
          return Math.abs(values[0]) > MovingWindowAlgorithm.this.zscore;
        }
      }, COL_QUANTILE_VIOLATION, COL_QUANTILE).fillNull(COL_QUANTILE_VIOLATION);
    }

    // anomalies
    df.mapInPlace(BooleanSeries.HAS_TRUE, COL_ANOMALY, COL_ZSCORE_VIOLATION, COL_QUANTILE_VIOLATION);

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    for (int i = 0; i < df.size(); i++) {
      if (BooleanSeries.booleanValueOf(df.getBoolean(COL_ANOMALY, i))) {
        long start = df.getLong(COL_TIME, i);
        long end = getEndTime(df, i);

        anomalies.add(this.makeAnomaly(this.slice.withStart(start).withEnd(end)));
      }
    }

    long maxTime = -1;
    if (!df.isEmpty()) {
      maxTime = df.getLongs(COL_TIME).max().longValue();
    }

    return new DetectionPipelineResult(anomalies, maxTime);
  }

  private long getEndTime(DataFrame df, int index) {
    if (index < df.size() - 1) {
      return df.getLong(COL_TIME, index + 1);
    }
    return df.getLongs(COL_TIME).max().longValue() + 1; // TODO use time granularity
  }

  /**
   * Returns data frame augmented with moving window statistics (std, mean, quantile)
   *
   * @param df data
   * @return data augmented with moving window statistics
   */
  DataFrame applyMovingWindow(DataFrame df) {
    DataFrame res = new DataFrame(df);

    LongSeries timestamps = res.getLongs(COL_TIME).filter(new Series.LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] >= MovingWindowAlgorithm.this.startTime;
      }
    }).dropNull();

    double[] std = new double[timestamps.size()];
    double[] mean = new double[timestamps.size()];
    double[] quantile = new double[timestamps.size()];

    Arrays.fill(std, Double.NaN);
    Arrays.fill(mean, Double.NaN);
    Arrays.fill(quantile, Double.NaN);

    for (int i = 0; i < timestamps.size(); i++) {
      DataFrame window = this.makeWindow(res, timestamps.getLong(i));

      if (window.isEmpty()) {
        continue;
      }

      mean[i] = window.getDoubles(COL_VALUE).mean().doubleValue();

      if (window.size() >= 2) {
        std[i] = window.getDoubles(COL_VALUE).std().doubleValue();
      }

      if (!Double.isNaN(this.quantile)) {
        quantile[i] = window.getDoubles(COL_VALUE).quantile(this.quantile).doubleValue();
      }
    }

    DataFrame output = new DataFrame(COL_TIME, timestamps)
        .addSeries(COL_STD, DoubleSeries.buildFrom(std))
        .addSeries(COL_MEAN, DoubleSeries.buildFrom(mean))
        .addSeries(COL_QUANTILE, DoubleSeries.buildFrom(quantile));

    return res.addSeries(output);
  }

  /**
   * Returns a data with values differentiated week-over-week.
   *
   * @param df data
   * @return data with differentiated values
   */
  DataFrame differentiateWeekOverWeek(DataFrame df) {
    DataFrame dfCurr = new DataFrame(df);
    DataFrame dfBase = new DataFrame(df);

    dfBase.mapInPlace(new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return new DateTime(values[0], MovingWindowAlgorithm.this.timezone)
            .plus(MovingWindowAlgorithm.this.lookback).getMillis();
      }
    }, COL_TIME);
    dfBase.renameSeries(COL_VALUE, COL_BASE);

    dfCurr.addSeries(dfBase, COL_BASE);
    dfCurr.renameSeries(COL_VALUE, COL_CURR);

    dfCurr.addSeries(COL_VALUE, dfCurr.getDoubles(COL_CURR).subtract(dfCurr.get(COL_BASE)));

    return dfCurr.dropNull();
  }

  /**
   * Returns variable-size look back window for given timestamp.
   *
   * @param df data
   * @param tCurrent end timestamp (exclusive)
   * @return window data frame
   */
  DataFrame makeWindow(DataFrame df, final long tCurrent) {
    final long tStart = new DateTime(tCurrent, this.timezone).minus(this.lookback).getMillis();

    return df.filter(new Series.LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] >= tStart && values[0] < tCurrent;
      }
    }, COL_TIME).dropNull();
  }

  /**
   * Helper for parsing lookback period string from config (e.g. '3 days', '1min', '3600000')
   *
   * @param lookback
   * @return
   */
  static Period parseLookback(String lookback) {
    if (StringUtils.isBlank(lookback)) {
      return PROP_LOOKBACK_DEFAULT;
    }

    Matcher m = PATTERN_LOOKBACK.matcher(lookback);
    if (!m.find()) {
      throw new IllegalArgumentException(String.format("Could not parse lookback expression '%s'", lookback));
    }

    int size = Integer.valueOf(m.group(1).trim());

    PeriodType t = PeriodType.millis();
    if (m.group(2).length() > 0) {
      t = parsePeriodType(m.group(2).trim());
    }

    return new Period().withFieldAdded(t.getFieldType(0), size);
  }

  /**
   * Helper for heuristically parsing period unit from config (e.g. 'millis', 'hour', 'd')
   *
   * @param type period type string
   * @return PeriodType
   */
  static PeriodType parsePeriodType(String type) {
    type = type.toLowerCase();

    if (type.startsWith("y") || type.startsWith("a")) {
      return PeriodType.years();
    }
    if (type.startsWith("mo")) {
      return PeriodType.months();
    }
    if (type.startsWith("w")) {
      return PeriodType.weeks();
    }
    if (type.startsWith("d")) {
      return PeriodType.days();
    }
    if (type.startsWith("h")) {
      return PeriodType.hours();
    }
    if (type.startsWith("s")) {
      return PeriodType.seconds();
    }
    if (type.startsWith("mill") || type.startsWith("ms")) {
      return PeriodType.millis();
    }
    if (type.startsWith("m")) {
      return PeriodType.minutes();
    }

    throw new IllegalArgumentException(String.format("Invalid period type '%s'", type));
  }
}
