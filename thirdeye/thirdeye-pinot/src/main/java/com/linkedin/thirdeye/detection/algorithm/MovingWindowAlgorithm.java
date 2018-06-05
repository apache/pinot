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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationFieldType;
import org.joda.time.Period;
import org.joda.time.PeriodType;


public class MovingWindowAlgorithm extends StaticDetectionPipeline {
  private static final String COL_CURR = "current";
  private static final String COL_BASE = "baseline";
  private static final String COL_STD = "std";
  private static final String COL_MEAN = "mean";
  private static final String COL_QUANTILE_MIN = "quantileMin";
  private static final String COL_QUANTILE_MAX = "quantileMax";
  private static final String COL_ZSCORE = "zscore";
  private static final String COL_QUANTILE_MIN_VIOLATION = "quantileMinViolation";
  private static final String COL_QUANTILE_MAX_VIOLATION = "quantileMaxViolation";
  private static final String COL_ZSCORE_MIN_VIOLATION = "zscoreMinViolation";
  private static final String COL_ZSCORE_MAX_VIOLATION = "zscoreMaxViolation";
  private static final String COL_ANOMALY = "anomaly";
  private static final String COL_TIME = DataFrameUtils.COL_TIME;
  private static final String COL_VALUE = DataFrameUtils.COL_VALUE;
  private static final String COL_OUTLIER = "outlier";

  private static final String PROP_METRIC_URN = "metricUrn";

  private static final String PROP_LOOKBACK = "lookback";
  private static final String PROP_LOOKBACK_DEFAULT = "1w";

  private static final String PROP_OUTLIER_DURATION = "outlierDuration";
  private static final String PROP_OUTLIER_DURATION_DEFAULT = "0";

  private static final String PROP_CHANGE_DURATION = "changeDuration";
  private static final String PROP_CHANGE_DURATION_DEFAULT = "0";

  private static final String PROP_QUANTILE_MIN = "quantileMin";
  private static final double PROP_QUANTILE_MIN_DEFAULT = Double.NaN;

  private static final String PROP_QUANTILE_MAX = "quantileMax";
  private static final double PROP_QUANTILE_MAX_DEFAULT = Double.NaN;

  private static final String PROP_ZSCORE_MIN = "zscoreMin";
  private static final double PROP_ZSCORE_MIN_DEFAULT = Double.NaN;

  private static final String PROP_ZSCORE_MAX = "zscoreMax";
  private static final double PROP_ZSCORE_MAX_DEFAULT = Double.NaN;

  private static final String PROP_WEEK_OVER_WEEK = "weekOverWeek";
  private static final boolean PROP_WEEK_OVER_WEEK_DEFAULT = false;

  private static final String PROP_TIMEZONE = "timezone";
  private static final String PROP_TIMEZONE_DEFAULT = "UTC";

  private static final Pattern PATTERN_PERIOD = Pattern.compile("([0-9]+)\\s*(\\S*)");

  private final MetricSlice slice;
  private final Period lookback;
  private final double zscoreMin;
  private final double zscoreMax;
  private final double quantileMin;
  private final double quantileMax;
  private final boolean weekOverWeek;
  private final DateTimeZone timezone;
  private final Period outlierDuration;
  private final Period changeDuration;

  public MovingWindowAlgorithm(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    Preconditions.checkArgument(config.getProperties().containsKey(PROP_METRIC_URN));

    String metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    MetricEntity me = MetricEntity.fromURN(metricUrn, 1.0);

    this.weekOverWeek = MapUtils.getBooleanValue(config.getProperties(), PROP_WEEK_OVER_WEEK, PROP_WEEK_OVER_WEEK_DEFAULT);
    this.quantileMin = MapUtils.getDoubleValue(config.getProperties(), PROP_QUANTILE_MIN, PROP_QUANTILE_MIN_DEFAULT);
    this.quantileMax = MapUtils.getDoubleValue(config.getProperties(), PROP_QUANTILE_MAX, PROP_QUANTILE_MAX_DEFAULT);
    this.zscoreMin = MapUtils.getDoubleValue(config.getProperties(), PROP_ZSCORE_MIN, PROP_ZSCORE_MIN_DEFAULT);
    this.zscoreMax = MapUtils.getDoubleValue(config.getProperties(), PROP_ZSCORE_MAX, PROP_ZSCORE_MAX_DEFAULT);
    this.timezone = DateTimeZone.forID(MapUtils.getString(config.getProperties(), PROP_TIMEZONE, PROP_TIMEZONE_DEFAULT));
    this.lookback = parsePeriod(MapUtils.getString(config.getProperties(), PROP_LOOKBACK, PROP_LOOKBACK_DEFAULT));
    this.outlierDuration = parsePeriod(MapUtils.getString(config.getProperties(), PROP_OUTLIER_DURATION, PROP_OUTLIER_DURATION_DEFAULT));
    this.changeDuration = parsePeriod(MapUtils.getString(config.getProperties(), PROP_CHANGE_DURATION, PROP_CHANGE_DURATION_DEFAULT));

    Preconditions.checkArgument(Double.isNaN(this.quantileMin) || (this.quantileMin >= 0 && this.quantileMin <= 1.0), PROP_QUANTILE_MIN + " must be between 0.0 and 1.0");
    Preconditions.checkArgument(Double.isNaN(this.quantileMax) || (this.quantileMax >= 0 && this.quantileMax <= 1.0), PROP_QUANTILE_MAX + " must be between 0.0 and 1.0");

    DateTime trainStart = new DateTime(startTime, this.timezone).minus(this.lookback);
    if (this.weekOverWeek) {
      trainStart = trainStart.minus(new Period().withField(DurationFieldType.weeks(), 1));
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

    // zscore check
    df.addSeries(COL_ZSCORE_MIN_VIOLATION, BooleanSeries.fillValues(df.size(), false));
    df.addSeries(COL_ZSCORE_MAX_VIOLATION, BooleanSeries.fillValues(df.size(), false));

    if (!Double.isNaN(this.zscoreMin) || !Double.isNaN(this.zscoreMax)) {
      df.addSeries(COL_ZSCORE, df.getDoubles(COL_VALUE).subtract(df.get(COL_MEAN)).divide(df.getDoubles(COL_STD).replace(0.0d, DoubleSeries.NULL)));

      if (!Double.isNaN(this.zscoreMin)) {
        df.addSeries(COL_ZSCORE_MIN_VIOLATION, df.getDoubles(COL_ZSCORE).lt(this.zscoreMin).fillNull());
      }

      if (!Double.isNaN(this.zscoreMax)) {
        df.addSeries(COL_ZSCORE_MAX_VIOLATION, df.getDoubles(COL_ZSCORE).gt(this.zscoreMax).fillNull());
      }
    }

    // quantile check
    df.addSeries(COL_QUANTILE_MIN_VIOLATION, BooleanSeries.fillValues(df.size(), false));
    df.addSeries(COL_QUANTILE_MAX_VIOLATION, BooleanSeries.fillValues(df.size(), false));

    if (!Double.isNaN(this.quantileMin)) {
      df.addSeries(COL_QUANTILE_MIN_VIOLATION, df.getDoubles(COL_VALUE).lt(df.get(COL_QUANTILE_MIN)).fillNull());
    }

    if (!Double.isNaN(this.quantileMax)) {
      df.addSeries(COL_QUANTILE_MAX_VIOLATION, df.getDoubles(COL_VALUE).gt(df.get(COL_QUANTILE_MAX)).fillNull());
    }

    // anomalies
    df.mapInPlace(BooleanSeries.HAS_TRUE, COL_ANOMALY, COL_ZSCORE_MIN_VIOLATION, COL_ZSCORE_MAX_VIOLATION, COL_QUANTILE_MIN_VIOLATION, COL_QUANTILE_MAX_VIOLATION);

    System.out.println("*******************************************");
    System.out.println(this.slice.getFilters());
    System.out.println(df);

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

    res.addSeries(COL_OUTLIER, BooleanSeries.fillValues(res.size(), false));

    if (this.outlierDuration.toStandardDuration().getMillis() > 0) {
      res.addSeries(COL_OUTLIER, AlgorithmUtils.getOutliers(res, this.outlierDuration.toStandardDuration()));
    }

    // TODO support change points

    double[] std = new double[timestamps.size()];
    double[] mean = new double[timestamps.size()];
    double[] quantileMin = new double[timestamps.size()];
    double[] quantileMax = new double[timestamps.size()];

    Arrays.fill(std, Double.NaN);
    Arrays.fill(mean, Double.NaN);
    Arrays.fill(quantileMin, Double.NaN);
    Arrays.fill(quantileMax, Double.NaN);

    for (int i = 0; i < timestamps.size(); i++) {
      DataFrame window = this.makeWindow(res, timestamps.getLong(i));

      if (window.isEmpty()) {
        continue;
      }

      mean[i] = window.getDoubles(COL_VALUE).mean().doubleValue();

      if (window.size() >= 2) {
        std[i] = window.getDoubles(COL_VALUE).std().doubleValue();
      }

      if (!Double.isNaN(this.quantileMin)) {
        quantileMin[i] = window.getDoubles(COL_VALUE).quantile(this.quantileMin).doubleValue();
      }

      if (!Double.isNaN(this.quantileMax)) {
        quantileMax[i] = window.getDoubles(COL_VALUE).quantile(this.quantileMax).doubleValue();
      }
    }

    DataFrame output = new DataFrame(COL_TIME, timestamps)
        .addSeries(COL_STD, DoubleSeries.buildFrom(std))
        .addSeries(COL_MEAN, DoubleSeries.buildFrom(mean))
        .addSeries(COL_QUANTILE_MIN, DoubleSeries.buildFrom(quantileMin))
        .addSeries(COL_QUANTILE_MAX, DoubleSeries.buildFrom(quantileMax));

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
            .plus(new Period().withField(DurationFieldType.weeks(), 1)).getMillis();
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
        return values[0] >= tStart && values[0] < tCurrent && values[1] != 1;
      }
    }, COL_TIME, COL_OUTLIER).dropNull();
  }

  /**
   * Helper for parsing a period string from config (e.g. '3 days', '1min', '3600000')
   *
   * @param period
   * @return
   */
  static Period parsePeriod(String period) {
    Matcher m = PATTERN_PERIOD.matcher(period);
    if (!m.find()) {
      throw new IllegalArgumentException(String.format("Could not parse period expression '%s'", period));
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
