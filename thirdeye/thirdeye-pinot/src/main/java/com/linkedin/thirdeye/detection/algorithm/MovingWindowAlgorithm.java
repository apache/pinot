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
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregate;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private static final Pattern PATTERN_PERIOD = Pattern.compile("([0-9]+)\\s*(\\S*)");

  private final MetricSlice sliceData;
  private final MetricSlice sliceBaseline;
  private final MetricSlice sliceDetection;
  private final Period lookback;
  private final double zscoreMin;
  private final double zscoreMax;
  private final double quantileMin;
  private final double quantileMax;
  private final Baseline baseline;
  private final DateTimeZone timezone;
  private final Period outlierDuration;
  private final Period changeDuration;

  public MovingWindowAlgorithm(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    Preconditions.checkArgument(config.getProperties().containsKey(PROP_METRIC_URN));

    String metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    MetricEntity me = MetricEntity.fromURN(metricUrn, 1.0);

    this.quantileMin = MapUtils.getDoubleValue(config.getProperties(), "quantileMin", Double.NaN);
    this.quantileMax = MapUtils.getDoubleValue(config.getProperties(), "quantileMax", Double.NaN);
    this.zscoreMin = MapUtils.getDoubleValue(config.getProperties(), "zscoreMin", Double.NaN);
    this.zscoreMax = MapUtils.getDoubleValue(config.getProperties(), "zscoreMax", Double.NaN);
    this.timezone = DateTimeZone.forID(MapUtils.getString(config.getProperties(), "timezone", "UTC"));
    this.lookback = parsePeriod(MapUtils.getString(config.getProperties(), "lookback", "1week"));
    this.outlierDuration = parsePeriod(MapUtils.getString(config.getProperties(), "outlierDuration", "0"));
    this.changeDuration = parsePeriod(MapUtils.getString(config.getProperties(), "changeDuration", "0"));

    int baselineWeeks = MapUtils.getIntValue(config.getProperties(), "baselineWeeks", 0);
    BaselineAggregateType baselineType = BaselineAggregateType.valueOf(MapUtils.getString(config.getProperties(), "baselineType", "SUM"));

    if (baselineWeeks <= 0) {
      this.baseline = null;
    } else {
      this.baseline = BaselineAggregate.fromWeekOverWeek(baselineType, baselineWeeks, 1, this.timezone);
    }

    Preconditions.checkArgument(Double.isNaN(this.quantileMin) || (this.quantileMin >= 0 && this.quantileMin <= 1.0), "quantileMin must be between 0.0 and 1.0");
    Preconditions.checkArgument(Double.isNaN(this.quantileMax) || (this.quantileMax >= 0 && this.quantileMax <= 1.0), "quantileMax must be between 0.0 and 1.0");

    DateTime trainStart = new DateTime(startTime, this.timezone)
        .minus(this.lookback);

    DateTime dataStart = trainStart.minus(new Period().withField(DurationFieldType.weeks(), baselineWeeks));

    this.sliceData = MetricSlice.from(me.getId(), dataStart.getMillis(), endTime, me.getFilters());
    this.sliceBaseline = MetricSlice.from(me.getId(), trainStart.getMillis(), endTime, me.getFilters());
    this.sliceDetection = MetricSlice.from(me.getId(), startTime, endTime, me.getFilters());
  }

  @Override
  public StaticDetectionPipelineModel getModel() {
    return new StaticDetectionPipelineModel()
        .withTimeseriesSlices(Collections.singleton(this.sliceData));
  }

  @Override
  public DetectionPipelineResult run(StaticDetectionPipelineData data) throws Exception {
    DataFrame df = data.getTimeseries().get(this.sliceData);

    if (this.baseline != null) {
      df = this.makeTimeseries(df);
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

    List<MergedAnomalyResultDTO> anomalies = this.makeAnomalies(this.sliceDetection, df, COL_ANOMALY);

    long maxTime = -1;
    if (!df.isEmpty()) {
      maxTime = df.getLongs(COL_TIME).max().longValue();
    }

    return new DetectionPipelineResult(anomalies, maxTime);
  }

  /**
   * Returns data frame augmented with moving window statistics (std, mean, quantile)
   *
   * @param df data
   * @return data augmented with moving window statistics
   */
  DataFrame applyMovingWindow(DataFrame df) {
    DataFrame res = new DataFrame(df);

    LongSeries timestamps = res.getLongs(COL_TIME).filter(res.getLongs(COL_TIME).gte(this.startTime)).dropNull();

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
  DataFrame makeTimeseries(DataFrame df) {
    Collection<MetricSlice> slices = this.baseline.scatter(this.sliceBaseline);
    Map<MetricSlice, DataFrame> timeseriesMap = new HashMap<>();
    for (MetricSlice slice : slices) {
      timeseriesMap.put(slice, sliceTimeseries(df, slice));
    }

    // TODO handle change points gracefully

    DataFrame dfCurr = new DataFrame(df).renameSeries(COL_VALUE, COL_CURR);
    DataFrame dfBase = this.baseline.gather(this.sliceBaseline, timeseriesMap).renameSeries(COL_VALUE, COL_BASE);

    DataFrame joined = new DataFrame(dfCurr);
    joined.addSeries(dfBase, COL_BASE);
    joined.addSeries(COL_VALUE, joined.getDoubles(COL_CURR).subtract(joined.get(COL_BASE)));

    return joined.dropNull();
  }

  /**
   * Helper slices base time series for given metric time slice
   *
   * @param df time series dataframe
   * @param slice metric slice
   * @return time series for given slice (range)
   */
  static DataFrame sliceTimeseries(DataFrame df, MetricSlice slice) {
    return df.filter(df.getLongs(COL_TIME).gte(slice.getStart()).and(df.getLongs(COL_TIME).lt(slice.getEnd()))).dropNull(COL_TIME);
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
