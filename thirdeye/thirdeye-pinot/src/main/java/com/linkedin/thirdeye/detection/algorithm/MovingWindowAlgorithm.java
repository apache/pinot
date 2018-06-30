package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.dataframe.BooleanSeries;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.AnomalySlice;
import com.linkedin.thirdeye.detection.ConfigUtils;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationFieldType;
import org.joda.time.Period;


public class MovingWindowAlgorithm extends StaticDetectionPipeline {
  private static final String COL_CURR = "current";
  private static final String COL_BASE = "baseline";
  private static final String COL_STD = "std";
  private static final String COL_MEAN = "mean";
  private static final String COL_QUANTILE_MIN = "quantileMin";
  private static final String COL_QUANTILE_MAX = "quantileMax";
  private static final String COL_ZSCORE = "zscore";
  private static final String COL_KERNEL = "kernel";
  private static final String COL_KERNEL_VIOLATION = "kernelViolation";
  private static final String COL_ANOMALY = "anomaly";
  private static final String COL_TIME = DataFrameUtils.COL_TIME;
  private static final String COL_VALUE = DataFrameUtils.COL_VALUE;

  private static final String PROP_METRIC_URN = "metricUrn";

  private final MetricSlice sliceData;
  private final MetricSlice sliceDetection;
  private final AnomalySlice anomalySlice;

  private final Period windowSize;
  private final Period minLookback;
  private final double zscoreMin;
  private final double zscoreMax;
  private final double kernelMin;
  private final double kernelMax;
  private final int kernelPasses;
  private final double quantileMin;
  private final double quantileMax;
  private final Baseline baseline;
  private final DateTimeZone timezone;
  private final Period changeDuration;

  private final long effectiveStartTime;

  public MovingWindowAlgorithm(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    Preconditions.checkArgument(config.getProperties().containsKey(PROP_METRIC_URN));

    String metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    MetricEntity me = MetricEntity.fromURN(metricUrn, 1.0);

    this.quantileMin = MapUtils.getDoubleValue(config.getProperties(), "quantileMin", Double.NaN);
    this.quantileMax = MapUtils.getDoubleValue(config.getProperties(), "quantileMax", Double.NaN);
    this.zscoreMin = MapUtils.getDoubleValue(config.getProperties(), "zscoreMin", Double.NaN);
    this.zscoreMax = MapUtils.getDoubleValue(config.getProperties(), "zscoreMax", Double.NaN);
    this.kernelMin = MapUtils.getDoubleValue(config.getProperties(), "kernelMin", Double.NaN);
    this.kernelMax = MapUtils.getDoubleValue(config.getProperties(), "kernelMax", Double.NaN);
    this.kernelPasses = MapUtils.getIntValue(config.getProperties(), "kernelPasses", 4);
    this.timezone = DateTimeZone.forID(MapUtils.getString(config.getProperties(), "timezone", "UTC"));
    this.windowSize = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), "windowSize", "1week"));
    this.minLookback = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), "minLookback", "1day"));
    this.changeDuration = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), "changeDuration", "0"));

    int baselineWeeks = MapUtils.getIntValue(config.getProperties(), "baselineWeeks", 0);
    BaselineAggregateType baselineType = BaselineAggregateType.valueOf(MapUtils.getString(config.getProperties(), "baselineType", "MEDIAN"));

    if (baselineWeeks <= 0) {
      this.baseline = null;
    } else {
      this.baseline = BaselineAggregate.fromWeekOverWeek(baselineType, baselineWeeks, 1, this.timezone);
    }

    Preconditions.checkArgument(Double.isNaN(this.quantileMin) || (this.quantileMin >= 0 && this.quantileMin <= 1.0), "quantileMin must be between 0.0 and 1.0");
    Preconditions.checkArgument(Double.isNaN(this.quantileMax) || (this.quantileMax >= 0 && this.quantileMax <= 1.0), "quantileMax must be between 0.0 and 1.0");

    long effectiveStartTime = startTime;
    if (endTime - startTime < this.minLookback.toStandardDuration().getMillis()) {
      effectiveStartTime = endTime - this.minLookback.toStandardDuration().getMillis();
    }
    this.effectiveStartTime = effectiveStartTime;

    DateTime trainStart = new DateTime(effectiveStartTime, this.timezone).minus(this.windowSize);
    DateTime dataStart = trainStart.minus(new Period().withField(DurationFieldType.weeks(), baselineWeeks));

    this.sliceData = MetricSlice.from(me.getId(), dataStart.getMillis(), endTime, me.getFilters());
    this.sliceDetection = MetricSlice.from(me.getId(), effectiveStartTime, endTime, me.getFilters());

    this.anomalySlice = new AnomalySlice()
        .withConfigId(this.config.getId())
        .withStart(this.sliceData.getStart())
        .withEnd(this.sliceData.getEnd());
  }

  @Override
  public StaticDetectionPipelineModel getModel() {
    StaticDetectionPipelineModel model = new StaticDetectionPipelineModel()
        .withTimeseriesSlices(Collections.singleton(this.sliceData));

    if (this.config.getId() != null) {
      model = model.withAnomalySlices(Collections.singleton(this.anomalySlice));
    }

    return model;
  }

  @Override
  public DetectionPipelineResult run(StaticDetectionPipelineData data) throws Exception {
    DataFrame df = data.getTimeseries().get(this.sliceData);

    Collection<MergedAnomalyResultDTO> existingAnomalies = data.getAnomalies().get(this.anomalySlice);

    // change point rescaling
    TreeSet<Long> changePoints = getChangePoints(df, existingAnomalies);
    df = AlgorithmUtils.getRescaledSeries(df, changePoints);

    // relative baseline
    if (this.baseline != null) {
      df = makeRelativeTimeseries(df, this.baseline, this.sliceData);
    }

    // populate pre-existing anomalies
    df = applyExistingAnomalies(df, existingAnomalies);

    // write-through arrays
    double[] sMean = df.addSeries(COL_MEAN, DoubleSeries.nulls(df.size())).getDoubles(COL_MEAN).values();
    double[] sStd = df.addSeries(COL_STD, DoubleSeries.nulls(df.size())).getDoubles(COL_STD).values();
    double[] sZscore = df.addSeries(COL_ZSCORE, DoubleSeries.nulls(df.size())).getDoubles(COL_ZSCORE).values();
    double[] sQuantileMin = df.addSeries(COL_QUANTILE_MIN, DoubleSeries.nulls(df.size())).getDoubles(COL_QUANTILE_MIN).values();
    double[] sQuantileMax = df.addSeries(COL_QUANTILE_MAX, DoubleSeries.nulls(df.size())).getDoubles(COL_QUANTILE_MAX).values();
    double[] sKernel = df.addSeries(COL_KERNEL, DoubleSeries.nulls(df.size())).getDoubles(COL_KERNEL).values();
    byte[] sAnomaly = df.addSeries(COL_ANOMALY, BooleanSeries.fillValues(df.size(), false)).getBooleans(COL_ANOMALY).values();

    List<Long> timestamps = df.getLongs(COL_TIME).filter(df.getLongs(COL_TIME).between(this.effectiveStartTime, this.endTime)).dropNull().toList();
    for (long timestamp : timestamps) {

      // variable window size. manual iteration required.
      DataFrame window = this.makeWindow(df, timestamp, changePoints);
      if (window.size() <= 1) {
        continue;
      }

      // source index
      int index = df.getLongs(COL_TIME).find(timestamp);
      boolean anomaly = false;

      DoubleSeries values = window.getDoubles(COL_VALUE);
      double value = df.getDouble(COL_VALUE, index);
      double mean = values.mean().doubleValue();
      double std = values.std().doubleValue();
      double zscore = (value - mean) / std;

      sMean[index] = mean;
      sStd[index] = std;
      sZscore[index] = zscore;

      if (!Double.isNaN(this.quantileMin)) {
        sQuantileMin[index] = values.quantile(this.quantileMin).doubleValue();
        anomaly |= value < sQuantileMin[index];
      }

      if (!Double.isNaN(this.quantileMax)) {
        sQuantileMax[index] = values.quantile(this.quantileMax).doubleValue();
        anomaly |= value > sQuantileMax[index];
      }

      if (!Double.isNaN(this.zscoreMin)) {
        anomaly |= zscore < this.zscoreMin;
      }

      if (!Double.isNaN(this.zscoreMax)) {
        anomaly |= zscore > this.zscoreMax;
      }

      // single anomaly
      if (anomaly) {
        sAnomaly[index] = 1;
      }

      // batch anomalies

      // kernel check
      if (!Double.isNaN(this.kernelMin) || !Double.isNaN(this.kernelMax)) {
        DoubleSeries smooth = AlgorithmUtils.fastBSpline(df.getDoubles(COL_ZSCORE), this.kernelPasses);

        // approximation only, may change retroactively
        sKernel[index] = smooth.values()[index];

        if (!Double.isNaN(this.kernelMin)) {
          df.addSeries(COL_KERNEL_VIOLATION, smooth.lt(this.kernelMin).fillNull());
          df.mapInPlace(BooleanSeries.HAS_TRUE, COL_ANOMALY, COL_ANOMALY, COL_KERNEL_VIOLATION);

          sAnomaly = df.getBooleans(COL_ANOMALY).values();
          df.dropSeries(COL_KERNEL_VIOLATION);
        }

        if (!Double.isNaN(this.kernelMax)) {
          df.addSeries(COL_KERNEL_VIOLATION, smooth.gt(this.kernelMax).fillNull());
          df.mapInPlace(BooleanSeries.HAS_TRUE, COL_ANOMALY, COL_ANOMALY, COL_KERNEL_VIOLATION);

          sAnomaly = df.getBooleans(COL_ANOMALY).values();
          df.dropSeries(COL_KERNEL_VIOLATION);
        }
      }
    }

    List<MergedAnomalyResultDTO> anomalies = this.makeAnomalies(this.sliceDetection, df, COL_ANOMALY);

    Map<String, Object> diagnostics = new HashMap<>();
    diagnostics.put("data", df.dropAllNullColumns());
    diagnostics.put("changepoints", changePoints);

    return new DetectionPipelineResult(anomalies)
        .setDiagnostics(diagnostics);
  }

  /**
   * Populates the anomaly series with {@code true} values for a given colleciton of anomalies.
   *
   * @param df data frame
   * @param anomalies pre-existing anomalies
   * @return anomaly populated data frame
   */
  DataFrame applyExistingAnomalies(DataFrame df, Collection<MergedAnomalyResultDTO> anomalies) {
    DataFrame res = new DataFrame(df);

    for (MergedAnomalyResultDTO anomaly : anomalies) {
      res.set(COL_ANOMALY, res.getLongs(COL_TIME).between(anomaly.getStartTime(), anomaly.getEndTime()), BooleanSeries.fillValues(df.size(), true));
    }

    return res;
  }

  /**
   * Returns a data with values differentiated week-over-week.
   *
   * @param df data
   * @return data with differentiated values
   */
  static DataFrame makeRelativeTimeseries(DataFrame df, Baseline baseline, MetricSlice baseSlice) {
    Collection<MetricSlice> slices = baseline.scatter(baseSlice);
    Map<MetricSlice, DataFrame> timeseriesMap = new HashMap<>();
    for (MetricSlice slice : slices) {
      timeseriesMap.put(slice, sliceTimeseries(df, slice));
    }

    DataFrame dfCurr = new DataFrame(df).renameSeries(COL_VALUE, COL_CURR);
    DataFrame dfBase = baseline.gather(baseSlice, timeseriesMap).renameSeries(COL_VALUE, COL_BASE);

    DataFrame joined = new DataFrame(dfCurr);
    joined.addSeries(dfBase, COL_BASE);
    joined.addSeries(COL_VALUE, joined.getDoubles(COL_CURR).subtract(joined.get(COL_BASE)));

    return joined.dropNull();
  }

  /**
   * Returns a merged set of change points computed from time series and user-labeled anomalies.
   *
   * @param df data
   * @return set of change points
   */
  TreeSet<Long> getChangePoints(DataFrame df, Collection<MergedAnomalyResultDTO> anomalies) {
    TreeSet<Long> changePoints = new TreeSet<>();

    // from time series
    if (this.changeDuration.toStandardDuration().getMillis() > 0) {
      // TODO configurable seasonality
      Baseline baseline = BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.SUM, 1, 1, this.timezone);
      DataFrame diffSeries = makeRelativeTimeseries(df, baseline, this.sliceData);

      changePoints.addAll(AlgorithmUtils.getChangePointsTercileMethod(diffSeries, this.changeDuration.toStandardDuration()));
    }

    // from anomalies
    Collection<MergedAnomalyResultDTO> changePointAnomalies = Collections2.filter(anomalies,
        new Predicate<MergedAnomalyResultDTO>() {
          @Override
          public boolean apply(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
            return mergedAnomalyResultDTO != null
                && mergedAnomalyResultDTO.getFeedback() != null
                && AnomalyFeedbackType.ANOMALY_NEW_TREND.equals(mergedAnomalyResultDTO.getFeedback().getFeedbackType());
          }
        });

    for (MergedAnomalyResultDTO anomaly : changePointAnomalies) {
      changePoints.add(anomaly.getStartTime());
    }

    return changePoints;
  }

  /**
   * Helper slices base time series for given metric time slice
   *
   * @param df time series dataframe
   * @param slice metric slice
   * @return time series for given slice (range)
   */
  static DataFrame sliceTimeseries(DataFrame df, MetricSlice slice) {
    return df.filter(df.getLongs(COL_TIME).between(slice.getStart(), slice.getEnd())).dropNull(COL_TIME);
  }

  /**
   * Returns variable-size look back window for given timestamp.
   *
   * @param df data
   * @param tCurrent end timestamp (exclusive)
   * @return window data frame
   */
  DataFrame makeWindow(DataFrame df, long tCurrent, TreeSet<Long> changePoints) {
    long tStart = new DateTime(tCurrent, this.timezone).minus(this.windowSize).getMillis();

    // truncate history at change point
    Long changePoint = changePoints.lower(tCurrent);
    if (changePoint != null) {
      tStart = Math.max(tStart, changePoint);
    }

    // use non-anomaly period, unless not enough history
    BooleanSeries filter = df.getBooleans(COL_ANOMALY).not().and(df.getLongs(COL_TIME).between(tStart, tCurrent));
    if (filter.sum().longValue() <= 24) {
      filter = BooleanSeries.fillValues(filter.size(), true);
    }

    return df.filter(df.getLongs(COL_TIME).between(tStart, tCurrent).and(filter)).dropNull(COL_TIME, COL_VALUE);
  }

  /**
   * Filters series names by existing series in data frame {@code df} only.
   *
   * @param df data frame
   * @param seriesNames series names
   * @return array of existing series names
   */
  static String[] existingOnly(DataFrame df, String... seriesNames) {
    Set<String> names = new HashSet<>(Arrays.asList(seriesNames));
    names.retainAll(df.getSeriesNames());
    return names.toArray(new String[names.size()]);
  }
}
