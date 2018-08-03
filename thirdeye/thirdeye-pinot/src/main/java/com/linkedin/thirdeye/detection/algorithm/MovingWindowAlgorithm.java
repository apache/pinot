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
import java.util.concurrent.TimeUnit;
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
  private static final String COL_KERNEL_SUM = "kernelSum";
  private static final String COL_VIOLATION = "violation";
  private static final String COL_ANOMALY = "anomaly";
  private static final String COL_OUTLIER = "outlier";
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
  private final double zscoreOutlier;
  private final double kernelMin;
  private final double kernelMax;
  private final double kernelSumMin;
  private final double kernelSumMax;
  private final double kernelDecay;
  private final int kernelSize;
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
    this.zscoreOutlier = MapUtils.getDoubleValue(config.getProperties(), "zscoreOutlier", 5);
    this.kernelMin = MapUtils.getDoubleValue(config.getProperties(), "kernelMin", Double.NaN);
    this.kernelMax = MapUtils.getDoubleValue(config.getProperties(), "kernelMax", Double.NaN);
    this.kernelSumMin = MapUtils.getDoubleValue(config.getProperties(), "kernelSumMin", Double.NaN);
    this.kernelSumMax = MapUtils.getDoubleValue(config.getProperties(), "kernelSumMax", Double.NaN);
    this.kernelDecay = MapUtils.getDoubleValue(config.getProperties(), "kernelDecay", 0.5);
    this.kernelSize = MapUtils.getIntValue(config.getProperties(), "kernelSize", 5);
    this.timezone = DateTimeZone.forID(MapUtils.getString(config.getProperties(), "timezone", "UTC"));
    this.windowSize = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), "windowSize", "1week"));
    this.minLookback = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), "minLookback", "1day"));
    this.changeDuration = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), "changeDuration", "5days"));

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
    double[] sKernelSum = df.addSeries(COL_KERNEL_SUM, DoubleSeries.zeros(df.size())).getDoubles(COL_KERNEL_SUM).values();
    byte[] sAnomaly = df.addSeries(COL_ANOMALY, BooleanSeries.fillValues(df.size(), false)).getBooleans(COL_ANOMALY).values();
    byte[] sOutlier = df.addSeries(COL_OUTLIER, BooleanSeries.fillValues(df.size(), false)).getBooleans(COL_OUTLIER).values();

    List<Long> timestamps = df.getLongs(COL_TIME).filter(df.getLongs(COL_TIME).between(this.effectiveStartTime, this.endTime)).dropNull().toList();
    for (long timestamp : timestamps) {

      // variable window size. manual iteration required.
      DataFrame window = this.makeWindow(df, timestamp, changePoints);
      if (window.size() <= 1) {
        continue;
      }

      // source index
      int index = df.getLongs(COL_TIME).find(timestamp);

      DoubleSeries values = window.getDoubles(COL_VALUE);
      double value = df.getDouble(COL_VALUE, index);
      double mean = values.mean().doubleValue();
      double std = values.std().doubleValue();
      double zscore = (value - mean) / std;

      sMean[index] = mean;
      sStd[index] = std;
      sZscore[index] = zscore;

      // outlier elimination for future windows
      if (!Double.isNaN(this.zscoreOutlier) && Math.abs(zscore) > this.zscoreOutlier) {
        sOutlier[index] = 1;
      }

      // point anomalies
      if (!Double.isNaN(this.quantileMin)) {
        sQuantileMin[index] = values.quantile(this.quantileMin).doubleValue();
        sAnomaly[index] |= (value < sQuantileMin[index] ? 1 : 0);
      }

      if (!Double.isNaN(this.quantileMax)) {
        sQuantileMax[index] = values.quantile(this.quantileMax).doubleValue();
        sAnomaly[index] |= (value > sQuantileMax[index] ? 1 : 0);
      }

      if (!Double.isNaN(this.zscoreMin)) {
        sAnomaly[index] |= (zscore < this.zscoreMin ? 1 : 0);
      }

      if (!Double.isNaN(this.zscoreMax)) {
        sAnomaly[index] |= (zscore > this.zscoreMax ? 1 : 0);
      }

      // range anomalies

      // kernel
      if (!Double.isNaN(this.kernelMin) || !Double.isNaN(this.kernelMax)
          || !Double.isNaN(this.kernelSumMin) || !Double.isNaN(this.kernelSumMax)) {
        final int kernelOffset = -1 * this.kernelSize / 2;

        DoubleSeries smooth = AlgorithmUtils.robustMean(df.getDoubles(COL_ZSCORE).sliceTo(index + 1), this.kernelSize).shift(kernelOffset);

        int kernelIndex = index + kernelOffset;
        if (index + kernelOffset >= 0) {
          double kernelValue = smooth.values()[kernelIndex];

          int prevKernelIndex = kernelIndex - 1;
          sKernelSum[kernelIndex] = (prevKernelIndex >= 0) ? sKernelSum[prevKernelIndex] * (1.0 - this.kernelDecay) : 0;
          sKernelSum[kernelIndex] += DoubleSeries.isNull(kernelValue) ? 0 : kernelValue;

          // approximate diagnostics only, may change at later execution
          sKernel[index + kernelOffset] = kernelValue;
        }

        DoubleSeries kernelSum = df.getDoubles(COL_KERNEL_SUM).fillNull();
        BooleanSeries smoothPadding = BooleanSeries.fillValues(df.size() - smooth.size(), false);

        if (!Double.isNaN(this.kernelMin)) {
          BooleanSeries violations = smooth.lt(this.kernelMin).append(smoothPadding);
          BooleanSeries partialViolations = smooth.lt(this.kernelMin / 3).append(smoothPadding);
          anomalyRangeHelper(df, violations, partialViolations);
        }

        if (!Double.isNaN(this.kernelMax)) {
          BooleanSeries violations = smooth.gt(this.kernelMax).append(smoothPadding);
          BooleanSeries partialViolations = smooth.gt(this.kernelMax / 3).append(smoothPadding);
          anomalyRangeHelper(df, violations, partialViolations);
        }

        if (!Double.isNaN(this.kernelSumMin)) {
          BooleanSeries violations = kernelSum.lt(this.kernelSumMin);
          BooleanSeries partialViolations = kernelSum.lt(this.kernelSumMin / 3);
          anomalyRangeHelper(df, violations, partialViolations);
        }

        if (!Double.isNaN(this.kernelSumMax)) {
          BooleanSeries violations = kernelSum.gt(this.kernelSumMax);
          BooleanSeries partialViolations = kernelSum.gt(this.kernelSumMax / 3);
          anomalyRangeHelper(df, violations, partialViolations);
        }

        sAnomaly = df.getBooleans(COL_ANOMALY).values();
      }

      // mark anomalies as outliers
      df.mapInPlace(BooleanSeries.HAS_TRUE, COL_OUTLIER, COL_OUTLIER, COL_ANOMALY);
      sOutlier = df.getBooleans(COL_OUTLIER).values();
    }

    List<MergedAnomalyResultDTO> anomalies = this.makeAnomalies(this.sliceDetection, df, COL_ANOMALY);

    Map<String, Object> diagnostics = new HashMap<>();
    diagnostics.put("data", df.dropAllNullColumns());
    diagnostics.put("changepoints", changePoints);

    return new DetectionPipelineResult(anomalies)
        .setDiagnostics(diagnostics);
  }

  /**
   * Helper for in-place insertion and expansion of anomaly ranges
   *
   * @param df data frame
   * @param violations boolean series of violations
   * @param partialViolations boolean series of partial violations for expansion
   * @return modified data frame
   */
  static DataFrame anomalyRangeHelper(DataFrame df, BooleanSeries violations, BooleanSeries partialViolations) {
    df.addSeries(COL_VIOLATION, expandViolation(violations, partialViolations).fillNull());
    df.mapInPlace(BooleanSeries.HAS_TRUE, COL_ANOMALY, COL_ANOMALY, COL_VIOLATION);
    df.dropSeries(COL_VIOLATION);
    return df;
  }

  /**
   * Expand violation ranges via the half-violation threshold.
   *
   * @param violation boolean series of violations
   * @param halfViolation boolean series of half violations
   * @return boolean series of expanded violations
   */
  static BooleanSeries expandViolation(BooleanSeries violation, BooleanSeries halfViolation) {
    if (violation.size() != halfViolation.size()) {
      throw new IllegalArgumentException("Series must be of equal size");
    }

    // TODO max lookback/forward range for performance

    byte[] full = violation.values();
    byte[] half = halfViolation.values();
    byte[] output = new byte[full.length];

    int lastHalf = -1;
    for (int i = 0; i < violation.size(); i++) {
      if (lastHalf >= 0 && BooleanSeries.isFalse(half[i])) {
        lastHalf = -1;
      }

      if (lastHalf < 0 && BooleanSeries.isTrue(half[i])) {
        lastHalf = i;
      }

      if (full[i] > 0) {
        // half[i] must be 1 here

        int j = lastHalf;

        for (; j < full.length && !BooleanSeries.isFalse(half[j]); j++) {
          if (!BooleanSeries.isNull(half[j])) {
            output[j] = 1;
          }
        }

        // move i to last checked candidate
        i = j - 1;
      }
    }

    return BooleanSeries.buildFrom(output);
  }

  /**
   * Populates the anomaly series with {@code true} values for a given collection of anomalies.
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

      final int kernelOffset = -1 * this.kernelSize / 2;

      DataFrame smooth = new DataFrame(diffSeries)
        .addSeries(COL_VALUE, AlgorithmUtils.robustMean(diffSeries.getDoubles(COL_VALUE), this.kernelSize).fillNullBackward().shift(kernelOffset));

      changePoints.addAll(AlgorithmUtils.getChangePoints(smooth, this.changeDuration.toStandardDuration()));
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

    // truncate history at change point but leave at least 1 week
    Long changePoint = changePoints.lower(tCurrent);
    if (changePoint != null) {
      // TODO make minimum configurable
      tStart = Math.min(Math.max(tStart, changePoint), tCurrent - TimeUnit.DAYS.toMillis(7));
    }

    // use non-outlier period, unless not enough history (anomalies are outliers too)
    // TODO make lookback configurable
    BooleanSeries filter = df.getBooleans(COL_OUTLIER).not().and(df.getLongs(COL_TIME).between(tStart, tCurrent));
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
