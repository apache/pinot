/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.dataframe.BooleanSeries;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.spi.model.AnomalySlice;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.StaticDetectionPipeline;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregate;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationFieldType;
import org.joda.time.Period;


/**
 * Exponential smoothing for baseline generation. Detects anomalies via normalized
 * zscore or quantile rules from rolling look-back window. Supports basic noise
 * suppression, outlier elimination and change point detection.
 */
public class MovingWindowAlgorithm extends StaticDetectionPipeline {
  private static final String COL_CURR = "currentValue";
  private static final String COL_BASE = "baselineValue";
  private static final String COL_STD = "std";
  private static final String COL_MEAN = "mean";
  private static final String COL_QUANTILE_MIN = "quantileMin";
  private static final String COL_QUANTILE_MAX = "quantileMax";
  private static final String COL_ZSCORE = "zscore";
  private static final String COL_VIOLATION = "violation";
  private static final String COL_ANOMALY = "anomaly";
  private static final String COL_NOT_ANOMALY = "notAnomaly";
  private static final String COL_OUTLIER = "outlier";
  private static final String COL_TIME = DataFrameUtils.COL_TIME;
  private static final String COL_VALUE = DataFrameUtils.COL_VALUE;
  private static final String COL_COMPUTED_VALUE = "computedValue";
  private static final String COL_WINDOW_SIZE = "windowSize";
  private static final String COL_BASELINE = "baseline";

  private static final String COL_WEIGHT = "weight";
  private static final String COL_WEIGHTED_VALUE = "weightedValue";
  private static final String COL_WEIGHTED_MEAN = "weightedMena";
  private static final String COL_WEIGHTED_STD = "weightedStd";

  private static final String PROP_METRIC_URN = "metricUrn";

  private final MetricSlice sliceData;
  private final MetricSlice sliceDetection;
  private final AnomalySlice anomalySlice;

  private final Period windowSize;
  private final Period lookbackPeriod;
  private final Period reworkPeriod;
  private final double zscoreMin;
  private final double zscoreMax;
  private final double zscoreOutlier;
  private final int kernelSize;
  private final double quantileMin;
  private final double quantileMax;
  private final DateTimeZone timezone;
  private final Period changeDuration;
  private final double changeFraction;
  private final int baselineWeeks;
  private final boolean applyLog;
  private final double learningRate;

  private final long effectiveStartTime;

  public MovingWindowAlgorithm(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    Preconditions.checkArgument(config.getProperties().containsKey(PROP_METRIC_URN));

    String metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    MetricEntity me = MetricEntity.fromURN(metricUrn);

    this.quantileMin = MapUtils.getDoubleValue(config.getProperties(), "quantileMin", Double.NaN);
    this.quantileMax = MapUtils.getDoubleValue(config.getProperties(), "quantileMax", Double.NaN);
    this.zscoreMin = MapUtils.getDoubleValue(config.getProperties(), "zscoreMin", Double.NaN);
    this.zscoreMax = MapUtils.getDoubleValue(config.getProperties(), "zscoreMax", Double.NaN);
    this.zscoreOutlier = MapUtils.getDoubleValue(config.getProperties(), "zscoreOutlier", Double.NaN);
    this.kernelSize = MapUtils.getIntValue(config.getProperties(), "kernelSize", 1);
    this.timezone = DateTimeZone.forID(MapUtils.getString(config.getProperties(), "timezone", "UTC"));
    this.windowSize = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), "windowSize", "1week"));
    this.lookbackPeriod = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), "lookbackPeriod", "1week"));
    this.reworkPeriod = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), "reworkPeriod", "1day"));
    this.changeDuration = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), "changeDuration", "5days"));
    this.changeFraction = MapUtils.getDoubleValue(config.getProperties(), "changeFraction", 0.666);
    this.baselineWeeks = MapUtils.getIntValue(config.getProperties(), "baselineWeeks", 0);
    this.applyLog = MapUtils.getBooleanValue(config.getProperties(), "applyLog", true);
    this.learningRate = MapUtils.getDoubleValue(config.getProperties(), "learningRate", 0.666);

    Preconditions.checkArgument(Double.isNaN(this.quantileMin) || (this.quantileMin >= 0 && this.quantileMin <= 1.0), "quantileMin must be between 0.0 and 1.0");
    Preconditions.checkArgument(Double.isNaN(this.quantileMax) || (this.quantileMax >= 0 && this.quantileMax <= 1.0), "quantileMax must be between 0.0 and 1.0");

    this.effectiveStartTime = new DateTime(startTime, this.timezone).minus(this.lookbackPeriod).getMillis();

    DateTime trainStart = new DateTime(this.effectiveStartTime, this.timezone).minus(this.windowSize);
    DateTime dataStart = trainStart.minus(new Period().withField(DurationFieldType.weeks(), baselineWeeks));
    DateTime detectionStart = new DateTime(startTime, this.timezone).minus(this.reworkPeriod);

    this.sliceData = MetricSlice.from(me.getId(), dataStart.getMillis(), endTime, me.getFilters());
    this.sliceDetection = MetricSlice.from(me.getId(), detectionStart.getMillis(), endTime, me.getFilters());

    this.anomalySlice = new AnomalySlice()
        .withStart(this.sliceData.getStart())
        .withEnd(this.sliceData.getEnd());
  }

  @Override
  public InputDataSpec getInputDataSpec() {
    InputDataSpec dataSpec = new InputDataSpec()
        .withTimeseriesSlices(Collections.singleton(this.sliceData));

    if (this.config.getId() != null) {
      dataSpec = dataSpec.withAnomalySlices(Collections.singleton(this.anomalySlice));
    }

    return dataSpec;
  }

  @Override
  public DetectionPipelineResult run(InputData data) throws Exception {
    DataFrame dfInput = data.getTimeseries().get(this.sliceData);

    // log transform
    if (this.applyLog) {
      if (dfInput.getDoubles(COL_VALUE).lt(0).hasTrue()) {
        throw new IllegalArgumentException("Cannot apply log to series with negative values");
      }
      dfInput.addSeries(COL_VALUE, dfInput.getDoubles(COL_VALUE).add(1).log());
    }

    // kernel smoothing
    if (this.kernelSize > 1) {
      final int kernelOffset = this.kernelSize / 2;
      double[] values = dfInput.getDoubles(COL_VALUE).values();
      for (int i = 0; i < values.length - this.kernelSize + 1; i++) {
        values[i + kernelOffset] = AlgorithmUtils.robustMean(dfInput.getDoubles(COL_VALUE).slice(i, i + this.kernelSize), this.kernelSize).getDouble(this.kernelSize - 1);
      }
      dfInput.addSeries(COL_VALUE, values);
    }

    Collection<MergedAnomalyResultDTO> existingAnomalies = data.getAnomalies().get(this.anomalySlice);

    // pre-detection change points
    TreeSet<Long> changePoints = getChangePoints(dfInput, this.effectiveStartTime, existingAnomalies);

    // write-through arrays
    dfInput.addSeries(COL_BASELINE, DoubleSeries.nulls(dfInput.size()));
    dfInput.addSeries(COL_MEAN, DoubleSeries.nulls(dfInput.size()));
    dfInput.addSeries(COL_STD, DoubleSeries.nulls(dfInput.size()));
    dfInput.addSeries(COL_ZSCORE, DoubleSeries.nulls(dfInput.size()));
    dfInput.addSeries(COL_QUANTILE_MIN, DoubleSeries.nulls(dfInput.size()));
    dfInput.addSeries(COL_QUANTILE_MAX, DoubleSeries.nulls(dfInput.size()));
    dfInput.addSeries(COL_WINDOW_SIZE, LongSeries.nulls(dfInput.size()));
    dfInput.addSeries(COL_COMPUTED_VALUE, DoubleSeries.nulls(dfInput.size()));
    dfInput.addSeries(COL_OUTLIER, BooleanSeries.fillValues(dfInput.size(), false));

    // populate pre-existing anomalies (but don't mark as outliers yet)
    dfInput = applyExistingAnomalies(dfInput, existingAnomalies);

    // populate pre-computed values
    long[] sTimestamp = dfInput.getLongs(COL_TIME).values();
    double[] sComputed = dfInput.getDoubles(COL_COMPUTED_VALUE).values();
    for (int i = 0; i < sTimestamp.length && sTimestamp[i] < this.effectiveStartTime; i++) {
      double baseline = 0;
      if (this.baselineWeeks > 0) {
        baseline = this.makeBaseline(dfInput, sTimestamp[i], changePoints);
      }
      sComputed[i] = dfInput.getDouble(COL_VALUE, i) - baseline;
    }

    // estimate outliers (anomaly and non-anomaly outliers)
    // https://en.m.wikipedia.org/wiki/Median_absolute_deviation
    if (!Double.isNaN(this.zscoreOutlier)) {
      DataFrame dfPrefix = dfInput.filter(dfInput.getLongs(COL_TIME).lt(this.effectiveStartTime)).dropNull(COL_TIME, COL_COMPUTED_VALUE);
      DoubleSeries prefix = dfPrefix.getDoubles(COL_COMPUTED_VALUE);
      DoubleSeries mad = prefix.subtract(prefix.median()).abs().median();
      if (!mad.isNull(0) && mad.getDouble(0) > 0.0) {
        double std = 1.4826 * mad.doubleValue();
        double mean = AlgorithmUtils.robustMean(prefix, prefix.size()).getDouble(prefix.size() - 1);
        DoubleSeries zscoreAbs = prefix.subtract(mean).divide(std).abs();
        BooleanSeries fullOutlier = zscoreAbs.gt(this.zscoreOutlier);
        BooleanSeries partialOutlier = zscoreAbs.gt(this.zscoreOutlier / 2);

        dfPrefix.addSeries(COL_OUTLIER, expandViolation(fullOutlier, partialOutlier));
        dfInput.addSeries(dfPrefix, COL_OUTLIER);
        dfInput = dfInput.fillNull(COL_OUTLIER);
      }
    }

    // finally apply existing anomalies as outliers
    dfInput.addSeries(COL_OUTLIER, dfInput.getBooleans(COL_OUTLIER)
        .or(dfInput.getBooleans(COL_ANOMALY))
        .and(dfInput.getBooleans(COL_NOT_ANOMALY).not()));

    // generate detection time series
    Result result = this.run(dfInput, this.effectiveStartTime, changePoints);

    List<MergedAnomalyResultDTO> anomalies = this.makeAnomalies(this.sliceDetection, result.data, COL_ANOMALY);

    Map<String, Object> diagnostics = new HashMap<>();
    diagnostics.put(DetectionPipelineResult.DIAGNOSTICS_DATA, result.data.dropAllNullColumns());
    diagnostics.put(DetectionPipelineResult.DIAGNOSTICS_CHANGE_POINTS, result.changePoints);

    return new DetectionPipelineResult(anomalies)
        .setDiagnostics(diagnostics);
  }

  /**
   * Run anomaly detection from a given start timestamp
   *
   * @param df raw input data
   * @param start start time stamp
   * @param changePoints set of change points
   * @return detection result
   * @throws Exception
   */
  private Result run(DataFrame df, long start, TreeSet<Long> changePoints) throws Exception {

    // write-through arrays
    double[] sBaseline = df.getDoubles(COL_BASELINE).values();
    double[] sMean = df.getDoubles(COL_MEAN).values();
    double[] sStd = df.getDoubles(COL_STD).values();
    double[] sZscore = df.getDoubles(COL_ZSCORE).values();
    double[] sQuantileMin = df.getDoubles(COL_QUANTILE_MIN).values();
    double[] sQuantileMax = df.getDoubles(COL_QUANTILE_MAX).values();
    double[] sComputed = df.getDoubles(COL_COMPUTED_VALUE).values();
    byte[] sAnomaly = df.getBooleans(COL_ANOMALY).values();
    byte[] sOutlier = df.getBooleans(COL_OUTLIER).values();
    long[] sWindowSize = df.getLongs(COL_WINDOW_SIZE).values();

    // scan
    List<Long> timestamps = df.getLongs(COL_TIME).filter(df.getLongs(COL_TIME).between(start, this.endTime)).dropNull().toList();
    for (long timestamp : timestamps) {

      //
      // test for intra-detection change points
      //
      long fractionRangeStart = new DateTime(timestamp, this.timezone).minus(this.changeDuration).getMillis();
      DataFrame changePointWindow = df.filter(df.getLongs(COL_TIME).between(fractionRangeStart, timestamp)).dropNull(COL_TIME);

      Long latestChangePoint = changePoints.floor(timestamp);
      long minChangePoint = latestChangePoint == null ? fractionRangeStart : new DateTime(latestChangePoint, this.timezone).plus(this.changeDuration).getMillis();

      long fractionChangePoint = extractAnomalyFractionChangePoint(changePointWindow, this.changeFraction);

      // TODO prevent change point from anomalies labeled as outliers (but not new trend) by user

      if (fractionChangePoint >= 0 && fractionChangePoint >= minChangePoint) {
        TreeSet<Long> changePointsNew = new TreeSet<>(changePoints);
        changePointsNew.add(fractionChangePoint);
        return this.run(df, timestamp, changePointsNew);
      }

      // source index
      int index = df.getLongs(COL_TIME).find(timestamp);

      //
      // computed values
      //
      double value = df.getDouble(COL_VALUE, index);

      double baseline = 0;
      if (this.baselineWeeks > 0) {
        baseline = this.makeBaseline(df, timestamp, changePoints);
      }
      sBaseline[index] = baseline;

      double computed = value - baseline;
      sComputed[index] = computed;

      //
      // variable look back window
      //
      DataFrame window = this.makeWindow(df, timestamp, changePoints);
      sWindowSize[index] = window.size();

      if (window.size() <= 1) {
        continue;
      }

      //
      // quantiles
      //
      DoubleSeries computedValues = window.getDoubles(COL_COMPUTED_VALUE);

      if (!Double.isNaN(this.quantileMin) || !Double.isNaN(this.quantileMax)) {
        if (!Double.isNaN(this.quantileMin)) {
          sQuantileMin[index] = computedValues.quantile(this.quantileMin).doubleValue();
          sAnomaly[index] |= (computed < sQuantileMin[index] ? 1 : 0);
        }

        if (!Double.isNaN(this.quantileMax)) {
          sQuantileMax[index] = computedValues.quantile(this.quantileMax).doubleValue();
          sAnomaly[index] |= (computed > sQuantileMax[index] ? 1 : 0);
        }
      }

      //
      // zscore
      //
      if (!Double.isNaN(this.zscoreOutlier) || !Double.isNaN(this.zscoreMin) || !Double.isNaN(this.zscoreMax)) {
        DataFrame windowStats = this.makeWindowStats(window, timestamp);

        if (!windowStats.isEmpty()) {
          double mean = windowStats.getDouble(COL_MEAN, 0);
          double std = windowStats.getDouble(COL_STD, 0);
          double zscore = (computed - mean) / std;

          sMean[index] = mean;
          sStd[index] = std;
          sZscore[index] = zscore;

          // outlier elimination
          if (!Double.isNaN(this.zscoreOutlier) && Math.abs(zscore) > this.zscoreOutlier) {
            sOutlier[index] = 1;
          }

          BooleanSeries partialViolation = BooleanSeries.fillValues(df.size(), false);

          if (!Double.isNaN(this.zscoreMin) && zscore < this.zscoreMin) {
            sAnomaly[index] |= 1;
            partialViolation = partialViolation.or(df.getDoubles(COL_ZSCORE).lt(this.zscoreMin / 2).fillNull());
          }

          if (!Double.isNaN(this.zscoreMax) && zscore > this.zscoreMax) {
            sAnomaly[index] |= 1;
            partialViolation = partialViolation.or(df.getDoubles(COL_ZSCORE).gt(this.zscoreMax / 2).fillNull());
          }

          // anomaly region expansion
          if (partialViolation.hasTrue()) {
            partialViolation = partialViolation.or(df.getBooleans(COL_ANOMALY).fillNull());
            sAnomaly = anomalyRangeHelper(df, df.getBooleans(COL_ANOMALY), partialViolation).getBooleans(COL_ANOMALY).values();
          }
        }
      }

      //
      // house keeping
      //

      // mark anomalies as outliers (except regions marked as NOT_ANOMALY)
      sOutlier = df.mapInPlace(BooleanSeries.HAS_TRUE, COL_OUTLIER, COL_ANOMALY, COL_OUTLIER)
          .getBooleans(COL_OUTLIER).and(df.getBooleans(COL_NOT_ANOMALY).not()).values();
    }

    return new Result(df, changePoints);
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
   * Expand violation ranges via the partial-violation threshold.
   *
   * @param violation boolean series of violations
   * @param partialViolation boolean series of partial violations
   * @return boolean series of expanded violations
   */
  static BooleanSeries expandViolation(BooleanSeries violation, BooleanSeries partialViolation) {
    if (violation.size() != partialViolation.size()) {
      throw new IllegalArgumentException("Series must be of equal size");
    }

    byte[] full = violation.values();
    byte[] partial = partialViolation.values();
    byte[] output = new byte[full.length];

    int lastPartial = -1;
    for (int i = 0; i < violation.size(); i++) {
      if (BooleanSeries.isFalse(partial[i])) {
        lastPartial = -1;
      }

      if (lastPartial < 0 && BooleanSeries.isTrue(partial[i])) {
        lastPartial = i;
      }

      if (full[i] > 0) {
        lastPartial = lastPartial >= 0 ? lastPartial : i;

        int j;

        for (j = lastPartial; j < full.length && !BooleanSeries.isFalse(partial[j]); j++) {
          if (BooleanSeries.isTrue(full[j]) || BooleanSeries.isTrue(partial[j])) {
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
   * Find change points within window via anomaly fraction
   *
   * @param window data frame
   * @param fraction anomaly range fraction of total window
   * @return fraction of anomaly period compared to overall period
   */
  static long extractAnomalyFractionChangePoint(DataFrame window, double fraction) {
    long[] timestamp = window.getLongs(COL_TIME).values();
    byte[] anomaly = window.getBooleans(COL_ANOMALY).values();

    int max = window.get(COL_TIME).count();
    int count = 0;
    for (int i = window.size() - 1; i >= 0; i--) {
      if (!LongSeries.isNull(timestamp[i]) && !BooleanSeries.isNull(anomaly[i])) {
        count += BooleanSeries.isTrue(anomaly[i]) ? 1 : 0;
      }

      if (count / (double) max >= fraction) {
        return timestamp[i];
      }
    }

    return -1;
  }

  /**
   * Populates the anomaly series with {@code true} values for a given collection of anomalies.
   *
   * @param df data frame
   * @param anomalies pre-existing anomalies
   * @return anomaly populated data frame
   */
  DataFrame applyExistingAnomalies(DataFrame df, Collection<MergedAnomalyResultDTO> anomalies) {
    DataFrame res = new DataFrame(df)
        .addSeries(COL_ANOMALY, BooleanSeries.fillValues(df.size(), false))
        .addSeries(COL_NOT_ANOMALY, BooleanSeries.fillValues(df.size(), false));

    BooleanSeries trainingRange = res.getLongs(COL_TIME).lt(this.effectiveStartTime);

    for (MergedAnomalyResultDTO anomaly : anomalies) {
      // NOTE: NEW_TREND not labeled as anomaly
      // NOTE: NO_FEEDBACK not labeled as anomaly

      if (anomaly.getFeedback() != null && anomaly.getFeedback().getFeedbackType().isAnomaly()
          && !anomaly.getFeedback().getFeedbackType().equals(AnomalyFeedbackType.ANOMALY_NEW_TREND)) {
        res.set(COL_ANOMALY, res.getLongs(COL_TIME).between(anomaly.getStartTime(), anomaly.getEndTime()).and(trainingRange), BooleanSeries.fillValues(df.size(), true));
      }

      if (anomaly.getFeedback() != null && anomaly.getFeedback().getFeedbackType().isNotAnomaly()) {
        res.set(COL_NOT_ANOMALY, res.getLongs(COL_TIME).between(anomaly.getStartTime(), anomaly.getEndTime()).and(trainingRange), BooleanSeries.fillValues(df.size(), true));
      }
    }

    return res;
  }

  /**
   * Returns a dataframe with values differentiated vs a baseline. Prefers values after change-point if available
   *
   * @param df data (COL_TIME, COL_VALUE, COL_OUTLIER)
   * @param baseline baseline config
   * @param baseSlice base metric slice
   * @return data with differentiated values
   */
  static DataFrame diffTimeseries(DataFrame df, Baseline baseline, MetricSlice baseSlice) {
    Collection<MetricSlice> slices = baseline.scatter(baseSlice);

    Map<MetricSlice, DataFrame> map = new HashMap<>();

    for (MetricSlice slice : slices) {
      map.put(slice, sliceTimeseries(df, slice));
    }

    DataFrame dfCurr = new DataFrame(df).renameSeries(COL_VALUE, COL_CURR);
    DataFrame dfBase = baseline.gather(baseSlice, map).renameSeries(COL_VALUE, COL_BASE);
    DataFrame joined = new DataFrame(dfCurr).addSeries(dfBase, COL_BASE);
    joined.addSeries(COL_VALUE, joined.getDoubles(COL_CURR).subtract(joined.get(COL_BASE)));

    return joined;
  }

  /**
   * Returns a merged set of change points computed from time series and user-labeled anomalies.
   *
   * @param df data
   * @return set of change points
   */
  TreeSet<Long> getChangePoints(DataFrame df, long start, Collection<MergedAnomalyResultDTO> anomalies) {
    TreeSet<Long> changePoints = new TreeSet<>();

    // from time series
    if (this.changeDuration.toStandardDuration().getMillis() > 0) {
      // TODO configurable seasonality
      DataFrame dfChangePoint = new DataFrame(df).addSeries(COL_OUTLIER, BooleanSeries.fillValues(df.size(), false));
      Baseline baseline = BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.SUM, 1, 1, this.timezone);
      DataFrame diffSeries = diffTimeseries(dfChangePoint, baseline, this.sliceData).dropNull(COL_TIME, COL_VALUE);

      // less than or equal to start only
      changePoints.addAll(AlgorithmUtils.getChangePointsRobustMean(diffSeries, this.kernelSize, this.changeDuration.toStandardDuration()).headSet(start, true));
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
   * @param changePoints change points
   * @return window data frame
   */
  DataFrame makeWindow(DataFrame df, long tCurrent, TreeSet<Long> changePoints) {
    DateTime now = new DateTime(tCurrent);
    long tStart = now.minus(this.windowSize).getMillis();

    // truncate history at change point but leave at least a window equal to changeDuration
    Long changePoint = changePoints.lower(tCurrent);
    if (changePoint != null) {
      tStart = Math.max(tStart, changePoint);
    }

    // use non-outlier period, unless not enough history (anomalies are outliers too)
    BooleanSeries timeFilter = df.getLongs(COL_TIME).between(tStart, tCurrent);
    BooleanSeries outlierAndTimeFilter = df.getBooleans(COL_OUTLIER).not().and(timeFilter);

    // TODO make threshold for fallback to outlier period configurable
    if (outlierAndTimeFilter.sum().fillNull().longValue() <= timeFilter.sum().fillNull().longValue() / 3) {
      return df.filter(timeFilter).dropNull(COL_TIME, COL_COMPUTED_VALUE);
    }

    return df.filter(outlierAndTimeFilter).dropNull(COL_TIME, COL_COMPUTED_VALUE);
  }

  /**
   * Returns window stats (mean, std) using exponential smoothing for weekly stats.
   *
   * @param window time series window (truncated at change points)
   * @param tCurrent current time stamp
   * @return widnow stats with exp smoothing
   */
  // TODO use exp smoothing on raw values directly rather than week-over-week based. requires learning rate adjustment
  DataFrame makeWindowStats(DataFrame window, long tCurrent) {
    DateTime now = new DateTime(tCurrent, this.timezone);
    int windowSizeWeeks = this.windowSize.toStandardWeeks().getWeeks();

    // construct baseline
    DataFrame raw = new DataFrame(COL_TIME, LongSeries.nulls(windowSizeWeeks))
        .addSeries(COL_MEAN, DoubleSeries.nulls(windowSizeWeeks))
        .addSeries(COL_STD, DoubleSeries.nulls(windowSizeWeeks))
        .addSeries(COL_WEIGHT, DoubleSeries.nulls(windowSizeWeeks));

    long[] sTimestamp = raw.getLongs(COL_TIME).values();
    double[] sMean = raw.getDoubles(COL_MEAN).values();
    double[] sStd = raw.getDoubles(COL_STD).values();
    double[] sWeight = raw.getDoubles(COL_WEIGHT).values();

    for (int i = 0; i < windowSizeWeeks; i++) {
      int offset = windowSizeWeeks - i;
      long tFrom = now.minus(new Period().withWeeks(offset)).getMillis();
      long tTo = now.getMillis();

      DoubleSeries dfWeek = window.filter(window.getLongs(COL_TIME).between(tFrom, tTo))
          .dropNull(COL_TIME, COL_COMPUTED_VALUE)
          .getDoubles(COL_COMPUTED_VALUE);

      DoubleSeries mean = dfWeek.mean();
      DoubleSeries std = dfWeek.std();

      if (mean.hasNull() || std.hasNull()) {
        continue;
      }

      sTimestamp[i] = tFrom; // not used
      sMean[i] = mean.doubleValue();
      sStd[i] = Math.pow(std.doubleValue(), 2); // use variance for estimate
      sWeight[i] = Math.pow(this.learningRate, offset - 1);
    }

    DataFrame data = raw.dropNull();
    data.addSeries(COL_WEIGHTED_MEAN, data.getDoubles(COL_MEAN).multiply(data.get(COL_WEIGHT)));
    data.addSeries(COL_WEIGHTED_STD, data.getDoubles(COL_STD).multiply(data.get(COL_WEIGHT)));

    DoubleSeries totalWeight = data.getDoubles(COL_WEIGHT).sum();
    if (totalWeight.hasNull()) {
      return new DataFrame();
    }

    DataFrame out = new DataFrame();
    out.addSeries(COL_MEAN, data.getDoubles(COL_WEIGHTED_MEAN).sum().divide(totalWeight));
    out.addSeries(COL_STD, data.getDoubles(COL_WEIGHTED_STD).sum().divide(totalWeight).pow(0.5)); // back to std

    return out;
  }

  /**
   * Helper generates baseline value for timestamp via exponential smoothing
   *
   * @param df time series data
   * @param tCurrent current time stamp
   * @param changePoints set of change points
   * @return baseline value for time stamp
   */
  double makeBaseline(DataFrame df, long tCurrent, TreeSet<Long> changePoints) {
    DateTime now = new DateTime(tCurrent);

    int index = df.getLongs(COL_TIME).find(tCurrent);
    if (index < 0) {
      return Double.NaN;
    }

    if (this.baselineWeeks <= 0) {
      return 0.0;
    }

    Long lastChangePoint = changePoints.floor(tCurrent);

    // construct baseline
    DataFrame raw = new DataFrame(COL_TIME, LongSeries.nulls(this.baselineWeeks))
        .addSeries(COL_VALUE, DoubleSeries.nulls(this.baselineWeeks))
        .addSeries(COL_WEIGHT, DoubleSeries.nulls(this.baselineWeeks));

    long[] sTimestamp = raw.getLongs(COL_TIME).values();
    double[] sValue = raw.getDoubles(COL_VALUE).values();
    double[] sWeight = raw.getDoubles(COL_WEIGHT).values();

    // exponential smoothing with weekly seasonality
    for (int i = 0; i < this.baselineWeeks; i++) {
      int offset = this.baselineWeeks - i;
      long timestamp = now.minus(new Period().withWeeks(offset)).getMillis();
      sTimestamp[i] = timestamp;

      int valueIndex = df.getLongs(COL_TIME).find(timestamp);
      if (valueIndex >= 0) {
        sValue[i] = df.getDouble(COL_VALUE, valueIndex);
        sWeight[i] = Math.pow(this.learningRate, offset - 1);

        // adjust for change point
        if (lastChangePoint != null && timestamp < lastChangePoint) {
          sWeight[i] *= 0.001;
        }

        // adjust for outlier
        if (BooleanSeries.isTrue(df.getBoolean(COL_OUTLIER, valueIndex))) {
          sWeight[i] *= 0.001;
        }
      }
    }

    DataFrame data = raw.dropNull();
    data.addSeries(COL_WEIGHTED_VALUE, data.getDoubles(COL_VALUE).multiply(data.get(COL_WEIGHT)));

    DoubleSeries totalWeight = data.getDoubles(COL_WEIGHT).sum();
    if (totalWeight.hasNull()) {
      return Double.NaN;
    }

    DoubleSeries computed = data.getDoubles(COL_WEIGHTED_VALUE).sum().divide(totalWeight);

    if (computed.hasNull()) {
      return Double.NaN;
    }

    return computed.doubleValue();
  }

  /**
   * Container class for detection result
   */
  final class Result {
    final DataFrame data;
    final TreeSet<Long> changePoints;

    public Result(DataFrame data, TreeSet<Long> changePoints) {
      this.data = data;
      this.changePoints = changePoints;
    }
  }
}
