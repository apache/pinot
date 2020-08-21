/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.thirdeye.detection.components;

import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.commons.math3.analysis.MultivariateFunction;
import org.apache.commons.math3.optim.InitialGuess;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.MaxIter;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.SimpleBounds;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.nonlinear.scalar.ObjectiveFunction;
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.BOBYQAOptimizer;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.BooleanSeries;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.Pattern;
import org.apache.pinot.thirdeye.detection.algorithm.AlgorithmUtils;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.annotation.Param;
import org.apache.pinot.thirdeye.detection.spec.HoltWintersDetectorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyDetector;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionResult;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * Holt-Winters forecasting algorithm with multiplicative method
 * Supports seasonality and trend detection
 * https://otexts.com/fpp2/holt-winters.html
 */
@Components(title = "Holt Winters triple exponential smoothing forecasting and detection",
    type = "HOLT_WINTERS_RULE",
    tags = {DetectionTag.RULE_DETECTION},
    description = "Forecast with holt winters triple exponential smoothing and generate anomalies",
    params = {
        @Param(name = "alpha"),
        @Param(name = "beta"),
        @Param(name = "gamma"),
        @Param(name = "period"),
        @Param(name = "pattern"),
        @Param(name = "sensitivity"),
        @Param(name = "kernelSmoothing")})
public class HoltWintersDetector implements BaselineProvider<HoltWintersDetectorSpec>,
                                            AnomalyDetector<HoltWintersDetectorSpec> {
  private static final Logger LOG = LoggerFactory.getLogger(HoltWintersDetector.class);
  private InputDataFetcher dataFetcher;
  private static final String COL_CURR = "current";
  private static final String COL_ANOMALY = "anomaly";
  private static final String COL_PATTERN = "pattern";
  private static final String COL_DIFF = "diff";
  private static final String COL_DIFF_VIOLATION = "diff_violation";
  private static final String COL_ERROR = "error";
  private static final long KERNEL_PERIOD = 3600000L;
  private static final int LOOKBACK = 60;

  private int period;
  private double alpha;
  private double beta;
  private double gamma;
  private Pattern pattern;
  private double sensitivity;
  private boolean smoothing;
  private String monitoringGranularity;
  private TimeGranularity timeGranularity;
  private DayOfWeek weekStart;

  @Override
  public void init(HoltWintersDetectorSpec spec, InputDataFetcher dataFetcher) {
    this.period = spec.getPeriod();
    this.alpha = spec.getAlpha();
    this.beta = spec.getBeta();
    this.gamma = spec.getGamma();
    this.dataFetcher = dataFetcher;
    this.pattern = spec.getPattern();
    this.smoothing = spec.getSmoothing();
    this.sensitivity = spec.getSensitivity();
    this.monitoringGranularity = spec.getMonitoringGranularity();

    if (this.monitoringGranularity.endsWith(TimeGranularity.MONTHS) || this.monitoringGranularity.endsWith(TimeGranularity.WEEKS)) {
      this.timeGranularity = MetricSlice.NATIVE_GRANULARITY;
    } else {
      this.timeGranularity = TimeGranularity.fromString(this.monitoringGranularity);
    }
    if (this.monitoringGranularity.endsWith(TimeGranularity.WEEKS)) {
      this.weekStart = DayOfWeek.valueOf(spec.getWeekStart());
    }
  }

  @Override
  public TimeSeries computePredictedTimeSeries(MetricSlice slice) {
    MetricEntity metricEntity = MetricEntity.fromSlice(slice, 0);
    Interval window = new Interval(slice.getStart(), slice.getEnd());
    DateTime trainStart = getTrainingStartTime(window.getStart());

    DatasetConfigDTO datasetConfig = this.dataFetcher.fetchData(new InputDataSpec()
        .withMetricIdsForDataset(Collections.singleton(metricEntity.getId()))).getDatasetForMetricId()
        .get(metricEntity.getId());

    DataFrame inputDf = fetchData(metricEntity, trainStart.getMillis(), window.getEndMillis(), datasetConfig);
    DataFrame resultDF = computePredictionInterval(inputDf, window.getStartMillis(), datasetConfig.getTimezone());
    resultDF = resultDF.joinLeft(inputDf.renameSeries(COL_VALUE, COL_CURR), COL_TIME);

    // Exclude the end because baseline calculation should not contain the end
    if (resultDF.size() > 1) {
      resultDF = resultDF.head(resultDF.size() - 1);
    }

    return TimeSeries.fromDataFrame(resultDF);
  }

  private DateTime getTrainingStartTime(DateTime windowStart) {
    DateTime trainStart;
    if (isMultiDayGranularity()) {
      trainStart = windowStart.minusDays(timeGranularity.getSize() * LOOKBACK);
    } else if (this.monitoringGranularity.endsWith(TimeGranularity.MONTHS)) {
      trainStart = windowStart.minusMonths(LOOKBACK);
    } else if (this.monitoringGranularity.endsWith(TimeGranularity.WEEKS)) {
      trainStart = windowStart.minusWeeks(LOOKBACK);
    } else {
      trainStart = windowStart.minusDays(LOOKBACK);
    }
    return trainStart;
  }

  @Override
  public DetectionResult runDetection(Interval window, String metricUrn) {
    MetricEntity metricEntity = MetricEntity.fromURN(metricUrn);
    DateTime windowStart = window.getStart();
    // align start day to the user specified week start
    if (Objects.nonNull(this.weekStart)) {
      windowStart = window.getStart().withTimeAtStartOfDay().withDayOfWeek(weekStart.getValue()).minusWeeks(1);
    }

    DateTime trainStart = getTrainingStartTime(windowStart);

    DatasetConfigDTO datasetConfig = this.dataFetcher.fetchData(new InputDataSpec()
        .withMetricIdsForDataset(Collections.singleton(metricEntity.getId()))).getDatasetForMetricId()
        .get(metricEntity.getId());
    MetricSlice sliceData = MetricSlice.from(metricEntity.getId(), trainStart.getMillis(), window.getEndMillis(),
        metricEntity.getFilters());
    DataFrame dfInput = fetchData(metricEntity, trainStart.getMillis(), window.getEndMillis(), datasetConfig);

    // Kernel smoothing
    if (smoothing && !TimeUnit.DAYS.equals(datasetConfig.bucketTimeGranularity().getUnit())) {
      int kernelSize = (int) (KERNEL_PERIOD / datasetConfig.bucketTimeGranularity().toMillis());
      if (kernelSize > 1) {
        int kernelOffset = kernelSize / 2;
        double[] values = dfInput.getDoubles(COL_VALUE).values();
        for (int i = 0; i <= values.length - kernelSize; i++) {
          values[i + kernelOffset] = AlgorithmUtils.robustMean(dfInput.getDoubles(COL_VALUE)
              .slice(i, i + kernelSize), kernelSize).getDouble(kernelSize - 1);
        }
        dfInput.addSeries(COL_VALUE, values);
      }
    }

    DataFrame dfCurr = new DataFrame(dfInput).renameSeries(COL_VALUE, COL_CURR);
    DataFrame dfBase = computePredictionInterval(dfInput, windowStart.getMillis(), datasetConfig.getTimezone());
    DataFrame df = new DataFrame(dfCurr).addSeries(dfBase, COL_VALUE, COL_ERROR);
    df.addSeries(COL_DIFF, df.getDoubles(COL_CURR).subtract(df.get(COL_VALUE)));
    df.addSeries(COL_ANOMALY, BooleanSeries.fillValues(df.size(), false));

    // Filter pattern
    if (pattern.equals(Pattern.UP_OR_DOWN) ) {
      df.addSeries(COL_PATTERN, BooleanSeries.fillValues(df.size(), true));
    } else {
      df.addSeries(COL_PATTERN, pattern.equals(Pattern.UP) ? df.getDoubles(COL_DIFF).gt(0) :
          df.getDoubles(COL_DIFF).lt(0));
    }
    df.addSeries(COL_DIFF_VIOLATION, df.getDoubles(COL_DIFF).abs().gte(df.getDoubles(COL_ERROR)));
    df.mapInPlace(BooleanSeries.ALL_TRUE, COL_ANOMALY, COL_PATTERN, COL_DIFF_VIOLATION);

    // Anomalies
    List<MergedAnomalyResultDTO> anomalyResults =
        DetectionUtils.makeAnomalies(sliceData, df, COL_ANOMALY,
            DetectionUtils.getMonitoringGranularityPeriod(this.monitoringGranularity, datasetConfig), datasetConfig);
    dfBase = dfBase.joinRight(df.retainSeries(COL_TIME, COL_CURR), COL_TIME);
    return DetectionResult.from(anomalyResults, TimeSeries.fromDataFrame(dfBase));
  }

  /**
   * Fetch data from metric
   *
   * @param metricEntity metric entity
   * @param start start timestamp
   * @param end end timestamp
   * @param datasetConfig the dataset config
   * @return Data Frame that has data from start to end
   */
  private DataFrame fetchData(MetricEntity metricEntity, long start, long end, DatasetConfigDTO datasetConfig) {

    List<MetricSlice> slices = new ArrayList<>();
    MetricSlice sliceData = MetricSlice.from(metricEntity.getId(), start, end,
        metricEntity.getFilters(), timeGranularity);
    slices.add(sliceData);
    LOG.info("Getting data for" + sliceData.toString());
    InputData data = this.dataFetcher.fetchData(new InputDataSpec().withTimeseriesSlices(slices)
        .withMetricIdsForDataset(Collections.singletonList(metricEntity.getId()))
        .withMetricIds(Collections.singletonList(metricEntity.getId())));
    MetricConfigDTO metricConfig = data.getMetrics().get(metricEntity.getId());
    DataFrame df = data.getTimeseries().get(sliceData);

    // aggregate data to specified weekly granularity
    if (this.monitoringGranularity.endsWith(TimeGranularity.WEEKS)) {
      Period monitoringGranularityPeriod =
          DetectionUtils.getMonitoringGranularityPeriod(this.monitoringGranularity, datasetConfig);
      long latestDataTimeStamp = df.getLong(COL_TIME, df.size() - 1);
      df = DetectionUtils.aggregateByPeriod(df, new DateTime(start, DateTimeZone.forID(datasetConfig.getTimezone())),
          monitoringGranularityPeriod, metricConfig.getDefaultAggFunction());
      df = DetectionUtils.filterIncompleteAggregation(df, latestDataTimeStamp, datasetConfig.bucketTimeGranularity(),
          monitoringGranularityPeriod);
    }
    return df;
  }

  /**
   * Returns a data frame containing lookback number of data before prediction time
   * @param originalDF the original dataframe
   * @param time the prediction time, in unix timestamp
   * @return DataFrame containing lookback number of data
   */
  private DataFrame getLookbackDF(DataFrame originalDF, Long time) {
    LongSeries longSeries = (LongSeries) originalDF.get(COL_TIME);
    int indexFinish = longSeries.find(time);
    DataFrame df = DataFrame.builder(COL_TIME, COL_VALUE).build();

    if (indexFinish != -1) {
      int indexStart = Math.max(0, indexFinish - LOOKBACK);
      df = df.append(originalDF.slice(indexStart, indexFinish));
    }
    return df;
  }

  /**
   * Returns a data frame containing the same time daily data, based on input time
   * @param originalDF the original dataframe
   * @param time the prediction time, in unix timestamp
   * @return DataFrame containing same time of daily data for LOOKBACK number of days
   */
  private DataFrame getDailyDF(DataFrame originalDF, Long time, String timezone) {
    LongSeries longSeries = (LongSeries) originalDF.get(COL_TIME);
    long start = longSeries.getLong(0);
    DateTime dt = new DateTime(time).withZone(DateTimeZone.forID(timezone));
    DataFrame df = DataFrame.builder(COL_TIME, COL_VALUE).build();

    for (int i = 0; i < LOOKBACK; i++) {
      DateTime subDt = dt.minusDays(1);
      long t = subDt.getMillis();
      if (t < start) {
        break;
      }
      int index = longSeries.find(t);
      if (index != -1) {
        df = df.append(originalDF.slice(index, index + 1));
      } else {
        int backtrackCounter = 0;
        // If the 1 day look back data doesn't exist, use the data one period before till backtrackCounter greater than 4
        while (index == -1 && backtrackCounter <= 4) {
          subDt = subDt.minusDays(period);
          long timestamp = subDt.getMillis();
          index = longSeries.find(timestamp);
          backtrackCounter++;
        }

        if (index != -1) {
          df = df.append(originalDF.slice(index, index + 1));
        } else {
          // If not found value up to 4 weeks, insert the last value
          double lastVal = (originalDF.get(COL_VALUE)).getDouble(longSeries.find(dt.getMillis()));
          DateTime nextDt = dt.minusDays(1);
          DataFrame appendDf = DataFrame.builder(COL_TIME, COL_VALUE).append(nextDt, lastVal).build();
          df = df.append(appendDf);
        }
      }
      dt = dt.minusDays(1);
    }
    df = df.reverse();
    return df;
  }

  private static double calculateInitialLevel(double[] y) {
    return y[0];
  }

  /**
   * See: http://www.itl.nist.gov/div898/handbook/pmc/section4/pmc435.htm
   *
   * @return - Initial trend - Bt[1]
   */
  private static double calculateInitialTrend(double[] y, int period) {
    double sum = 0;

    for (int i = 0; i < period; i++) {
      sum += y[period + i] - y[i];
    }

    return sum / (period * period);
  }

  /**
   * See: http://www.itl.nist.gov/div898/handbook/pmc/section4/pmc435.htm
   *
   * @return - Seasonal Indices.
   */
  private static double[] calculateSeasonalIndices(double[] y, int period,
      int seasons) {
    double[] seasonalMean = new double[seasons];
    double[] seasonalIndices = new double[period];

    double[] averagedObservations = new double[y.length];

    for (int i = 0; i < seasons; i++) {
      for (int j = 0; j < period; j++) {
        seasonalMean[i] += y[(i * period) + j];
      }
      seasonalMean[i] /= period;
    }

    for (int i = 0; i < seasons; i++) {
      for (int j = 0; j < period; j++) {
        averagedObservations[(i * period) + j] = y[(i * period) + j]
            / seasonalMean[i];
      }
    }

    for (int i = 0; i < period; i++) {
      for (int j = 0; j < seasons; j++) {
        seasonalIndices[i] += averagedObservations[(j * period) + i];
      }
      seasonalIndices[i] /= seasons;
    }

    return seasonalIndices;
  }

  /**
   * Holt Winters forecasting method
   *
   * @param y Timeseries to be forecasted
   * @param alpha level smoothing factor
   * @param beta trend smoothing factor
   * @param gamma seasonality smoothing factor
   * @return ForecastResults containing predicted value, SSE(sum of squared error) and error bound
   */
  private ForecastResults forecast(double[] y, double alpha, double beta, double gamma) {
    double[] seasonal = new double[y.length+1];
    double[] forecast = new double[y.length+1];

    double a0 = calculateInitialLevel(y);
    double b0 = calculateInitialTrend(y, period);

    int seasons = y.length / period;
    double[] initialSeasonalIndices = calculateSeasonalIndices(y, period,
        seasons);

    for (int i = 0; i < period; i++) {
      seasonal[i] = initialSeasonalIndices[i];
    }

    // s is level and t is trend
    double s = a0;
    double t = b0;
    double predictedValue = 0;

    for (int i = 0; i < y.length; i++) {
      double sNew;
      double tNew;
      forecast[i] = (s + t) * seasonal[i];
      sNew = alpha * (y[i] / seasonal[i]) + (1 - alpha) * (s + t);
      tNew = beta * (sNew - s) + (1 - beta) * t;
      if (i + period <= y.length) {
        seasonal[i + period] = gamma * (y[i] / (sNew * seasonal[i])) + (1 - gamma) * seasonal[i];
      }
      s = sNew;
      t = tNew;
      if (i == y.length - 1) {
        predictedValue = (s + t) * seasonal[i+1];
      }
    }

    List<Double> diff = new ArrayList<>();
    double sse = 0;
    for (int i = 0; i < y.length; i++) {
      if (forecast[i] != 0) {
        sse += Math.pow(y[i] - forecast[i], 2);
        diff.add(forecast[i] - y[i]);
      }
    }

    double error = calculateErrorBound(diff, sensitivityToZscore(sensitivity));
    return new ForecastResults(predictedValue, sse, error);
  }

  /**
   * Compute the baseline and error bound for given data
   *
   * @param inputDF training dataframe
   * @param windowStartTime prediction start time
   * @return DataFrame with timestamp, baseline, error bound
   */
  private DataFrame computePredictionInterval(DataFrame inputDF, long windowStartTime, String timezone) {

    DataFrame resultDF = new DataFrame();
    DataFrame forecastDF = inputDF.filter(new Series.LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] >= windowStartTime;
      }
    }, COL_TIME).dropNull();

    int size = forecastDF.size();
    double[] baselineArray = new double[size];
    double[] upperBoundArray = new double[size];
    double[] lowerBoundArray = new double[size];
    long[] resultTimeArray = new long[size];
    double[] errorArray = new double[size];

    double lastAlpha = alpha;
    double lastBeta = beta;
    double lastGamma = gamma;

    for (int k = 0; k < size; k++) {
      DataFrame trainingDF;
      if (timeGranularity.equals(MetricSlice.NATIVE_GRANULARITY) && !this.monitoringGranularity.endsWith(
          TimeGranularity.MONTHS) && !this.monitoringGranularity.endsWith(TimeGranularity.WEEKS)) {
        trainingDF = getDailyDF(inputDF, forecastDF.getLong(COL_TIME, k), timezone);
      } else {
        trainingDF = getLookbackDF(inputDF, forecastDF.getLong(COL_TIME, k));
      }

      // We need at least 2 periods of data
      if (trainingDF.size() < 2 * period) {
        continue;
      }

      resultTimeArray[k] = forecastDF.getLong(COL_TIME, k);

      double[] y = trainingDF.getDoubles(COL_VALUE).values();
      HoltWintersParams params;
      if (alpha < 0 && beta < 0 && gamma < 0) {
        params = fitModelWithBOBYQA(y, lastAlpha, lastBeta, lastGamma);
      } else {
        params = new HoltWintersParams(alpha, beta, gamma);
      }

      lastAlpha = params.getAlpha();
      lastBeta = params.getBeta();
      lastGamma = params.getGamma();

      ForecastResults result = forecast(y, params.getAlpha(), params.getBeta(), params.getGamma());
      double predicted = result.getPredictedValue();
      double error = result.getErrorBound();

      baselineArray[k] = predicted;
      errorArray[k] = error;
      upperBoundArray[k] = predicted + error;
      lowerBoundArray[k] = predicted - error;
    }

    resultDF.addSeries(COL_TIME, LongSeries.buildFrom(resultTimeArray)).setIndex(COL_TIME);
    resultDF.addSeries(COL_VALUE, DoubleSeries.buildFrom(baselineArray));
    resultDF.addSeries(COL_UPPER_BOUND, DoubleSeries.buildFrom(upperBoundArray));
    resultDF.addSeries(COL_LOWER_BOUND, DoubleSeries.buildFrom(lowerBoundArray));
    resultDF.addSeries(COL_ERROR, DoubleSeries.buildFrom(errorArray));
    return resultDF;
  }

  /**
   * Returns the error bound of given list based on mean, std and given zscore
   *
   * @param givenNumbers double list
   * @param zscore zscore used to multiply by std
   * @return the error bound
   */
  private static double calculateErrorBound(List<Double> givenNumbers, double zscore) {
    // calculate the mean value (= average)
    double sum = 0.0;
    for (double num : givenNumbers) {
      sum += num;
    }
    double mean = sum / givenNumbers.size();

    // calculate standard deviation
    double squaredDifferenceSum = 0.0;
    for (double num : givenNumbers) {
      squaredDifferenceSum += (num - mean) * (num - mean);
    }
    double variance = squaredDifferenceSum / givenNumbers.size();
    double standardDeviation = Math.sqrt(variance);

    return zscore * standardDeviation;
  }

  /**
   * Fit alpha, beta, gamma by optimizing SSE (Sum of squared errors) using BOBYQA
   * It is a derivative free bound constrained optimization algorithm
   * https://en.wikipedia.org/wiki/BOBYQA
   *
   * @param y the data
   * @param lastAlpha last alpha value
   * @param lastBeta last beta value
   * @param lastGamma last gamma value
   * @return double array containing fitted alpha, beta and gamma
   */
  private HoltWintersParams fitModelWithBOBYQA(double[] y, double lastAlpha, double lastBeta, double lastGamma) {
    BOBYQAOptimizer optimizer = new BOBYQAOptimizer(7);
    if (lastAlpha < 0) {
      lastAlpha = 0.1;
    }
    if (lastBeta < 0) {
      lastBeta = 0.01;
    }
    if (lastGamma < 0) {
      lastGamma = 0.001;
    }
    InitialGuess initGuess = new InitialGuess(new double[]{lastAlpha, lastBeta, lastGamma});;
    MaxIter maxIter = new MaxIter(30000);
    MaxEval maxEval  = new MaxEval(30000);
    GoalType goal = GoalType.MINIMIZE;
    ObjectiveFunction objectiveFunction = new ObjectiveFunction(new MultivariateFunction() {
      public double value(double[] params) {
        return forecast(y, params[0], params[1], params[2]).getSSE();
      }
    });
    SimpleBounds bounds = new SimpleBounds(new double[]{0.001, 0.001, 0.001}, new double[]{0.999, 0.999, 0.999});

    HoltWintersParams params;
    try {
      PointValuePair optimal = optimizer.optimize(objectiveFunction, goal, bounds, initGuess, maxIter, maxEval);
      params = new HoltWintersParams(optimal.getPoint()[0], optimal.getPoint()[1], optimal.getPoint()[2]);
    } catch (Exception e) {
      LOG.error(e.toString());
      params = new HoltWintersParams(lastAlpha, lastBeta, lastGamma);
    }
    return params;
  }

  /**
   * Mapping of sensitivity to zscore on range of 1 - 3
   * @param sensitivity double from 0 to 10
   * @return zscore
   */
  private static double sensitivityToZscore(double sensitivity) {
    // If out of bound, use boundary sensitivity
    if (sensitivity < 0) {
      sensitivity = 0;
    } else if (sensitivity > 10) {
      sensitivity = 10;
    }
    double z = 1 + 0.2 * (10 - sensitivity);
    return z;
  }

  // Check whether monitoring timeGranularity is multiple days
  private boolean isMultiDayGranularity() {
    return !timeGranularity.equals(MetricSlice.NATIVE_GRANULARITY) && timeGranularity.getUnit() == TimeUnit.DAYS;
  }

  /**
   * Container class to store holt winters parameters
   */
  final static class HoltWintersParams {
    private final double alpha;
    private final double beta;
    private final double gamma;

    HoltWintersParams(double alpha, double beta, double gamma) {
      this.alpha = alpha;
      this.beta = beta;
      this.gamma = gamma;
    }

    double getAlpha() {
      return alpha;
    }

    double getBeta() {
      return beta;
    }

    double getGamma() {
      return gamma;
    }
  }

  /**
   * Container class to store forecasting results
   */
  final static class ForecastResults {
    private final double predictedValue;
    private final double SSE;
    private final double errorBound;

    ForecastResults(double predictedValue, double SSE, double errorBound) {
      this.predictedValue = predictedValue;
      this.SSE = SSE;
      this.errorBound = errorBound;
    }

    double getPredictedValue() {
      return predictedValue;
    }

    double getSSE() {
      return SSE;
    }

    double getErrorBound() {
      return errorBound;
    }
  }
}