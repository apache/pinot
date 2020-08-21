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

import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.BooleanSeries;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.Pattern;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.annotation.Param;
import org.apache.pinot.thirdeye.detection.spec.MeanVarianceRuleDetectorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyDetector;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionResult;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.pinot.thirdeye.dataframe.DoubleSeries.*;
import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;

@Components(title = "History mean and standard deviation based forecasting and detection.",
    type = "MEAN_VARIANCE_RULE",
    tags = {DetectionTag.RULE_DETECTION},
    description = "Forecast using history mean and standard deviation.",
    params = {
        @Param(name = "offset", defaultValue = "wo1w"),
        @Param(name = "pattern", allowableValues = {"up", "down"}),
        @Param(name = "lookback"),
        @Param(name = "sensitivity")})
public class MeanVarianceRuleDetector implements AnomalyDetector<MeanVarianceRuleDetectorSpec>,
                                                 BaselineProvider<MeanVarianceRuleDetectorSpec> {
  private static final Logger LOG = LoggerFactory.getLogger(MeanVarianceRuleDetector.class);
  private InputDataFetcher dataFetcher;
  private Pattern pattern;
  private String monitoringGranularity;
  private TimeGranularity timeGranularity;
  private double sensitivity;
  private int lookback;

  private static final String COL_CURR = "current";
  private static final String COL_ANOMALY = "anomaly";
  private static final String COL_PATTERN = "pattern";
  private static final String COL_DIFF = "diff";
  private static final String COL_DIFF_VIOLATION = "diff_violation";
  private static final String COL_ERROR = "error";
  private static final String COL_CHANGE = "change";

  @Override
  public void init(MeanVarianceRuleDetectorSpec spec, InputDataFetcher dataFetcher) {
    this.dataFetcher = dataFetcher;
    this.pattern = spec.getPattern();
    this.lookback = spec.getLookback();
    this.sensitivity = spec.getSensitivity();
    this.monitoringGranularity = spec.getMonitoringGranularity();

    if (this.monitoringGranularity.equals("1_MONTHS")) {
      this.timeGranularity = MetricSlice.NATIVE_GRANULARITY;
    } else {
      this.timeGranularity = TimeGranularity.fromString(spec.getMonitoringGranularity());
    }

    //Lookback spec validation
    //Minimum lookback set to 9. That's 8 change data points.
    if (this.lookback < 9) {
      throw new IllegalArgumentException(String.format("Lookback of %d is too small. Please increase to greater than 9.", this.lookback));
    }

  }

  @Override
  public TimeSeries computePredictedTimeSeries(MetricSlice slice) {
    MetricEntity metricEntity = MetricEntity.fromSlice(slice, 0);
    Interval window = new Interval(slice.getStart(), slice.getEnd());
    DateTime trainStart;

    if (isMultiDayGranularity()) {
      trainStart = window.getStart().minusDays(timeGranularity.getSize() * lookback);
    } else if (this.monitoringGranularity.equals("1_MONTHS")) {
      trainStart = window.getStart().minusMonths(lookback);
    } else {
      trainStart = window.getStart().minusWeeks(lookback);
    }

    DatasetConfigDTO datasetConfig = this.dataFetcher.fetchData(new InputDataSpec()
        .withMetricIdsForDataset(Collections.singleton(metricEntity.getId()))).getDatasetForMetricId()
        .get(metricEntity.getId());
    DataFrame inputDf = fetchData(metricEntity, trainStart.getMillis(), window.getEndMillis());
    DataFrame resultDF = computePredictionInterval(inputDf, window.getStartMillis(), datasetConfig.getTimezone());
    resultDF = resultDF.joinLeft(inputDf.renameSeries(COL_VALUE, COL_CURR), COL_TIME);

    // Exclude the end because baseline calculation should not contain the end
    if (resultDF.size() > 1) {
      resultDF = resultDF.head(resultDF.size() - 1);
    }

    return TimeSeries.fromDataFrame(resultDF);
  }

  @Override
  public DetectionResult runDetection(Interval window, String metricUrn) {
    MetricEntity me = MetricEntity.fromURN(metricUrn);
    DateTime fetchStart;
    //get historical data
    if (isMultiDayGranularity()) {
      fetchStart = window.getStart().minusDays(timeGranularity.getSize() * lookback);
    } else if (this.monitoringGranularity.equals("1_MONTHS")) {
      fetchStart = window.getStart().minusMonths(lookback);
    } else {
      fetchStart = window.getStart().minusWeeks(lookback);
    }

    MetricSlice slice = MetricSlice.from(me.getId(), fetchStart.getMillis(), window.getEndMillis(), me.getFilters(), timeGranularity);
    DatasetConfigDTO datasetConfig = this.dataFetcher.fetchData(new InputDataSpec()
        .withMetricIdsForDataset(Collections.singleton(me.getId()))).getDatasetForMetricId()
        .get(me.getId());
    // getting data (window + earliest lookback) all at once.
    LOG.info("Getting data for" + slice.toString());
    DataFrame dfInput = fetchData(me, fetchStart.getMillis(), window.getEndMillis());
    DataFrame dfCurr = new DataFrame(dfInput).renameSeries(COL_VALUE, COL_CURR);
    DataFrame dfBase = computePredictionInterval(dfInput, window.getStartMillis(), datasetConfig.getTimezone());
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
    List<MergedAnomalyResultDTO> anomalyResults = DetectionUtils.makeAnomalies(slice, df, COL_ANOMALY,
        DetectionUtils.getMonitoringGranularityPeriod(timeGranularity.toAggregationGranularityString(),
            datasetConfig), datasetConfig);
    dfBase = dfBase.joinRight(df.retainSeries(COL_TIME, COL_CURR), COL_TIME);
    return DetectionResult.from(anomalyResults, TimeSeries.fromDataFrame(dfBase));
  }

  private DataFrame computePredictionInterval(DataFrame inputDF, long windowStartTime, String timezone) {

    DataFrame resultDF = new DataFrame();
    //filter the data inside window for current values.
    DataFrame forecastDF = inputDF.filter(new LongConditional() {
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
    double[] std = new double[size];
    double[] mean = new double[size];

    //get the trainingDF for each week, which is the number of lookback to 1 week before the each week predict start time
    for (int k = 0; k < size; k++) {
      DataFrame trainingDF;
      trainingDF = getLookbackDF(inputDF, forecastDF.getLong(COL_TIME, k));
      //the get historical WoW mean and std.
      std[k]= trainingDF.getDoubles(COL_CHANGE).std().value();
      mean[k] = trainingDF.getDoubles(COL_CHANGE).mean().value();

      //calculate baseline, error , upper and lower bound for prediction window.
      resultTimeArray[k] = forecastDF.getLong(COL_TIME, k);
      baselineArray[k] = trainingDF.getDouble(COL_VALUE,trainingDF.size()-1) * (1 + mean[k]);
      errorArray[k] = trainingDF.getDouble(COL_VALUE,trainingDF.size()-1) * sensitivityToSigma(this.sensitivity) * std[k];
      upperBoundArray[k] = baselineArray[k] + errorArray[k];
      lowerBoundArray[k] = baselineArray[k] - errorArray[k];
    }
    //Construct the dataframe.
    resultDF.addSeries(COL_TIME, LongSeries.buildFrom(resultTimeArray)).setIndex(COL_TIME);
    resultDF.addSeries(COL_VALUE, DoubleSeries.buildFrom(baselineArray));
    resultDF.addSeries(COL_UPPER_BOUND, DoubleSeries.buildFrom(upperBoundArray));
    resultDF.addSeries(COL_LOWER_BOUND, DoubleSeries.buildFrom(lowerBoundArray));
    resultDF.addSeries(COL_ERROR, DoubleSeries.buildFrom(errorArray));
    return resultDF;
  }

  /**
   * Fetch data from metric
   *
   * @param metricEntity metric entity
   * @param start start timestamp
   * @param end end timestamp
   * @return Data Frame that has data from start to end
   */
  private DataFrame fetchData(MetricEntity metricEntity, long start, long end) {
    List<MetricSlice> slices = new ArrayList<>();
    MetricSlice sliceData = MetricSlice.from(metricEntity.getId(), start, end,
        metricEntity.getFilters(), timeGranularity);
    slices.add(sliceData);
    LOG.info("Getting data for" + sliceData.toString());
    InputData data = this.dataFetcher.fetchData(new InputDataSpec().withTimeseriesSlices(slices));
    return data.getTimeseries().get(sliceData);
  }

  /**
   * Returns a data frame containing lookback number of data before prediction time
   * @param originalDF the original dataframe
   * @param time the prediction time, in unix timestamp
   * @return DataFrame containing lookback number of data
   */
  private DataFrame getLookbackDF(DataFrame originalDF, Long time) {
    LongSeries longSeries = (LongSeries) originalDF.get(COL_TIME);
    int indexEnd = longSeries.find(time);
    DataFrame df = DataFrame.builder(COL_TIME, COL_VALUE).build();

    if (indexEnd != -1) {
      int indexStart = Math.max(0, indexEnd - lookback);
      df = df.append(originalDF.slice(indexStart, indexEnd));
    }
    // calculate percentage change
    df.addSeries(COL_CURR,df.getDoubles(COL_VALUE).shift(-1));
    df.addSeries(COL_CHANGE, map((DoubleFunction) values -> {
      if (Double.compare(values[1], 0.0) == 0) {
        // divide by zero handling
        return 0.0;
      }
      return (values[0] - values[1]) / values[1];
    }, df.getDoubles(COL_CURR), df.get(COL_VALUE)));
    return df;
  }

  // Check whether monitoring timeGranularity is multiple days
  private boolean isMultiDayGranularity() {
    return !timeGranularity.equals(MetricSlice.NATIVE_GRANULARITY) && timeGranularity.getUnit() == TimeUnit.DAYS;
  }

  /**
   * Mapping of sensitivity to sigma on range of 0.5 - 1.5
   * @param sensitivity double from 0 to 10
   * @return sigma
   */
  private static double sensitivityToSigma(double sensitivity) {
    // If out of bound, use boundary sensitivity
    if (sensitivity < 0) {
      sensitivity = 0;
    } else if (sensitivity > 10) {
      sensitivity = 10;
    }
    double sigma = 0.5 + 0.1 * (10 - sensitivity);
    return sigma;
  }

}
