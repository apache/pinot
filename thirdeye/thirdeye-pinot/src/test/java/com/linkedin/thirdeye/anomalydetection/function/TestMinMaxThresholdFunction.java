package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeriesKey;
import com.linkedin.thirdeye.anomalydetection.model.detection.MinMaxThresholdDetectionModel;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestMinMaxThresholdFunction {
  private final static double EPSILON = 0.00001d;

  private final static long bucketMillis = TimeUnit.SECONDS.toMillis(1);
  private final static long observedStartTime = 1000;

  private final static String mainMetric = "testMetric";

  @DataProvider(name = "timeSeriesDataProvider")
  public Object[][] timeSeriesDataProvider() {
    // The properties for the testing time series
    Properties properties = new Properties();
    long bucketSizeInMS = TimeUnit.SECONDS.toMillis(1);

    // Set up time series key for the testing time series
    TimeSeriesKey timeSeriesKey = new TimeSeriesKey();
    String metric = mainMetric;
    timeSeriesKey.setMetricName(metric);
    DimensionMap dimensionMap = new DimensionMap();
    dimensionMap.put("dimensionName1", "dimensionValue1");
    dimensionMap.put("dimensionName2", "dimensionValue2");
    timeSeriesKey.setDimensionMap(dimensionMap);

    TimeSeries observedTimeSeries = new TimeSeries();
    {
      observedTimeSeries.set(observedStartTime, 10d);
      observedTimeSeries.set(observedStartTime + bucketMillis, 15d);
      observedTimeSeries.set(observedStartTime + bucketMillis * 2, 13d);
      observedTimeSeries.set(observedStartTime + bucketMillis * 3, 22d);
      observedTimeSeries.set(observedStartTime + bucketMillis * 4, 8d);
      Interval observedTimeSeriesInterval =
          new Interval(observedStartTime, observedStartTime + bucketMillis * 5);
      observedTimeSeries.setTimeSeriesInterval(observedTimeSeriesInterval);
    }

    return new Object[][] { {properties, timeSeriesKey, bucketSizeInMS, observedTimeSeries} };
  }

  @Test(dataProvider = "timeSeriesDataProvider")
  public void analyze(Properties properties, TimeSeriesKey timeSeriesKey, long bucketSizeInMs,
      TimeSeries observedTimeSeries) throws Exception {
    AnomalyDetectionContext anomalyDetectionContext = new AnomalyDetectionContext();
    anomalyDetectionContext.setBucketSizeInMS(bucketSizeInMs);

    properties.put(MinMaxThresholdDetectionModel.MAX_VAL, "20");
    properties.put(MinMaxThresholdDetectionModel.MIN_VAL, "12");

    // Create anomaly function spec
    AnomalyFunctionDTO functionSpec = new AnomalyFunctionDTO();
    functionSpec.setMetric(mainMetric);
    functionSpec.setProperties(TestWeekOverWeekRuleFunction.toString(properties));

    AnomalyDetectionFunction function = new MinMaxThresholdFunction();
    function.init(functionSpec);
    anomalyDetectionContext.setAnomalyDetectionFunction(function);
    anomalyDetectionContext.setCurrent(mainMetric, observedTimeSeries);
    anomalyDetectionContext.setTimeSeriesKey(timeSeriesKey);

    List<RawAnomalyResultDTO> actualAnomalyResults = function.analyze(anomalyDetectionContext);

    // Expected RawAnomalies of WoW without smoothing
    List<RawAnomalyResultDTO> expectedRawAnomalies = new ArrayList<>();
    RawAnomalyResultDTO rawAnomaly1 = new RawAnomalyResultDTO();
    rawAnomaly1.setStartTime(observedStartTime);
    rawAnomaly1.setEndTime(observedStartTime + bucketMillis);
    rawAnomaly1.setWeight(-0.166666d);
    rawAnomaly1.setScore(13.6d);
    expectedRawAnomalies.add(rawAnomaly1);

    RawAnomalyResultDTO rawAnomaly2 = new RawAnomalyResultDTO();
    rawAnomaly2.setStartTime(observedStartTime + bucketMillis * 3);
    rawAnomaly2.setEndTime(observedStartTime + bucketMillis * 4);
    rawAnomaly2.setWeight(0.1d);
    rawAnomaly2.setScore(13.6d);
    expectedRawAnomalies.add(rawAnomaly2);

    RawAnomalyResultDTO rawAnomaly3 = new RawAnomalyResultDTO();
    rawAnomaly3.setStartTime(observedStartTime + bucketMillis * 4);
    rawAnomaly3.setEndTime(observedStartTime + bucketMillis * 5);
    rawAnomaly3.setWeight(-0.33333d);
    rawAnomaly3.setScore(13.6d);
    expectedRawAnomalies.add(rawAnomaly3);

    Assert.assertEquals(actualAnomalyResults.size(), expectedRawAnomalies.size());
    for (int i = 0; i < actualAnomalyResults.size(); ++i) {
      RawAnomalyResultDTO actualAnomaly = actualAnomalyResults.get(i);
      RawAnomalyResultDTO expectedAnomaly = actualAnomalyResults.get(i);
      Assert.assertEquals(actualAnomaly.getWeight(), expectedAnomaly.getWeight(), EPSILON);
      Assert.assertEquals(actualAnomaly.getScore(), expectedAnomaly.getScore(), EPSILON);
    }

    // Test getTimeSeriesIntervals
    List<Interval> expectedDataRanges = new ArrayList<>();
    expectedDataRanges.add(new Interval(observedStartTime, observedStartTime + bucketMillis * 5));
    List<Interval> actualDataRanges =
        function.getTimeSeriesIntervals(observedStartTime, observedStartTime + bucketMillis * 5);
    Assert.assertEquals(actualDataRanges, expectedDataRanges);
  }

  @Test(dataProvider = "timeSeriesDataProvider")
  public void recomputeMergedAnomalyWeight(Properties properties, TimeSeriesKey timeSeriesKey,
      long bucketSizeInMs, TimeSeries observedTimeSeries) throws Exception {

    AnomalyDetectionContext anomalyDetectionContext = new AnomalyDetectionContext();
    anomalyDetectionContext.setBucketSizeInMS(bucketSizeInMs);

    properties.put(MinMaxThresholdDetectionModel.MAX_VAL, "20");
    properties.put(MinMaxThresholdDetectionModel.MIN_VAL, "12");

    // Create anomaly function spec
    AnomalyFunctionDTO functionSpec = new AnomalyFunctionDTO();
    functionSpec.setMetric(mainMetric);
    functionSpec.setProperties(TestWeekOverWeekRuleFunction.toString(properties));

    AnomalyDetectionFunction function = new MinMaxThresholdFunction();
    function.init(functionSpec);
    anomalyDetectionContext.setAnomalyDetectionFunction(function);
    anomalyDetectionContext.setCurrent(mainMetric, observedTimeSeries);
    anomalyDetectionContext.setTimeSeriesKey(timeSeriesKey);

    List<RawAnomalyResultDTO> expectedRawAnomalies = new ArrayList<>();

    RawAnomalyResultDTO rawAnomaly1 = new RawAnomalyResultDTO();
    rawAnomaly1.setStartTime(observedStartTime + bucketMillis * 3);
    rawAnomaly1.setEndTime(observedStartTime + bucketMillis * 4);
    rawAnomaly1.setWeight(0.1d);
    rawAnomaly1.setScore(13.6d);
    expectedRawAnomalies.add(rawAnomaly1);

    RawAnomalyResultDTO rawAnomaly2 = new RawAnomalyResultDTO();
    rawAnomaly2.setStartTime(observedStartTime + bucketMillis * 4);
    rawAnomaly2.setEndTime(observedStartTime + bucketMillis * 5);
    rawAnomaly2.setWeight(-0.33333d);
    rawAnomaly2.setScore(13.6d);
    expectedRawAnomalies.add(rawAnomaly2);

    MergedAnomalyResultDTO mergedAnomaly = new MergedAnomalyResultDTO();
    mergedAnomaly.setStartTime(expectedRawAnomalies.get(0).getStartTime());
    mergedAnomaly.setEndTime(expectedRawAnomalies.get(1).getEndTime());
    mergedAnomaly.setAnomalyResults(expectedRawAnomalies);

    function.updateMergedAnomalyInfo(anomalyDetectionContext, mergedAnomaly);

    double currentTotal = 0d;
    double deviationFromThreshold = 0d;
    Interval interval = new Interval(mergedAnomaly.getStartTime(), mergedAnomaly.getEndTime());
    TimeSeries currentTS = anomalyDetectionContext.getTransformedCurrent(mainMetric);
    for (long timestamp : currentTS.timestampSet()) {
      if (interval.contains(timestamp)) {
        double value = currentTS.get(timestamp);
        currentTotal += value;
        deviationFromThreshold += computeDeviationFromMinMax(value, 12d, 20d);
      }
    }
    double score = currentTotal / 2d;
    double weight = deviationFromThreshold / 2d;

    Assert.assertEquals(mergedAnomaly.getScore(), score, EPSILON);
    Assert.assertEquals(mergedAnomaly.getAvgCurrentVal(), score, EPSILON);
    Assert.assertEquals(mergedAnomaly.getWeight(), weight, EPSILON);
  }

  private static double computeDeviationFromMinMax(double currentValue, double min, double max) {
    if (currentValue < min && min != 0d) {
      return (currentValue - min) / min;
    } else if (currentValue > max && max != 0d) {
      return (currentValue - max) / max;
    }
    return 0;
  }
}
