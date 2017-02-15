package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeriesKey;
import com.linkedin.thirdeye.anomalydetection.model.detection.SimpleThresholdDetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.transform.MovingAverageSmoothingFunction;
import com.linkedin.thirdeye.anomalydetection.model.transform.TotalCountThresholdRemovalFunction;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestWeekOverWeekRuleFunction {
  private final static double EPSILON = 0.00001d;

  private final static long oneWeekInMillis = TimeUnit.DAYS.toMillis(7);
  private final static long bucketMillis = TimeUnit.SECONDS.toMillis(1);
  private final static long observedStartTime = 1000 + oneWeekInMillis * 2;
  private final static long baseline1StartTime = 1000 + oneWeekInMillis;
  private final static long baseline2StartTime = 1000;

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
      observedTimeSeries.set(observedStartTime + bucketMillis * 3, 27d);
      observedTimeSeries.set(observedStartTime + bucketMillis * 4, 10d);
      Interval observedTimeSeriesInterval =
          new Interval(observedStartTime, observedStartTime + bucketMillis * 5);
      observedTimeSeries.setTimeSeriesInterval(observedTimeSeriesInterval);
    }

    List<TimeSeries> baselines = new ArrayList<>();
    TimeSeries baseline1TimeSeries = new TimeSeries();
    {
      baseline1TimeSeries.set(baseline1StartTime, 10d);
      baseline1TimeSeries.set(baseline1StartTime + bucketMillis, 20d);
      baseline1TimeSeries.set(baseline1StartTime + bucketMillis * 2, 15d);
      baseline1TimeSeries.set(baseline1StartTime + bucketMillis * 3, 24d);
      baseline1TimeSeries.set(baseline1StartTime + bucketMillis * 4, 14d);
      Interval baseline1Interval = new Interval(baseline1StartTime, baseline1StartTime + bucketMillis * 5);
      baseline1TimeSeries.setTimeSeriesInterval(baseline1Interval);
    }
    baselines.add(baseline1TimeSeries);

    TimeSeries baseline2TimeSeries = new TimeSeries();
    {
      baseline2TimeSeries.set(baseline2StartTime, 10d);
      baseline2TimeSeries.set(baseline2StartTime + bucketMillis, 10d);
      baseline2TimeSeries.set(baseline2StartTime + bucketMillis * 2, 5d);
      baseline2TimeSeries.set(baseline2StartTime + bucketMillis * 3, 20d);
      baseline2TimeSeries.set(baseline2StartTime + bucketMillis * 4, 10d);
      Interval baseline2Interval = new Interval(baseline2StartTime, baseline2StartTime + bucketMillis * 5);
      baseline2TimeSeries.setTimeSeriesInterval(baseline2Interval);
    }
    baselines.add(baseline2TimeSeries);

    return new Object[][] { {properties, timeSeriesKey, bucketSizeInMS, observedTimeSeries, baselines} };
  }

  @Test(dataProvider = "timeSeriesDataProvider") public void analyzeWoW(
      Properties properties, TimeSeriesKey timeSeriesKey, long bucketSizeInMs,
      TimeSeries observedTimeSeries, List<TimeSeries> baselines) throws Exception {
    // Expected RawAnomalies without smoothing
    List<RawAnomalyResultDTO> expectedRawAnomalies = new ArrayList<>();
    RawAnomalyResultDTO rawAnomaly1 = new RawAnomalyResultDTO();
    rawAnomaly1.setStartTime(observedStartTime + bucketMillis);
    rawAnomaly1.setEndTime(observedStartTime + bucketMillis * 2);
    rawAnomaly1.setWeight(-0.25d);
    rawAnomaly1.setScore(15d);
    expectedRawAnomalies.add(rawAnomaly1);

    RawAnomalyResultDTO rawAnomaly2 = new RawAnomalyResultDTO();
    rawAnomaly2.setStartTime(observedStartTime + bucketMillis * 4);
    rawAnomaly2.setEndTime(observedStartTime + bucketMillis * 5);
    rawAnomaly2.setWeight(-0.2857142857d);
    rawAnomaly2.setScore(15d);
    expectedRawAnomalies.add(rawAnomaly2);

    AnomalyDetectionContext anomalyDetectionContext = new AnomalyDetectionContext();
    anomalyDetectionContext.setBucketSizeInMS(bucketSizeInMs);

    // Append properties for anomaly function specific setting
    properties.put(WeekOverWeekRuleFunction.BASELINE, "w/w");
    properties.put(SimpleThresholdDetectionModel.CHANGE_THRESHOLD, "-0.2");

    // Create anomaly function spec
    AnomalyFunctionDTO functionSpec = new AnomalyFunctionDTO();
    functionSpec.setMetric(mainMetric);
    functionSpec.setProperties(toString(properties));

    WeekOverWeekRuleFunction function = new WeekOverWeekRuleFunction();
    function.init(functionSpec);
    anomalyDetectionContext.setAnomalyDetectionFunction(function);
    anomalyDetectionContext.setCurrent(mainMetric, observedTimeSeries);
    List<TimeSeries> singleBaseline = new ArrayList<>();
    singleBaseline.add(baselines.get(0));
    anomalyDetectionContext.setBaselines(mainMetric, singleBaseline);
    anomalyDetectionContext.setTimeSeriesKey(timeSeriesKey);

    List<RawAnomalyResultDTO> rawAnomalyResults = function.analyze(anomalyDetectionContext);
    Assert.assertEquals(rawAnomalyResults.size(), expectedRawAnomalies.size());
    for (int i = 0; i < rawAnomalyResults.size(); ++i) {
      RawAnomalyResultDTO actualAnomaly = rawAnomalyResults.get(i);
      RawAnomalyResultDTO expectedAnomaly = rawAnomalyResults.get(i);
      Assert.assertEquals(actualAnomaly.getWeight(), expectedAnomaly.getWeight(), EPSILON);
      Assert.assertEquals(actualAnomaly.getScore(), expectedAnomaly.getScore(), EPSILON);
    }

    // Test data model
    List<Interval> expectedDataRanges = new ArrayList<>();
    expectedDataRanges.add(new Interval(observedStartTime, observedStartTime + bucketMillis * 5));
    expectedDataRanges.add(new Interval(observedStartTime - oneWeekInMillis,
        observedStartTime + bucketMillis * 5 - oneWeekInMillis));

    List<Interval> actualDataRanges = function.getDataModel()
        .getAllDataIntervals(observedStartTime, observedStartTime + bucketMillis * 5);
    Assert.assertEquals(actualDataRanges, expectedDataRanges);
  }

  @Test(dataProvider = "timeSeriesDataProvider") public void analyzeWo2WAvg(
      Properties properties, TimeSeriesKey timeSeriesKey, long bucketSizeInMs,
      TimeSeries observedTimeSeries, List<TimeSeries> baselines) throws Exception {
    // Expected RawAnomalies without smoothing
    List<RawAnomalyResultDTO> expectedRawAnomalies = new ArrayList<>();
    RawAnomalyResultDTO rawAnomaly1 = new RawAnomalyResultDTO();
    rawAnomaly1.setStartTime(observedStartTime + bucketMillis * 2);
    rawAnomaly1.setEndTime(observedStartTime + bucketMillis * 3);
    rawAnomaly1.setWeight(0.3d);
    rawAnomaly1.setScore(15d);
    expectedRawAnomalies.add(rawAnomaly1);

    RawAnomalyResultDTO rawAnomaly2 = new RawAnomalyResultDTO();
    rawAnomaly2.setStartTime(observedStartTime + bucketMillis * 3);
    rawAnomaly2.setEndTime(observedStartTime + bucketMillis * 4);
    rawAnomaly2.setWeight(0.22727272727272727);
    rawAnomaly2.setScore(15d);
    expectedRawAnomalies.add(rawAnomaly2);

    AnomalyDetectionContext anomalyDetectionContext = new AnomalyDetectionContext();
    anomalyDetectionContext.setBucketSizeInMS(bucketSizeInMs);

    // Append properties for anomaly function specific setting
    properties.put(WeekOverWeekRuleFunction.BASELINE, "w/2wAvg");
    properties.put(SimpleThresholdDetectionModel.CHANGE_THRESHOLD, "0.2");

    // Create anomaly function spec
    AnomalyFunctionDTO functionSpec = new AnomalyFunctionDTO();
    functionSpec.setMetric(mainMetric);
    functionSpec.setProperties(toString(properties));

    WeekOverWeekRuleFunction function = new WeekOverWeekRuleFunction();
    function.init(functionSpec);
    anomalyDetectionContext.setAnomalyDetectionFunction(function);
    anomalyDetectionContext.setCurrent(mainMetric, observedTimeSeries);
    anomalyDetectionContext.setBaselines(mainMetric, baselines);
    anomalyDetectionContext.setTimeSeriesKey(timeSeriesKey);

    List<RawAnomalyResultDTO> rawAnomalyResults = function.analyze(anomalyDetectionContext);
    compareWo2WAvgRawAnomalies(rawAnomalyResults);


    // Test data model
    List<Interval> expectedDataRanges = new ArrayList<>();
    expectedDataRanges.add(new Interval(observedStartTime, observedStartTime + bucketMillis * 5));
    expectedDataRanges.add(new Interval(observedStartTime - oneWeekInMillis,
        observedStartTime + bucketMillis * 5 - oneWeekInMillis));
    expectedDataRanges.add(new Interval(observedStartTime - oneWeekInMillis * 2,
        observedStartTime + bucketMillis * 5 - oneWeekInMillis * 2));

    List<Interval> actualDataRanges = function.getDataModel()
        .getAllDataIntervals(observedStartTime, observedStartTime + bucketMillis * 5);
    Assert.assertEquals(actualDataRanges, expectedDataRanges);
  }

  @Test(dataProvider = "timeSeriesDataProvider") public void analyzeWo2WAvgSmoothedTimeSeries(
      Properties properties, TimeSeriesKey timeSeriesKey, long bucketSizeInMs,
      TimeSeries observedTimeSeries, List<TimeSeries> baselines) throws Exception {
    AnomalyDetectionContext anomalyDetectionContext = new AnomalyDetectionContext();
    anomalyDetectionContext.setBucketSizeInMS(bucketSizeInMs);

    // Append properties for anomaly function specific setting
    properties.put(WeekOverWeekRuleFunction.BASELINE, "w/2wAvg");
    properties.put(SimpleThresholdDetectionModel.CHANGE_THRESHOLD, "0.2");
    properties.put(WeekOverWeekRuleFunction.ENABLE_SMOOTHING, "true");
    properties.put(MovingAverageSmoothingFunction.MOVING_AVERAGE_SMOOTHING_WINDOW_SIZE, "3");

    // Create anomaly function spec
    AnomalyFunctionDTO functionSpec = new AnomalyFunctionDTO();
    functionSpec.setMetric(mainMetric);
    functionSpec.setProperties(toString(properties));

    WeekOverWeekRuleFunction function = new WeekOverWeekRuleFunction();
    function.init(functionSpec);
    anomalyDetectionContext.setAnomalyDetectionFunction(function);
    anomalyDetectionContext.setCurrent(mainMetric, observedTimeSeries);
    anomalyDetectionContext.setBaselines(mainMetric, baselines);
    anomalyDetectionContext.setTimeSeriesKey(timeSeriesKey);

    List<RawAnomalyResultDTO> rawAnomalyResults = function.analyze(anomalyDetectionContext);
    // The transformed observed time series is resized from 5 to 3 due to moving average algorithm
    Assert.assertEquals(anomalyDetectionContext.getTransformedCurrent(mainMetric).size(), 3);
    // No anomalies after smoothing the time series
    Assert.assertEquals(rawAnomalyResults.size(), 0);
  }

  @Test(dataProvider = "timeSeriesDataProvider") public void recomputeMergedAnomalyWeight(
      Properties properties, TimeSeriesKey timeSeriesKey, long bucketSizeInMs,
      TimeSeries observedTimeSeries, List<TimeSeries> baselines) throws Exception {
    // Expected RawAnomalies without smoothing
    List<RawAnomalyResultDTO> expectedRawAnomalies = new ArrayList<>();
    RawAnomalyResultDTO rawAnomaly1 = new RawAnomalyResultDTO();
    rawAnomaly1.setStartTime(observedStartTime + bucketMillis * 2);
    rawAnomaly1.setEndTime(observedStartTime + bucketMillis * 3);
    rawAnomaly1.setWeight(0.3d);
    rawAnomaly1.setScore(15d);
    expectedRawAnomalies.add(rawAnomaly1);

    RawAnomalyResultDTO rawAnomaly2 = new RawAnomalyResultDTO();
    rawAnomaly2.setStartTime(observedStartTime + bucketMillis * 3);
    rawAnomaly2.setEndTime(observedStartTime + bucketMillis * 4);
    rawAnomaly2.setWeight(0.22727272727272727);
    rawAnomaly2.setScore(15d);
    expectedRawAnomalies.add(rawAnomaly2);

    AnomalyDetectionContext anomalyDetectionContext = new AnomalyDetectionContext();
    anomalyDetectionContext.setBucketSizeInMS(bucketSizeInMs);

    // Append properties for anomaly function specific setting
    properties.put(WeekOverWeekRuleFunction.BASELINE, "w/2wAvg");
    properties.put(SimpleThresholdDetectionModel.CHANGE_THRESHOLD, "0.2");

    // Create anomaly function spec
    AnomalyFunctionDTO functionSpec = new AnomalyFunctionDTO();
    functionSpec.setMetric(mainMetric);
    functionSpec.setProperties(toString(properties));

    WeekOverWeekRuleFunction function = new WeekOverWeekRuleFunction();
    function.init(functionSpec);
    anomalyDetectionContext.setAnomalyDetectionFunction(function);
    anomalyDetectionContext.setCurrent(mainMetric, observedTimeSeries);
    anomalyDetectionContext.setBaselines(mainMetric, baselines);
    anomalyDetectionContext.setTimeSeriesKey(timeSeriesKey);

    MergedAnomalyResultDTO mergedAnomaly = new MergedAnomalyResultDTO();
    mergedAnomaly.setStartTime(expectedRawAnomalies.get(0).getStartTime());
    mergedAnomaly.setEndTime(expectedRawAnomalies.get(1).getEndTime());
    mergedAnomaly.setAnomalyResults(expectedRawAnomalies);

    function.updateMergedAnomalyInfo(anomalyDetectionContext, mergedAnomaly);

    // Test weight; weight is the percentage change between the sums of observed values and
    // expected values, respectively. Note that expected values are generated by the trained model,
    // which takes as input one or many baseline time series.
    final long oneWeekInMillis = TimeUnit.DAYS.toMillis(7);
    double observedTotal = 0d;
    double baselineTotal = 0d;
    Interval interval = new Interval(mergedAnomaly.getStartTime(), mergedAnomaly.getEndTime());
    TimeSeries observedTS = anomalyDetectionContext.getTransformedCurrent(mainMetric);
    List<TimeSeries> baselineTSs = anomalyDetectionContext.getTransformedBaselines(mainMetric);
    for (long timestamp : observedTS.timestampSet()) {
      if (interval.contains(timestamp)) {
        observedTotal += observedTS.get(timestamp);
        for (int i = 0; i < baselineTSs.size(); ++i) {
          TimeSeries baselineTS = baselineTSs.get(i);
          long baseTimeStamp = timestamp - oneWeekInMillis * (i + 1);
          baselineTotal += baselineTS.get(baseTimeStamp);
        }
      }
    }
    baselineTotal /= baselineTSs.size();
    double expectedWeight = (observedTotal - baselineTotal) / baselineTotal;
    Assert.assertEquals(mergedAnomaly.getWeight(), expectedWeight, EPSILON);

    // Test Score; score is the average of all raw anomalies' score
    double expectedScore = 0d;
    for (RawAnomalyResultDTO rawAnomaly : expectedRawAnomalies) {
      expectedScore += rawAnomaly.getScore();
    }
    expectedScore /= expectedRawAnomalies.size();
    Assert.assertEquals(mergedAnomaly.getScore(), expectedScore, EPSILON);
  }

  @Test(dataProvider = "timeSeriesDataProvider") public void testTotalCountThresholdFunction(
      Properties properties, TimeSeriesKey timeSeriesKey, long bucketSizeInMs,
      TimeSeries observedTimeSeries, List<TimeSeries> baselines) throws Exception {
    AnomalyDetectionContext anomalyDetectionContext = new AnomalyDetectionContext();
    anomalyDetectionContext.setBucketSizeInMS(bucketSizeInMs);

    // Append properties for anomaly function specific setting
    String totalCountTimeSeriesName = "totalCount";
    TimeSeries totalCountTimeSeries = new TimeSeries();
    {
      totalCountTimeSeries.set(observedStartTime, 10d);
      totalCountTimeSeries.set(observedStartTime + bucketMillis, 10d);
      totalCountTimeSeries.set(observedStartTime + bucketMillis * 2, 10d);
      totalCountTimeSeries.set(observedStartTime + bucketMillis * 3, 10d);
      totalCountTimeSeries.set(observedStartTime + bucketMillis * 4, 10d);
      Interval totalCountTimeSeriesInterval =
          new Interval(observedStartTime, observedStartTime + bucketMillis * 5);
      totalCountTimeSeries.setTimeSeriesInterval(totalCountTimeSeriesInterval);
    }
    properties.put(TotalCountThresholdRemovalFunction.TOTAL_COUNT_METRIC_NAME, totalCountTimeSeriesName);
    properties.put(TotalCountThresholdRemovalFunction.TOTAL_COUNT_THRESHOLD, "51");

    properties.put(WeekOverWeekRuleFunction.BASELINE, "w/2wAvg");
    properties.put(SimpleThresholdDetectionModel.CHANGE_THRESHOLD, "0.2");


    // Create anomaly function spec
    AnomalyFunctionDTO functionSpec = new AnomalyFunctionDTO();
    functionSpec.setMetric(mainMetric);
    functionSpec.setProperties(toString(properties));

    // Create anomalyDetectionContext using anomaly function spec
    WeekOverWeekRuleFunction function = new WeekOverWeekRuleFunction();
    function.init(functionSpec);
    anomalyDetectionContext.setAnomalyDetectionFunction(function);
    anomalyDetectionContext.setCurrent(mainMetric, observedTimeSeries);
    anomalyDetectionContext.setBaselines(mainMetric, baselines);
    anomalyDetectionContext.setTimeSeriesKey(timeSeriesKey);
    anomalyDetectionContext.setCurrent(totalCountTimeSeriesName, totalCountTimeSeries);

    List<RawAnomalyResultDTO> rawAnomalyResults = function.analyze(anomalyDetectionContext);
    // No anomalies after smoothing the time series
    Assert.assertEquals(rawAnomalyResults.size(), 0);


    // Test disabled total count by lowering the threshold
    anomalyDetectionContext = new AnomalyDetectionContext();
    anomalyDetectionContext.setBucketSizeInMS(bucketSizeInMs);

    properties.put(TotalCountThresholdRemovalFunction.TOTAL_COUNT_THRESHOLD, "0");
    // Create anomaly function spec
    functionSpec = new AnomalyFunctionDTO();
    functionSpec.setMetric(mainMetric);
    functionSpec.setProperties(toString(properties));
    function = new WeekOverWeekRuleFunction();
    function.init(functionSpec);

    anomalyDetectionContext.setAnomalyDetectionFunction(function);
    anomalyDetectionContext.setCurrent(mainMetric, observedTimeSeries);
    anomalyDetectionContext.setBaselines(mainMetric, baselines);
    anomalyDetectionContext.setTimeSeriesKey(timeSeriesKey);
    anomalyDetectionContext.setCurrent(totalCountTimeSeriesName, totalCountTimeSeries);

    rawAnomalyResults = function.analyze(anomalyDetectionContext);
    compareWo2WAvgRawAnomalies(rawAnomalyResults);
  }

  @Test
  public void testParseWowString() {
    String testString = "w/w";
    Assert.assertEquals(WeekOverWeekRuleFunction.parseWowString(testString), "1");

    testString = "Wo4W";
    Assert.assertEquals(WeekOverWeekRuleFunction.parseWowString(testString), "4");

    testString = "W/243wABCD";
    Assert.assertEquals(WeekOverWeekRuleFunction.parseWowString(testString), "243");

    testString = "2abc";
    Assert.assertEquals(WeekOverWeekRuleFunction.parseWowString(testString), "2");

    testString = "W/243";
    Assert.assertEquals(WeekOverWeekRuleFunction.parseWowString(testString), "243");

    testString = "A Random string 34 and it is 54 a long one";
    Assert.assertEquals(WeekOverWeekRuleFunction.parseWowString(testString), "34");
  }

  private String toString(Properties properties) {
    if (properties != null && properties.size() != 0) {
      List<String> propertyEntry = new ArrayList<>();
      StringBuilder sb = new StringBuilder();
      Set<Object> keys = properties.keySet();
      for (Object key : keys) {
        sb.append((String) key).append("=").append((String) properties.get(key));
        propertyEntry.add(sb.toString());
        sb.setLength(0);
      }
      return String.join(";", propertyEntry);
    }
    return "";
  }

  private void compareWo2WAvgRawAnomalies(List<RawAnomalyResultDTO> rawAnomalyResults) {
    // Expecting the same anomaly result from analyzeWo2WAvg
    List<RawAnomalyResultDTO> expectedRawAnomalies = new ArrayList<>();
    RawAnomalyResultDTO rawAnomaly1 = new RawAnomalyResultDTO();
    rawAnomaly1.setStartTime(observedStartTime + bucketMillis * 2);
    rawAnomaly1.setEndTime(observedStartTime + bucketMillis * 3);
    rawAnomaly1.setWeight(0.3d);
    rawAnomaly1.setScore(15d);
    expectedRawAnomalies.add(rawAnomaly1);

    RawAnomalyResultDTO rawAnomaly2 = new RawAnomalyResultDTO();
    rawAnomaly2.setStartTime(observedStartTime + bucketMillis * 3);
    rawAnomaly2.setEndTime(observedStartTime + bucketMillis * 4);
    rawAnomaly2.setWeight(0.22727272727272727);
    rawAnomaly2.setScore(15d);
    expectedRawAnomalies.add(rawAnomaly2);

    Assert.assertEquals(rawAnomalyResults.size(), expectedRawAnomalies.size());
    for (int i = 0; i < rawAnomalyResults.size(); ++i) {
      RawAnomalyResultDTO actualAnomaly = rawAnomalyResults.get(i);
      RawAnomalyResultDTO expectedAnomaly = rawAnomalyResults.get(i);
      Assert.assertEquals(actualAnomaly.getWeight(), expectedAnomaly.getWeight(), EPSILON);
      Assert.assertEquals(actualAnomaly.getScore(), expectedAnomaly.getScore(), EPSILON);
    }
  }
}
