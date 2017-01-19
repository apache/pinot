package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeriesKey;
import com.linkedin.thirdeye.anomalydetection.control.AnomalyDetectionExecutor;
import com.linkedin.thirdeye.anomalydetection.model.detection.SimpleThresholdDetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.SeasonalAveragePredictionModel;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestWeekOverWeekRuleFunction {

  @DataProvider(name = "timeSeriesDataProvider")
  public Object[][] timeSeriesDataProvider() {
    AnomalyDetectionContext anomalyDetectionContext = new AnomalyDetectionContext();

    Properties properties = new Properties();
    properties.put(WeekOverWeekRule.BASELINE, "w/2wAvg");
    properties.put(SeasonalAveragePredictionModel.BUCKET_SIZE, "1");
    properties.put(SeasonalAveragePredictionModel.BUCKET_UNIT, TimeUnit.SECONDS.toString());
    properties.put(SimpleThresholdDetectionModel.CHANGE_THRESHOLD, "0.2");

    WeekOverWeekRule function = new WeekOverWeekRule();
    function.init(properties);
    anomalyDetectionContext.setAnomalyDetectionFunction(function);

    long oneWeekInMillis = TimeUnit.DAYS.toMillis(7);
    long bucketMillis = TimeUnit.SECONDS.toMillis(1);

    TimeSeries observedTimeSeries = new TimeSeries();
    long observedStartTime = 1000 + oneWeekInMillis * 2;
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
    anomalyDetectionContext.setCurrent(observedTimeSeries);

    List<TimeSeries> baselines = new ArrayList<>();
    TimeSeries baseline1TimeSeries = new TimeSeries();
    {
      long baseline1StartTime = 1000 + oneWeekInMillis;
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
      long baseline2StartTime = 1000;
      baseline2TimeSeries.set(baseline2StartTime, 10d);
      baseline2TimeSeries.set(baseline2StartTime + bucketMillis, 10d);
      baseline2TimeSeries.set(baseline2StartTime + bucketMillis * 2, 5d);
      baseline2TimeSeries.set(baseline2StartTime + bucketMillis * 3, 20d);
      baseline2TimeSeries.set(baseline2StartTime + bucketMillis * 4, 10d);
      Interval baseline2Interval = new Interval(baseline2StartTime, baseline2StartTime + bucketMillis * 5);
      baseline2TimeSeries.setTimeSeriesInterval(baseline2Interval);
    }
    baselines.add(baseline2TimeSeries);
    anomalyDetectionContext.setBaselines(baselines);

    TimeSeriesKey timeSeriesKey = new TimeSeriesKey();
    String metric = "testMetric";
    timeSeriesKey.setMetricName(metric);
    DimensionMap dimensionMap = new DimensionMap();
    dimensionMap.put("dimensionName1", "dimensionValue1");
    dimensionMap.put("dimensionName2", "dimensionValue2");
    timeSeriesKey.setDimensionMap(dimensionMap);
    anomalyDetectionContext.setTimeSeriesKey(timeSeriesKey);

    // Expected RawAnomalies
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

    return new Object[][] { {anomalyDetectionContext, expectedRawAnomalies} };
  }

  @Test(dataProvider = "timeSeriesDataProvider") public void analyze(
      AnomalyDetectionContext anomalyDetectionContext,
      List<RawAnomalyResultDTO> expectedRawAnomalies) throws Exception {
    final double epison = 0.00001d;
    List<RawAnomalyResultDTO> rawAnomalyResults = AnomalyDetectionExecutor.analyze(anomalyDetectionContext);
    Assert.assertEquals(rawAnomalyResults.size(), expectedRawAnomalies.size());
    for (int i = 0; i < rawAnomalyResults.size(); ++i) {
      RawAnomalyResultDTO actualAnomaly = rawAnomalyResults.get(i);
      RawAnomalyResultDTO expectedAnomaly = rawAnomalyResults.get(i);
      Assert.assertEquals(actualAnomaly.getWeight(), expectedAnomaly.getWeight(), epison);
      Assert.assertEquals(actualAnomaly.getScore(), expectedAnomaly.getScore(), epison);
    }
  }
}
