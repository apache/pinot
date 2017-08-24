package com.linkedin.thirdeye.anomaly.merge;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.util.JodaDateTimeUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class TestAnomalyTimeBasedSummarizer {
  @Test
  public void testMergeAnomalies(){
    AnomalyFunctionDTO anomalyFunction = new AnomalyFunctionDTO();
    anomalyFunction.setCollection("test");
    // anomaly merge config initialization
    AnomalyMergeConfig anomalyMergeConfig = new AnomalyMergeConfig();
    anomalyMergeConfig.setSequentialAllowedGap(TimeUnit.MINUTES.toMillis(30)); // merge anomalies apart 30 minutes
    anomalyMergeConfig.setMaxMergeDurationLength(TimeUnit.DAYS.toMillis(2)); // break anomaly longer than 1 days 23 hours
    anomalyMergeConfig.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);
    anomalyMergeConfig.setMergeablePropertyKeys(Collections.EMPTY_LIST);

    // Target Merged Anomaly
    MergedAnomalyResultDTO mergedAnomaly = new MergedAnomalyResultDTO();
    mergedAnomaly.setCollection(anomalyFunction.getCollection());
    mergedAnomaly.setFunction(anomalyFunction);
    mergedAnomaly.setStartTime(JodaDateTimeUtils.toDateTime("2017-07-04T00:00:00Z").getMillis());
    mergedAnomaly.setEndTime(JodaDateTimeUtils.toDateTime("2017-07-04T23:59:59Z").getMillis());

    List<RawAnomalyResultDTO> rawAnomalyResults = new ArrayList<>();
    RawAnomalyResultDTO rawAnomalyResult1 = new RawAnomalyResultDTO();
    rawAnomalyResult1.setFunction(anomalyFunction);
    rawAnomalyResult1.setStartTime(JodaDateTimeUtils.toDateTime("2017-07-05T00:00:00Z").getMillis());
    rawAnomalyResult1.setEndTime(JodaDateTimeUtils.toDateTime("2017-07-05T23:59:59Z").getMillis());
    RawAnomalyResultDTO rawAnomalyResult2 = new RawAnomalyResultDTO();
    rawAnomalyResult2.setFunction(anomalyFunction);
    rawAnomalyResult2.setStartTime(JodaDateTimeUtils.toDateTime("2017-07-06T00:00:00Z").getMillis());
    rawAnomalyResult2.setEndTime(JodaDateTimeUtils.toDateTime("2017-07-06T23:59:59Z").getMillis());
    rawAnomalyResults.add(rawAnomalyResult1);
    rawAnomalyResults.add(rawAnomalyResult2);

    // Test when raw anomalies come in bulk
    List<MergedAnomalyResultDTO> mergedResults = AnomalyTimeBasedSummarizer
        .mergeAnomalies(mergedAnomaly, rawAnomalyResults, anomalyMergeConfig);

    // There should be two merged anomaly: 1) mergedAnomaly + rawAnomalyResult1 and 2) rawAnomalyResult2
    assert(mergedResults.size() == 2);
    // Check if the first raw anomaly is merged with the merged anomaly
    MergedAnomalyResultDTO firstMergedAnomaly = mergedResults.get(0);
    assert(firstMergedAnomaly.getStartTime() == mergedAnomaly.getStartTime());
    assert(firstMergedAnomaly.getEndTime() == rawAnomalyResult1.getEndTime());

    // Check if the second anomaly is not merged with the merged anomaly
    MergedAnomalyResultDTO secondMergedAnomaly = mergedResults.get(1);
    assert(secondMergedAnomaly.getStartTime() == rawAnomalyResult2.getStartTime());
    assert(secondMergedAnomaly.getEndTime() == rawAnomalyResult2.getEndTime());

    rawAnomalyResults.clear();

    // Test when raw anomalies come one-by-one
    rawAnomalyResults.add(rawAnomalyResult1);
    mergedResults = AnomalyTimeBasedSummarizer
        .mergeAnomalies(mergedAnomaly, rawAnomalyResults, anomalyMergeConfig);
    assert(mergedResults.size() == 1);
    rawAnomalyResults.clear();
    rawAnomalyResults.add(rawAnomalyResult2);
    mergedResults = AnomalyTimeBasedSummarizer
        .mergeAnomalies(mergedAnomaly, rawAnomalyResults, anomalyMergeConfig);
    assert(mergedResults.size() == 2);

    // Check if the first raw anomaly is merged with the merged anomaly
    firstMergedAnomaly = mergedResults.get(0);
    assert(firstMergedAnomaly.getStartTime() == mergedAnomaly.getStartTime());
    assert(firstMergedAnomaly.getEndTime() == rawAnomalyResult1.getEndTime());

    // Check if the second anomaly is not merged with the merged anomaly
    secondMergedAnomaly = mergedResults.get(1);
    assert(secondMergedAnomaly.getStartTime() == rawAnomalyResult2.getStartTime());
    assert(secondMergedAnomaly.getEndTime() == rawAnomalyResult2.getEndTime());
  }

}
