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

package com.linkedin.thirdeye.anomaly.merge;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.anomalydetection.context.RawAnomalyResult;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
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
    anomalyFunction.setTopicMetric("metric");
    // anomaly merge config initialization
    AnomalyMergeConfig anomalyMergeConfig = new AnomalyMergeConfig();
    anomalyMergeConfig.setSequentialAllowedGap(TimeUnit.MINUTES.toMillis(30)); // merge anomalies apart 30 minutes
    anomalyMergeConfig.setMaxMergeDurationLength(TimeUnit.DAYS.toMillis(2)); // break anomaly longer than 1 days 23 hours
    anomalyMergeConfig.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);
    anomalyMergeConfig.setMergeablePropertyKeys(Collections.<String>emptyList());

    // Target Merged Anomaly
    MergedAnomalyResultDTO mergedAnomaly = new MergedAnomalyResultDTO();
    mergedAnomaly.setCollection(anomalyFunction.getCollection());
    mergedAnomaly.setFunction(anomalyFunction);
    mergedAnomaly.setStartTime(JodaDateTimeUtils.toDateTime("2017-07-04T00:00:00Z").getMillis());
    mergedAnomaly.setEndTime(JodaDateTimeUtils.toDateTime("2017-07-04T23:59:59Z").getMillis());

    List<AnomalyResult> rawAnomalyResults = new ArrayList<>();
    AnomalyResult rawAnomalyResult1 = new RawAnomalyResult();
    rawAnomalyResult1.setStartTime(JodaDateTimeUtils.toDateTime("2017-07-05T00:00:00Z").getMillis());
    rawAnomalyResult1.setEndTime(JodaDateTimeUtils.toDateTime("2017-07-05T23:59:59Z").getMillis());
    AnomalyResult rawAnomalyResult2 = new RawAnomalyResult();
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
