package com.linkedin.thirdeye.anomalydetection.model.operator;

import com.google.common.collect.ListMultimap;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.detection.TimeSeriesUtil;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnomalyContribution {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyContribution.class);

  public AnomalyContribution() {

  }

  public void run(ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergedAnomalies)
      throws JobExecutionException, ExecutionException {
    Map<AnomalyFunctionDTO, List<MergedAnomalyResultDTO>>
        anomalyFunctionDTOMergedAnomalyResultDTOListMap = new HashMap<>();
    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies.values()) {
      if (!anomalyFunctionDTOMergedAnomalyResultDTOListMap.containsKey(mergedAnomaly.getFunction())) {
        anomalyFunctionDTOMergedAnomalyResultDTOListMap.put(mergedAnomaly.getFunction(), new ArrayList<MergedAnomalyResultDTO>());
      }
      anomalyFunctionDTOMergedAnomalyResultDTOListMap.get(mergedAnomaly.getFunction()).add(mergedAnomaly);
    }

    for (AnomalyFunctionDTO anomalyFunction : anomalyFunctionDTOMergedAnomalyResultDTOListMap.keySet()) {
      List<MergedAnomalyResultDTO> mergedAnomalyResultsOfFunction =
          anomalyFunctionDTOMergedAnomalyResultDTOListMap.get(anomalyFunction);
        MetricTimeSeries totalTimeSeries = fetchGlobalMetric(anomalyFunction, getMergedAnomaliesTimeRange(anomalyFunctionDTOMergedAnomalyResultDTOListMap.get(anomalyFunction)));
        for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalyResultsOfFunction) {
          mergedAnomaly.setImpactToTotal(calculateImpactToTotal(totalTimeSeries, mergedAnomaly));
        }
    }
  }

  private Pair<Long, Long> getMergedAnomaliesTimeRange (List<MergedAnomalyResultDTO> mergedAnomalies) {
    long start = Long.MAX_VALUE;
    long end = Long.MIN_VALUE;

    for(MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
      if(start > mergedAnomaly.getStartTime()) {
        start = mergedAnomaly.getStartTime();
      }
      if(end < mergedAnomaly.getEndTime()) {
        end = mergedAnomaly.getEndTime();
      }
    }
    return new Pair<>(start, end);
  }

  private MetricTimeSeries fetchGlobalMetric (AnomalyFunctionDTO anomalyFunctionSpec, Pair<Long, Long> windowTime)
      throws JobExecutionException, ExecutionException {
    // Get traffic contribution ratio of each dimension
    // Clone anomalyFunctionSpec and remove filter and dimension info
    List<Pair<Long, Long>> windowTimeRange = new ArrayList<>();
    windowTimeRange.add(windowTime);
    AnomalyFunctionDTO cloneAnomalyFunctionSpec = new AnomalyFunctionDTO();
    cloneAnomalyFunctionSpec.setCollection(anomalyFunctionSpec.getCollection());
    cloneAnomalyFunctionSpec.setMetrics(anomalyFunctionSpec.getMetrics());
    cloneAnomalyFunctionSpec.setMetricFunction(anomalyFunctionSpec.getMetricFunction());
    cloneAnomalyFunctionSpec.setBucketSize(anomalyFunctionSpec.getBucketSize());
    cloneAnomalyFunctionSpec.setBucketUnit(anomalyFunctionSpec.getBucketUnit());
    Map<DimensionKey, MetricTimeSeries> dimensionKeyMetricTimeSeriesMap =
        TimeSeriesUtil.getTimeSeriesForAnomalyDetection(cloneAnomalyFunctionSpec, windowTimeRange);
    // Calculate traffic contribution ratio
    if (dimensionKeyMetricTimeSeriesMap.size() > 1) {
      LOG.warn("More than 1 dimensions when fetching traffic data for {}; take the 1st dimension", anomalyFunctionSpec);
    }
    DimensionKey dimensionKey = dimensionKeyMetricTimeSeriesMap.keySet().iterator().next();
    return dimensionKeyMetricTimeSeriesMap.get(dimensionKey);
  }

  private double calculateImpactToTotal(MetricTimeSeries totalMetric, MergedAnomalyResultDTO mergedAnomaly) {
    Set<Long> timestamps = totalMetric.getTimeWindowSet();
    double avgTraffic = 0.0;
    int count = 0;
    for (long timestamp : timestamps) {
      if (timestamp >= mergedAnomaly.getStartTime() && timestamp <= mergedAnomaly.getEndTime()) {
        avgTraffic += totalMetric.get(timestamp, mergedAnomaly.getMetric()).doubleValue();
        count++;
      }
    }
    avgTraffic /= count;
    return (mergedAnomaly.getAvgCurrentVal() - mergedAnomaly.getAvgBaselineVal()) / avgTraffic;
  }
}
