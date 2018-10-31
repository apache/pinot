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

import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContext;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.anomalydetection.context.RawAnomalyResult;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.detector.metric.transfer.MetricTransfer;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimeBasedAnomalyMerger {
  private final static Logger LOG = LoggerFactory.getLogger(TimeBasedAnomalyMerger.class);
  private final static Double NULL_DOUBLE = Double.NaN;

  private final MergedAnomalyResultManager mergedResultDAO;
  private final AnomalyFunctionFactory anomalyFunctionFactory;

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private static final double MERGE_ANOMALY_ERROR_THRESHOLD = 0.4; // 40%
  private static final int MAX_ERROR_MESSAGE_WORD_COUNT = 10_000; // 10k bytes

  private final static AnomalyMergeConfig DEFAULT_TIME_BASED_MERGE_CONFIG;
  static {
    DEFAULT_TIME_BASED_MERGE_CONFIG = new AnomalyMergeConfig();
    DEFAULT_TIME_BASED_MERGE_CONFIG.setSequentialAllowedGap(TimeUnit.HOURS.toMillis(2)); // merge anomalies apart 2 hours
    DEFAULT_TIME_BASED_MERGE_CONFIG.setMaxMergeDurationLength(TimeUnit.DAYS.toMillis(7) - 3600_000); // break anomaly longer than 6 days 23 hours
    DEFAULT_TIME_BASED_MERGE_CONFIG.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);
  }

  public TimeBasedAnomalyMerger(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.mergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  /**
   * Performs a time based merge, which merged anomalies that have the same function id and dimensions, etc.
   * This method is supposed to be performed by anomaly detectors right after their anomaly detection.
   *
   * Time based merge logic works as following:
   *
   * Step 1: for the given function, find all groups of raw (unprocessed) anomalies based on
   *         merge strategy (FunctionId and/or dimensions)
   *
   * Step 2: For each such group, find the base mergedAnomaly
   *
   * Step 3: perform time based merge
   *
   * Step 4: Recompute anomaly score / weight
   *
   * Step 5: persist merged anomalies
   *
   * @param functionSpec the spec of the function that detects anomalies
   *
   * @return the number of merged anomalies after merging
   */
  public ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergeAnomalies(AnomalyFunctionDTO functionSpec,
      ListMultimap<DimensionMap, AnomalyResult> unmergedAnomalies) throws Exception {

    int rawAnomaliesCount = 0;
    for (DimensionMap dimensionMap : unmergedAnomalies.keySet()) {
      rawAnomaliesCount += unmergedAnomalies.get(dimensionMap).size();
    }
    LOG.info("Running merge for function id : [{}], found [{}] raw anomalies", functionSpec.getId(), rawAnomaliesCount);

    AnomalyMergeConfig mergeConfig = functionSpec.getAnomalyMergeConfig();
    if (mergeConfig == null) {
      mergeConfig = DEFAULT_TIME_BASED_MERGE_CONFIG;
    }

    // From anomaly function spec initialize the anomaly function, and get its specified mergeable property keys if any
    // If failed to create anomaly function, default empty mergeable property keys will be used from mergeConfig
    try {
      BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(functionSpec);
      mergeConfig.setMergeablePropertyKeys(anomalyFunction.getMergeablePropertyKeys());
      LOG.info("Created anomaly function for class: {}, set mergeable keys as: {}", anomalyFunction.getClass(),
          anomalyFunction.getMergeablePropertyKeys());
    } catch (Exception e) {
      String message = String.format("Unable to create anomaly function %s from anomalyFunctionFactory.", functionSpec);
      LOG.error(message, e);
      throw new Exception(message, e);
    }


    if (unmergedAnomalies.size() == 0) {
      return ArrayListMultimap.create();
    } else {
      ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergedAnomalies =
          dimensionalShuffleAndUnifyMerge(functionSpec, mergeConfig, unmergedAnomalies);

      List<Exception> exceptions = new ArrayList<>();
      // Update information of merged anomalies
      for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies.values()) {
        try {
          computeMergedAnomalyInfo(mergedAnomaly, mergeConfig);
        } catch (Exception e) {
          String functionName = "";
          if (mergedAnomaly.getFunction() != null) {
            functionName = Strings.nullToEmpty(mergedAnomaly.getFunction().getFunctionName());
          } else if (mergedAnomaly.getFunctionId() != null) {
            functionName = mergedAnomaly.getFunctionId().toString();
          }
          long anomalyId = -1;
          if (mergedAnomaly.getId() != null) {
            anomalyId = mergedAnomaly.getId();
          }

          String message = String.format(
              "Failed to update merged anomaly info. ID: %s, dataset: %s, metric: %s, function: %s, time window:%s - %s",
              anomalyId, mergedAnomaly.getCollection(), mergedAnomaly.getMetric(), functionName,
              new DateTime(mergedAnomaly.getStartTime()), new DateTime(mergedAnomaly.getEndTime()));
          LOG.warn(message, e);
          exceptions.add(new Exception(message, e));

          if (Double.compare((double) exceptions.size() / (double) mergedAnomalies.values().size(),
              MERGE_ANOMALY_ERROR_THRESHOLD) >= 0) {
            String exceptionMessage = ThirdEyeUtils.exceptionsToString(exceptions, MAX_ERROR_MESSAGE_WORD_COUNT);
            throw new Exception(exceptionMessage);
          }
        }
      }

      return mergedAnomalies;
    }
  }

  private ListMultimap<DimensionMap, MergedAnomalyResultDTO> dimensionalShuffleAndUnifyMerge(AnomalyFunctionDTO function,
      AnomalyMergeConfig mergeConfig, ListMultimap<DimensionMap, AnomalyResult> unmergedAnomalies) {

    ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergedAnomalies = ArrayListMultimap.create();

    for (DimensionMap dimensionMap : unmergedAnomalies.keySet()) {
      List<AnomalyResult> unmergedResultsByDimensions = unmergedAnomalies.get(dimensionMap);
      long anomalyWindowStart = Long.MAX_VALUE;
      long anomalyWindowEnd = Long.MIN_VALUE;
      for (AnomalyResult unmergedResultsByDimension : unmergedResultsByDimensions) {
        anomalyWindowStart = Math.min(anomalyWindowStart, unmergedResultsByDimension.getStartTime());
        anomalyWindowEnd = Math.max(anomalyWindowEnd, unmergedResultsByDimension.getEndTime());
      }

      // NOTE: We get "latest overlapped (Conflict)" merged anomaly instead of "recent" merged anomaly in order to
      // prevent the merge results of current (online) detection interfere the merge results of back-fill (offline)
      // detection.
      // Moreover, the window start is modified by mergeConfig.getSequentialAllowedGap() in order to allow a gap between
      // anomalies to be merged.
      MergedAnomalyResultDTO latestOverlappedMergedResult =
          mergedResultDAO.findLatestOverlapByFunctionIdDimensions(function.getId(), dimensionMap.toString(),
              anomalyWindowStart - mergeConfig.getSequentialAllowedGap(), anomalyWindowEnd);

      List<MergedAnomalyResultDTO> mergedResults =
          AnomalyTimeBasedSummarizer.mergeAnomalies(latestOverlappedMergedResult, unmergedResultsByDimensions,
              mergeConfig);
      for (MergedAnomalyResultDTO mergedResult : mergedResults) {
        mergedResult.setFunction(function);
        mergedResult.setCollection(function.getCollection());
        mergedResult.setMetric(function.getTopicMetric());
      }
      LOG.info(
          "Merging [{}] raw anomalies into [{}] merged anomalies for function id : [{}] and dimensions : [{}]",
          unmergedResultsByDimensions.size(), mergedResults.size(), function.getId(), dimensionMap);
      mergedAnomalies.putAll(dimensionMap, mergedResults);
    }

    return mergedAnomalies;
  }

  /**
   * Uses function-specific method to re-computes the weight of merged anomaly.
   *
   * @param mergedAnomalies the merged anomaly to be updated
   * @param mergeConfig the merge configuration that was applied when merge the merged anomaly
   * @throws Exception if error occurs when retrieving the time series for calculating the weight
   */
  private void computeMergedAnomalyInfo(MergedAnomalyResultDTO mergedAnomalies, AnomalyMergeConfig mergeConfig)
      throws Exception {
    AnomalyFunctionDTO anomalyFunctionSpec = mergedAnomalies.getFunction();
    BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);
    DateTime windowStart = new DateTime(mergedAnomalies.getStartTime());
    DateTime windowEnd = new DateTime(mergedAnomalies.getEndTime());
    DimensionMap dimensions = mergedAnomalies.getDimensions();

    AnomalyDetectionInputContextBuilder anomalyDetectionInputContextBuilder =
        new AnomalyDetectionInputContextBuilder(anomalyFunctionFactory);
    anomalyDetectionInputContextBuilder.setFunction(anomalyFunctionSpec)
        .fetchTimeSeriesDataByDimension(windowStart, windowEnd, dimensions, false)
        .fetchScalingFactors(windowStart, windowEnd)
        .fetchExistingMergedAnomaliesByDimension(windowStart, windowEnd, dimensions);
    if (anomalyFunctionSpec.isToCalculateGlobalMetric()) {
      anomalyDetectionInputContextBuilder.fetchTimeSeriesGlobalMetric(windowStart, windowEnd);
    }
    AnomalyDetectionInputContext adInputContext = anomalyDetectionInputContextBuilder.build();

    MetricTimeSeries metricTimeSeries = adInputContext.getDimensionMapMetricTimeSeriesMap().get(dimensions);

    if (metricTimeSeries != null) {
      List<MergedAnomalyResultDTO> knownAnomalies = adInputContext.getKnownMergedAnomalies().get(dimensions);
      // Transform time series with scaling factor
      List<ScalingFactor> scalingFactors = adInputContext.getScalingFactors();
      if (CollectionUtils.isNotEmpty(scalingFactors)) {
        Properties properties = anomalyFunction.getProperties();
        MetricTransfer.rescaleMetric(metricTimeSeries, windowStart.getMillis(), scalingFactors,
            anomalyFunctionSpec.getTopicMetric(), properties);
      }
      anomalyFunction.updateMergedAnomalyInfo(mergedAnomalies, metricTimeSeries, windowStart, windowEnd, knownAnomalies);

      if(anomalyFunctionSpec.isToCalculateGlobalMetric()) {
        MetricTimeSeries subMetricTimeSeries = adInputContext.getDimensionMapMetricTimeSeriesMap().get(dimensions);
        computeImpactToGlobalMetric(adInputContext.getGlobalMetric(), subMetricTimeSeries, mergedAnomalies);
      }
    }
  }

  /**
   * Calculate the impact to global metric for the MergedAnomalyResult instance
   *
   * impact_to_global = severity * traffic_contribution
   *   \- severity is the change ratio in monitoring window
   *   \- trafic_contribution is the ratio of the average traffic of subMetric and global metric in training window, that is
   *        traffic_contribution = avg(subMetric)/avg(globalMetric)
   * @param globalMetricTimeSerise
   *      The time series data in global metric
   * @param subMetricTimeSeries
   *      The time series data in sub-metric
   * @param mergedAnomalygit
   *      The instance of merged anomaly
   */
  public static double computeImpactToGlobalMetric(MetricTimeSeries globalMetricTimeSerise,
      MetricTimeSeries subMetricTimeSeries, MergedAnomalyResultDTO mergedAnomaly) {
    double impactToTotal = NULL_DOUBLE;
    if (globalMetricTimeSerise == null || globalMetricTimeSerise.getTimeWindowSet().isEmpty()) {
      return impactToTotal;
    }

    String globalMetric = mergedAnomaly.getFunction().getGlobalMetric();
    String anomalyMetric = mergedAnomaly.getMetric();
    if (StringUtils.isNotBlank(globalMetric)) {
      globalMetric = mergedAnomaly.getFunction().getMetric();
    }
    // Calculate the traffic contribution
    double avgGlobal = 0.0;
    double avgSub = 0.0;
    int countGlobal = 0;
    int countSub = 0;
    // Calculate the average traffic
    for (long timestamp : globalMetricTimeSerise.getTimeWindowSet()) {
      if (timestamp < mergedAnomaly.getStartTime()) {
        double globalMetricValue = globalMetricTimeSerise.getOrDefault(timestamp, globalMetric, NULL_DOUBLE).doubleValue();
        double subMetricValue = subMetricTimeSeries.getOrDefault(timestamp, anomalyMetric, NULL_DOUBLE).doubleValue();
        if (Double.compare(globalMetricValue, NULL_DOUBLE) != 0) {
          avgGlobal += globalMetricValue;
          countGlobal++;
        }
        if (Double.compare(subMetricValue, NULL_DOUBLE) != 0) {
          avgSub += subMetricValue;
          countSub++;
        }
      }
    }
    avgGlobal /= (double) countGlobal;
    avgSub /= (double) countSub;

    if (avgGlobal > 0) {
      double trafficContribution = avgSub / avgGlobal;
      impactToTotal = mergedAnomaly.getWeight() * trafficContribution;
    }
    mergedAnomaly.setImpactToGlobal(impactToTotal);
    return impactToTotal;
  }
}
