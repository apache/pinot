package com.linkedin.thirdeye.anomaly.api;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyResult;
import com.linkedin.thirdeye.anomaly.exception.FunctionDidNotEvaluateException;
import com.linkedin.thirdeye.anomaly.server.ThirdEyeServerQueryUtils;
import com.linkedin.thirdeye.anomaly.util.DimensionKeyUtils;
import com.linkedin.thirdeye.anomaly.util.TimeGranularityUtils;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;


/**
 *
 */
public class AnomalyDetectionTask implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyDetectionTask.class);

  /** dimension value SqlUtils uses to express a group by. */
  private static final String GROUP_BY_VALUE = "!";

  /** the name of the metric to use when estimating a dimension key's contribution to the total metric */
  private final String dimensionKeyContributionMetric;

  private final StarTreeConfig starTreeConfig;
  private final AnomalyDetectionDriverConfig collectionDriverConfig;
  private final AnomalyDetectionTaskInfo taskInfo;
  private final AnomalyDetectionFunction function;
  private final AnomalyResultHandler handler;

  private final List<MetricSpec> metricsRequiredByTask;

  /**
   * @param starTreeConfig
   *  Configuration for the star-tree
   * @param collectionDriverConfig
   *  Configuration for the driver
   * @param taskInfo
   *  Information identifying the task
   * @param function
   *  Anomaly detection function to execute
   * @param handler
   *  Handler for any anomaly results
   */
  public AnomalyDetectionTask(StarTreeConfig starTreeConfig, AnomalyDetectionDriverConfig collectionDriverConfig,
      AnomalyDetectionTaskInfo taskInfo, AnomalyDetectionFunction function, AnomalyResultHandler handler) {
    this.function = function;
    this.handler = handler;
    this.starTreeConfig = starTreeConfig;
    this.collectionDriverConfig = collectionDriverConfig;
    this.taskInfo = taskInfo;

    /*
     * Use the metric specified in the config
     */
    if (collectionDriverConfig.getContributionEstimateMetric() != null) {
      dimensionKeyContributionMetric = collectionDriverConfig.getContributionEstimateMetric();
    } else {
      /*
       * Use the first metric in the collection if no metric is specified.
       */
      LOGGER.warn("no metric provided for thresholding collection {}", starTreeConfig.getCollection());
      dimensionKeyContributionMetric = starTreeConfig.getMetrics().get(0).getName();
    }

    metricsRequiredByTask = getMetricsRequiredByTask(starTreeConfig, collectionDriverConfig, function);
  }

  /**
   * Run anomaly detection
   */
  @Override
  public void run() {
    try {
      // start exploring from top level
      searchAndRun(new HashMap<String, String>(), new AnomalyTimeSeries());
    } catch (Exception e) {
      LOGGER.error("did not complete {}", taskInfo, e);
    }
  }

  /**
   * @param dimensionValues
   *  The group by and where clause values for dimensions
   * @param anomalies
   *  A map of timeWindows to a set of anomalous dimensionKeys
   * @throws IOException
   */
  private void searchAndRun(Map<String, String> dimensionValues, AnomalyTimeSeries anomalies) throws Exception
  {
    int explorationDepth = getExplorationDepth(dimensionValues);

    AnomalyDetectionDataset dataset = getDataset(dimensionValues);

    /*
     * Verify that the dataset returned the values we expected.
     */
    sanityCheckDataset(dataset);

    /*
     * Estimate each dimension key's contribution to the metric total using the configured metric or the first
     * metric in metrics.
     */
    Map<DimensionKey, Double> dimensionKeyContributionMap = computeDatasetProportions(dataset,
        dimensionKeyContributionMetric);

    for (DimensionKey dimensionKey : dataset.getDimensionKeys()) {

      /*
       * Skip dimension keys that are too sparse.
       */
      if (dimensionKeyContributionMap != null) {
        double proportion = dimensionKeyContributionMap.get(dimensionKey);
        if (proportion <= collectionDriverConfig.getContributionMinProportion()) {
          LOGGER.info("skipping series {} - proportion ({}) below threshold", dimensionKey,
              String.format("%.4f", proportion));
          continue;
        }
      }

      LOGGER.info("evaluating series for key {}", dimensionKey);
      MetricTimeSeries series = dataset.getMetricTimeSeries(dimensionKey);

      List<AnomalyResult> anomalyResults = null;
      try {
        anomalyResults = function.analyze(dimensionKey, series, taskInfo.getTimeRange());
      } catch (FunctionDidNotEvaluateException e) {
        LOGGER.warn("failed to execute function - {}", function.toString(), e);
        continue;
      }

      handleAnomalyResults(anomalies, dimensionKey, dimensionKeyContributionMap.get(dimensionKey), anomalyResults);

      LOGGER.info("analysis on {} between {} ({}) finished", dimensionKey, taskInfo.getTimeRange(),
          DateTimeZone.getDefault());
    }

    /*
     * Currently only one level of depth is supported.
     */
    if (explorationDepth >= collectionDriverConfig.getMaxExplorationDepth()) {
      return;
    } else {
      for (String dimension : getDimensionKeysToGroupBy(dimensionValues)) {
        dimensionValues.put(dimension, GROUP_BY_VALUE);
        searchAndRun(dimensionValues, anomalies);
        dimensionValues.remove(dimension);
      }
    }
  }

  /**
   * @param dimensionValues
   * @return
   *  The number of group by applied. This is the recursion depth.
   */
  private int getExplorationDepth(Map<String, String> dimensionValues) {
    int count = 0;
    for (String value : dimensionValues.values()) {
      if (GROUP_BY_VALUE.equals(value)) {
        count++;
      }
    }
    return count;
  }

  /**
   * @param dimensionValues
   * @return
   *  Returns the list dimension names to be explored
   */
  private Set<String> getDimensionKeysToGroupBy(Map<String, String> dimensionValues) {
    Set<String> dimensions = new HashSet<>();
    for (DimensionSpec dimensionSpec : starTreeConfig.getDimensions()) {
      String dimension = dimensionSpec.getName();
      if (dimensionValues.containsKey(dimension) || collectionDriverConfig.isNeverExploreDimension(dimension)) {
        continue;
      }
      dimensions.add(dimension);
    }
    return dimensions;
  }

  /**
   * @param anomalies
   *  Any existing anomalies that have been raised in this run
   * @param dimensionKeyContribution
   *  The estimated contribution of the dimension key
   * @param dimensionKey
   *  The dimension key that produced the anomaly results
   * @param anomalyResults
   *  List of anomaly results for current detection interval
   */
  private void handleAnomalyResults(AnomalyTimeSeries anomalies, DimensionKey dimensionKey,
      double dimensionKeyContribution, List<AnomalyResult> anomalyResults)
  {
    Set<String> metrics = function.getMetrics();

    /*
     * Sort the results by time window. This makes for cleaner anomaly ids.
     */
    Collections.sort(anomalyResults);

    for (AnomalyResult anomalyResult : anomalyResults) {
      /*
       * Only report anomalies in the specified time range
       */
      if (taskInfo.getTimeRange().contains(anomalyResult.getTimeWindow()) == false) {
        LOGGER.debug("function produced anomaly result not in window {}", taskInfo.getTimeRange());
        continue;
      }

      /*
       * Decide whether to suppress on this anomaly or not
       */
      if (shouldSupressAnomalyResult(anomalies, dimensionKey, anomalyResult)) {
        continue;
      } else {
        anomalies.put(anomalyResult.getTimeWindow(), dimensionKey);
      }

      // handle the anomaly result
      try {
        handler.handle(taskInfo, dimensionKey, dimensionKeyContribution, metrics, anomalyResult);
      } catch (IOException e) {
        LOGGER.error("failed to handle AnomalyResult from {}", dimensionKey);
      }
    }
  }

  /**
   * @param dimensionValues
   * @return
   * @throws IOException
   */
  private AnomalyDetectionDataset getDataset(Map<String, String> dimensionValues) throws IOException {
    long end = taskInfo.getTimeRange().getEnd();
    TimeGranularity functionGranularity = function.getTrainingWindowTimeGranularity();
    long start;
    if (functionGranularity != null) {
      // compute the start time of the dataset
      start = taskInfo.getTimeRange().getStart() - TimeGranularityUtils.toMillis(functionGranularity)
          - TimeGranularityUtils.toMillis(function.getAggregationTimeGranularity());
    } else {
      start = 0; // all time
    }
    TimeRange queryTimeRange = new TimeRange(start, end);

    return ThirdEyeServerQueryUtils.runQuery(collectionDriverConfig, dimensionValues, metricsRequiredByTask,
        function.getAggregationTimeGranularity(), queryTimeRange);
  }

  /**
   * Perform basic sanity check on the dataset and log warnings if failed.
   *
   * @param dataset
   */
  private void sanityCheckDataset(AnomalyDetectionDataset dataset) {
    TimeRange taskTimeRange = taskInfo.getTimeRange();
    for (DimensionKey dimensionKey : dataset.getDimensionKeys()) {
      MetricTimeSeries metricTimeSeries = dataset.getMetricTimeSeries(dimensionKey);
      Set<Long> seriesTimeWindowSet = metricTimeSeries.getTimeWindowSet();
      if (seriesTimeWindowSet.contains(taskTimeRange.getStart()) == false) {
        LOGGER.warn("dataset series {} does not contain expected start time window {}", dimensionKey,
            taskTimeRange.getStart());
      }
      long lastExpectedTimeWindow =
          taskTimeRange.getEnd() - TimeGranularityUtils.toMillis(function.getAggregationTimeGranularity());
      if (lastExpectedTimeWindow != taskTimeRange.getStart() &&
          seriesTimeWindowSet.contains(lastExpectedTimeWindow) == false) {
        LOGGER.warn("dataset series {} does not contain last expected time window {}", dimensionKey,
            lastExpectedTimeWindow);
      }
    }
  }

  /**
   * @param dataset
   * @param metricName
   * @return
   *  The proportion that each dimension key contributes to the metric for the timeseries.
   */
  private static Map<DimensionKey, Double> computeDatasetProportions(AnomalyDetectionDataset dataset,
      String metricName) {
    Map<DimensionKey, Double> result = new HashMap<>();
    int metricIndex = dataset.getMetrics().indexOf(metricName);
    double totalSum = 0;
    for (DimensionKey dk : dataset.getDimensionKeys()) {
      double dkValue = dataset.getMetricTimeSeries(dk).getMetricSums()[metricIndex].doubleValue();
      result.put(dk, dkValue);
      totalSum += dkValue;
    }
    for (DimensionKey dk : dataset.getDimensionKeys()) {
      result.put(dk, result.get(dk) / totalSum);
    }
    return result;
  }

  /**
   * @param starTreeConfig
   * @param collectionDriverConfig
   * @param function
   * @return
   *  A list of metrics needed by the task to complete
   */
  private ArrayList<MetricSpec> getMetricsRequiredByTask(StarTreeConfig starTreeConfig,
      AnomalyDetectionDriverConfig collectionDriverConfig, AnomalyDetectionFunction function) {
    ArrayList<MetricSpec> metrics = new ArrayList<MetricSpec>();
    Set<String> metricNames = function.getMetrics();
    metricNames.add(dimensionKeyContributionMetric);
    for (String metricName : metricNames) {
      MetricSpec metricSpec = findMetricSpec(metricName, starTreeConfig.getMetrics());
      if (metricSpec == null) { // not found, create it
        metricSpec = new MetricSpec(metricName, MetricType.DOUBLE);
      }
      metrics.add(metricSpec);
    }
    return metrics;
  }

  /**
   * @param metricName
   * @param metricSpecs
   * @return
   */
  private static MetricSpec findMetricSpec(String metricName, List<MetricSpec> metricSpecs) {
    for (MetricSpec metricSpec : metricSpecs) {
      if (metricName.equals(metricSpec.getName())) {
        return metricSpec;
      }
    }
    return null;
  }

  /**
   * @param primaryAnomalies
   * @param dimensionKey
   * @param anomalyResult
   * @return
   *  Whether the anomaly should be suppressed.
   */
  private boolean shouldSupressAnomalyResult(AnomalyTimeSeries anomalies, DimensionKey dimensionKey,
      AnomalyResult anomalyResult) {

    if (anomalyResult.isAnomaly() == false) {
      return true;
    }

    long anomalyTimeWindow = anomalyResult.getTimeWindow();

    /*
     * Suppress reporting of the anomaly if an anomaly was already reported for a series that contains the current
     * series.
     */
    if (collectionDriverConfig.isSuppressSecondaryAnomalies()) {
      boolean isSecondaryAnomaly = false;
      if (anomalies.getTimeWindowSet().contains(anomalyTimeWindow)) {
        for (DimensionKey anomalousDimensionKey : anomalies.getDimensionKeySet(anomalyTimeWindow)) {
          if (DimensionKeyUtils.isContainedWithin(anomalousDimensionKey, dimensionKey)) {
            isSecondaryAnomaly = true;
            break;
          }
        }
      }
      return isSecondaryAnomaly;
    }

    return false;
  }

  /**
   * Data structure for storing anomalies by timeWindow.
   */
  public class AnomalyTimeSeries {

    Map<Long, Set<DimensionKey>> timeWindowToAnomalies = new HashMap<>();

    public void put(long timeWindow, DimensionKey dimensionKey) {;
      if (timeWindowToAnomalies.containsKey(timeWindow)) {
        timeWindowToAnomalies.get(timeWindow).add(dimensionKey);
      } else {
        Set<DimensionKey> anomalousDimensionsAtTimeWindow = new HashSet<>();
        anomalousDimensionsAtTimeWindow.add(dimensionKey);
        timeWindowToAnomalies.put(timeWindow, anomalousDimensionsAtTimeWindow);
      }
    }

    public Set<Long> getTimeWindowSet() {
      return timeWindowToAnomalies.keySet();
    }

    public Set<DimensionKey> getDimensionKeySet(long timeWindow) {
      return timeWindowToAnomalies.get(timeWindow);
    }

  }
}
