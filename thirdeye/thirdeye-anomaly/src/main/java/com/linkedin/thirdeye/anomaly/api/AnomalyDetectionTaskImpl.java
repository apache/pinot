package com.linkedin.thirdeye.anomaly.api;


import java.io.IOException;
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
import com.linkedin.thirdeye.anomaly.util.DimensionKeyUtils;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;


/**
 *
 */
public class AnomalyDetectionTaskImpl extends AnomalyDetectionTask {

  /**
   * If more than this proportion of the evaluated dimension keys under a dimension group by are anomalous, then it is
   * likely that the anomaly lies in another dimension
   */
  private static final double PRUNING_MAX_NUMBER_ANOMALOUS_DIMENSION_KEYS_THRESHOLD = 0.8;

  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyDetectionTaskImpl.class);

  /**
   * @param starTreeConfig
   * @param collectionDriverConfig
   * @param taskInfo
   * @param function
   * @param handler
   * @param functionHistory
   */
  public AnomalyDetectionTaskImpl(StarTreeConfig starTreeConfig, AnomalyDetectionDriverConfig collectionDriverConfig,
      AnomalyDetectionTaskInfo taskInfo, AnomalyDetectionFunction function, AnomalyResultHandler handler,
      AnomalyDetectionFunctionHistory functionHistory) {
    super(starTreeConfig, collectionDriverConfig, taskInfo, function, handler, functionHistory);
  }

  /**
   * Run anomaly detection
   */
  @Override
  public void run() {
    try {
      // start exploring from top level
      searchAndRun(new HashMap<String, String>(), null, new AnomalyTimeSeries(), 1.0);
    } catch (Exception e) {
      LOGGER.error("did not complete {}", taskInfo, e);
    }
  }

  private void searchAndRun(Map<String, String> dimensionValues, String groupByDimension, AnomalyTimeSeries anomalies,
      double proportionMultiplier) throws Exception
  {
    // Base case : driver chooses to stop exploring
    boolean shouldContinueExploring = true;

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

    /*
     * The dimension keys that were evaluated.
     */
    Set<DimensionKey> dimensionKeysEvaluated = new HashSet<>();
    Set<DimensionKey> dimensionKeysWithAnomalies = new HashSet<>();

    for (DimensionKey dimensionKey : dataset.getDimensionKeys()) {

      /*
       * Skip dimension keys that are too sparse.
       */
      if (dimensionKeyContributionMap != null) {
        double proportion = dimensionKeyContributionMap.get(dimensionKey) * proportionMultiplier;
        if (proportion <= collectionDriverConfig.getContributionMinProportion()) {
          LOGGER.info("skipping series {} - proportion ({}) below threshold", dimensionKey,
              String.format("%.4f", proportion));
          continue;
        } else {
          LOGGER.info("analyzing series {} - proportion ({})", dimensionKey, String.format("%.4f", proportion));
        }
      }

      LOGGER.info("evaluating series for key {}", dimensionKey);
      MetricTimeSeries series = dataset.getMetricTimeSeries(dimensionKey);

      List<AnomalyResult> anomalyResults = null;
      try {
        anomalyResults = function.analyze(dimensionKey, series, taskInfo.getTimeRange(),
            functionHistory.getHistoryForDimensionKey(dimensionKey));
        filterAnomalyResults(anomalyResults);
      } catch (FunctionDidNotEvaluateException e) {
        LOGGER.warn("failed to execute function - {}", function.toString(), e);
        continue;
      }

      // publish anomalies to database
      handleAnomalyResults(anomalies, dimensionKey, dimensionKeyContributionMap.get(dimensionKey), anomalyResults);

      /*
       * Update dimensions evaluated map.
       */
      dimensionKeysEvaluated.add(dimensionKey);
      if (anomalyResults.isEmpty() == false) {
        dimensionKeysWithAnomalies.add(dimensionKey);
      }

      LOGGER.info("finished analysis on {} between {} ({})", dimensionKey, taskInfo.getTimeRange(),
          DateTimeZone.getDefault());
    }

    /*
     * Begin heuristics to decide whether to keep exploring.
     */

    if (collectionDriverConfig.isPruneExplortaionUsingFeedback()) {
      int numDimensionKeysEvaluated = dimensionKeysEvaluated.size();
      int numDimensionKeysWithAnomalies = dimensionKeysWithAnomalies.size();
      // too many anomalies in too many series, the problem lies in another dimension
      if (numDimensionKeysWithAnomalies / (double) numDimensionKeysEvaluated >=
          PRUNING_MAX_NUMBER_ANOMALOUS_DIMENSION_KEYS_THRESHOLD) {
        shouldContinueExploring = false;
        LOGGER.info("stop exploring because group by produced many anomalous series");
      }
    }

    /*
     * End heuristics.
     */

    // max recursion depth reached
    if (dimensionValues.size() >= collectionDriverConfig.getMaxExplorationDepth()) {
      shouldContinueExploring = false;
    }

    if (shouldContinueExploring) {
      for (DimensionKey dimensionKey : dimensionKeysEvaluated) {
        // do not continue exploring this dimension key
        if (collectionDriverConfig.isPruneExplortaionUsingFeedback()
            && dimensionKeysWithAnomalies.contains(dimensionKey)) {
          LOGGER.info("pruned computation due to anomaly in enclosing series");
          continue;
        }

        // fix a dimension
        if (groupByDimension != null) {
          String fixedDimensionValue = dimensionKey.getDimensionValue(starTreeConfig.getDimensions(),groupByDimension);
          LOGGER.info("fixing dimension '{}' to '{}'", groupByDimension, fixedDimensionValue);
          dimensionValues.put(groupByDimension, fixedDimensionValue);
        }

        // dive into the next group by
        for (String nextGroupByDimension : getDimensionKeysToGroupBy(dimensionValues)) {
          LOGGER.info("grouping by '{}'", nextGroupByDimension);
          dimensionValues.put(nextGroupByDimension, GROUP_BY_VALUE);
          searchAndRun(dimensionValues, nextGroupByDimension, anomalies,
              proportionMultiplier * dimensionKeyContributionMap.get(dimensionKey));
          dimensionValues.remove(nextGroupByDimension);
        }
      }
    }
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
      if (!taskInfo.getTimeRange().contains(anomalyResult.getTimeWindow())) {
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
   * @param primaryAnomalies
   * @param dimensionKey
   * @param anomalyResult
   * @return
   *  Whether the anomaly should be suppressed.
   */
  private boolean shouldSupressAnomalyResult(AnomalyTimeSeries anomalies, DimensionKey dimensionKey,
      AnomalyResult anomalyResult) {
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
  protected class AnomalyTimeSeries {

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
