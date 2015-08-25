package com.linkedin.thirdeye.anomaly.api.task;


import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDriverConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionHistory;
import com.linkedin.thirdeye.anomaly.api.AnomalyResultHandler;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;
import com.linkedin.thirdeye.anomaly.exception.FunctionDidNotEvaluateException;
import com.linkedin.thirdeye.anomaly.util.DimensionSpecUtils;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.client.ThirdEyeClient;

/**
 * This class represents a implementation of the anomaly detection driver where exploration is performed locally.
 */
public class LocalDriverAnomalyDetectionTask extends AbstractBaseAnomalyDetectionTask implements Runnable {

  /**
   * Shared thread pool for evaluating anomalies
   */
  private static final ExecutorService SHARED_EXECUTORS = Executors.newFixedThreadPool(
      Runtime.getRuntime().availableProcessors(),
      new ThreadFactoryBuilder().setDaemon(true).build());

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalDriverAnomalyDetectionTask.class);

  /** the name of the metric to use when estimating a dimension key's contribution to the total metric */
  private final String dimensionKeyContributionMetric;

  private final AnomalyDetectionDriverConfig driverConfig;
  private final AnomalyResultHandler handler;

  /**
   * @param starTreeConfig
   *  Configuration for the star-tree
   * @param driverConfig
   *  Configuration for the driver
   * @param taskInfo
   *  Information identifying the task
   * @param function
   *  Anomaly detection function to execute
   * @param handler
   *  Handler for any anomaly results
   * @param functionHistory
   *  A history of all anomaly results produced by this function
   * @param thirdEyeClient
   *  The client to use to request data
   */
  public LocalDriverAnomalyDetectionTask(
      StarTreeConfig starTreeConfig,
      AnomalyDetectionDriverConfig driverConfig,
      AnomalyDetectionTaskInfo taskInfo,
      AnomalyDetectionFunction function,
      AnomalyResultHandler handler,
      AnomalyDetectionFunctionHistory functionHistory,
      ThirdEyeClient thirdEyeClient)
  {
    super(starTreeConfig, taskInfo, function, functionHistory, thirdEyeClient,
        Collections.singleton(driverConfig.getContributionEstimateMetric()));

    this.driverConfig = driverConfig;
    this.handler = handler;

    /*
     * Use the metric specified in the config
     */
    if (driverConfig.getContributionEstimateMetric() != null) {
      dimensionKeyContributionMetric = driverConfig.getContributionEstimateMetric();
    } else {
      /*
       * Use the first metric in the collection if no metric is specified.
       */
      LOGGER.warn("no metric provided for thresholding collection {}", starTreeConfig.getCollection());
      dimensionKeyContributionMetric = starTreeConfig.getMetrics().get(0).getName();
    }
  }

  /**
   * Run anomaly detection
   */
  @Override
  public void run() {
    LOGGER.info("start executing AnomalyDetetcionTask : {}", getTaskInfo());
    final LinkedBlockingQueue<Future<?>> futures = new LinkedBlockingQueue<Future<?>>();

    futures.add(SHARED_EXECUTORS.submit(getRunnableSearchAndRun(futures, null, 1.0, new HashMap<String, String>())));

    // wait for everything to finish
    while (!futures.isEmpty()) {
      try {
        futures.poll().get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
    LOGGER.info("done executing AnomalyDetetcionTask : {}", getTaskInfo());
  }


  /**
   * @param dimensionValues
   * @param groupByDimension
   * @param proportionMultiplier
   * @param futures
   *  Before returning, any additional tasks will have been added top this list of futures
   * @throws Exception
   */
  private void searchAndRun(Map<String, String> dimensionValues, String groupByDimension, double proportionMultiplier,
      final LinkedBlockingQueue<Future<?>> futures) throws Exception
  {
    // Base case : driver chooses to stop exploring
    boolean shouldContinueExploring = true;

    Map<DimensionKey, MetricTimeSeries> dataset = getDataset(dimensionValues, groupByDimension);

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

    for (DimensionKey dimensionKey : dataset.keySet()) {

      /*
       * Skip dimension keys that are too sparse.
       */
      if (dimensionKeyContributionMap != null) {
        double proportion = dimensionKeyContributionMap.get(dimensionKey) * proportionMultiplier;
        if (proportion <= driverConfig.getContributionMinProportion()) {
          LOGGER.info("skipping series {} - proportion ({}) below threshold", dimensionKey,
              String.format("%.4f", proportion));
          continue;
        } else {
          LOGGER.info("analyzing series {} - proportion ({})", dimensionKey, String.format("%.4f", proportion));
        }
      }

      LOGGER.info("evaluating series for key {}", dimensionKey);
      MetricTimeSeries series = dataset.get(dimensionKey);

      List<AnomalyResult> anomalyResults = null;
      try {
        anomalyResults = analyze(dimensionKey, series);
      } catch (FunctionDidNotEvaluateException e) {
        LOGGER.warn("failed to execute function - {}", getFunction().toString(), e);
        continue;
      }

      // publish anomalies to database
      handleAnomalyResults(dimensionKey, dimensionKeyContributionMap.get(dimensionKey), anomalyResults);

      /*
       * Update dimensions evaluated.
       */
      dimensionKeysEvaluated.add(dimensionKey);
      if (anomalyResults.isEmpty() == false) {
        dimensionKeysWithAnomalies.add(dimensionKey);
      }

      LOGGER.info("finished analysis on {} between {} ({})", dimensionKey, getTaskInfo().getTimeRange(),
          DateTimeZone.getDefault());
    }

    /*
     * Begin heuristics to decide whether to keep exploring.
     */

    // max recursion depth reached (this is the number of non-star fields in the dimension key)
    int currentExplorationDepth = dimensionValues.size();
    if (groupByDimension != null) {
      currentExplorationDepth++;
    }
    if (currentExplorationDepth >= driverConfig.getMaxExplorationDepth()) {
      shouldContinueExploring = false;
    }

    /*
     * End heuristics.
     */

    if (shouldContinueExploring) {
      /*
       * Wait for exploration further down the cube to finish.
       */
      List<Runnable> runnables = new LinkedList<>();

      Set<String> dimensionsExplored = new HashSet<>(dimensionValues.keySet());
      if (groupByDimension != null) {
        dimensionsExplored.add(groupByDimension);
      }
      Set<String> nextGroupByCandidates = getDimensionKeysToGroupBy(dimensionsExplored);

      for (DimensionKey dimensionKey : dimensionKeysEvaluated) {
        // do not continue exploring this dimension key
        if (driverConfig.isPruneExplortaionUsingFeedback()
            && dimensionKeysWithAnomalies.contains(dimensionKey)) {
          LOGGER.info("pruned computation due to anomaly in enclosing series");
          continue;
        }

        // fix a dimension
        if (groupByDimension != null) {
          String fixedDimensionValue = dimensionKey.getDimensionValue(getStarTreeConfig().getDimensions(),
              groupByDimension);
          LOGGER.info("fixing dimension '{}' to '{}'", groupByDimension, fixedDimensionValue);
          dimensionValues.put(groupByDimension, fixedDimensionValue);
        }

        // dive into the next group by
        for (final String nextGroupByDimension : nextGroupByCandidates) {
          LOGGER.info("grouping by '{}'", nextGroupByDimension);
          final double nextProportionMultiplier = proportionMultiplier * dimensionKeyContributionMap.get(dimensionKey);
          final HashMap<String, String> nextDimensionValues = new HashMap<>(dimensionValues);
          runnables.add(
              getRunnableSearchAndRun(futures, nextGroupByDimension, nextProportionMultiplier, nextDimensionValues));
        }
      }

      // run on next time series
      for (Runnable runnable : runnables) {
        futures.add(SHARED_EXECUTORS.submit(runnable));
      }
    }
  }

  /**
   * @param seriesCounter
   * @param nextGroupByDimension
   * @param nextProportionMultiplier
   * @param nextDimensionValues
   * @return
   */
  private Runnable getRunnableSearchAndRun(
      final LinkedBlockingQueue<Future<?>> futures,
      final String nextGroupByDimension,
      final double nextProportionMultiplier,
      final HashMap<String, String> nextDimensionValues)
  {
    return new Runnable() {
      @Override
      public void run() {
        try {
          searchAndRun(
              nextDimensionValues,
              nextGroupByDimension,
              nextProportionMultiplier,
              futures);
        } catch (Exception e) {
          LOGGER.error("encountered exception in {}", getTaskInfo(), e);
        }
      }
    };
  }

  /**
   * @param dimensionValues
   * @return
   *  Returns the list dimension names to be explored
   */
  private Set<String> getDimensionKeysToGroupBy(Set<String> dimensionsExplored) {
    Set<String> dimensionsToGroupBy = new HashSet<>();
    List<String> dimensions = driverConfig.getDimensionPrecedence();
    if (dimensions == null) {
      LOGGER.warn("no dimension precedence given, applying default order in star-tree. This may be highly inefficient.");
      dimensions = DimensionSpecUtils.getDimensionNames(getStarTreeConfig().getDimensions());
    }

    /*
     * By exploring the dimensions in a strict ordering, we prevent running anomaly detection on the same set of
     * dimensionValues twice.
     */
    int firstUnexploredDimensionIndex = 0;

    for (int i = 0; i < dimensions.size(); i++) {
      if (dimensionsExplored.contains(dimensions.get(i))) {
        firstUnexploredDimensionIndex = i + 1;
      }
    }

    for (int i = firstUnexploredDimensionIndex; i < dimensions.size(); i++) {
      String dimensionName = dimensions.get(i);
      if (dimensionsExplored.contains(dimensionName)) {
        // this should not happen, because we already maxed over the precedence of already fixed dimensions
        throw new IllegalStateException();
      }
      dimensionsToGroupBy.add(dimensionName);
    }
    return dimensionsToGroupBy;
  }

  /**
   * @param dimensionKeyContribution
   *  The estimated contribution of the dimension key
   * @param dimensionKey
   *  The dimension key that produced the anomaly results
   * @param anomalyResults
   *  List of anomaly results for current detection interval
   */
  private void handleAnomalyResults(DimensionKey dimensionKey, double dimensionKeyContribution,
      List<AnomalyResult> anomalyResults)
  {
    Set<String> metrics = getFunction().getMetrics();

    /*
     * Sort the results by time window. This makes for cleaner anomaly ids.
     */
    Collections.sort(anomalyResults);

    for (AnomalyResult anomalyResult : anomalyResults) {
      /*
       * Only report anomalies in the specified time range
       */
      if (!getTaskInfo().getTimeRange().contains(anomalyResult.getTimeWindow())) {
        LOGGER.debug("function produced anomaly result not in window {}", getTaskInfo().getTimeRange());
        continue;
      }

      // handle the anomaly result
      try {
        handler.handle(getTaskInfo(), dimensionKey, dimensionKeyContribution, metrics, anomalyResult);
      } catch (IOException e) {
        LOGGER.error("failed to handle AnomalyResult from {}", dimensionKey);
      }
    }
  }

  /**
   * @param dataset
   * @param metricName
   * @return
   *  The proportion that each dimension key contributes to the metric for the timeseries.
   */
  private Map<DimensionKey, Double> computeDatasetProportions(Map<DimensionKey, MetricTimeSeries> dataset,
      String metricName) {
    Map<DimensionKey, Double> result = new HashMap<>();
    double totalSum = 0;
    for (Entry<DimensionKey, MetricTimeSeries> entry : dataset.entrySet()) {
      int metricIndex = entry.getValue().getSchema().getNames().indexOf(metricName);
      double dkValue = entry.getValue().getMetricSums()[metricIndex].doubleValue();
      result.put(entry.getKey(), dkValue);
      totalSum += dkValue;
    }
    for (DimensionKey dk : dataset.keySet()) {
      result.put(dk, result.get(dk) / totalSum);
    }
    return result;
  }

}
