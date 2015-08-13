package com.linkedin.thirdeye.anomaly.api;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyResult;
import com.linkedin.thirdeye.anomaly.exception.FunctionDidNotEvaluateException;
import com.linkedin.thirdeye.anomaly.util.DimensionSpecUtils;
import com.linkedin.thirdeye.anomaly.util.ThirdEyeRequestUtils;
import com.linkedin.thirdeye.anomaly.util.TimeGranularityUtils;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

/**
 *
 */
public class AnomalyDetectionTask implements Runnable {

  /**
   * Shared thread pool for evaluating anomalies
   */
  private static final ExecutorService SHARED_EXECUTORS = Executors.newFixedThreadPool(
      Runtime.getRuntime().availableProcessors(),
      new ThreadFactoryBuilder().setDaemon(true).build());

  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyDetectionTask.class);

  /** the name of the metric to use when estimating a dimension key's contribution to the total metric */
  private final String dimensionKeyContributionMetric;

  private final StarTreeConfig starTreeConfig;
  private final AnomalyDetectionDriverConfig driverConfig;
  private final AnomalyDetectionTaskInfo taskInfo;
  private final AnomalyDetectionFunction function;
  private final AnomalyResultHandler handler;
  private final AnomalyDetectionFunctionHistory functionHistory;
  private final ThirdEyeClient thirdEyeClient;

  /** the time range of data that the driver needs to provide to the function */
  private final TimeRange queryTimeRange;

  /** the metric specs that the driver queries the third-eye server for */
  private final List<MetricSpec> metricsRequiredByTask;

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
  public AnomalyDetectionTask(StarTreeConfig starTreeConfig, AnomalyDetectionDriverConfig driverConfig,
      AnomalyDetectionTaskInfo taskInfo, AnomalyDetectionFunction function, AnomalyResultHandler handler,
      AnomalyDetectionFunctionHistory functionHistory, ThirdEyeClient thirdEyeClient)
  {
    this.function = function;
    this.handler = handler;
    this.starTreeConfig = starTreeConfig;
    this.driverConfig = driverConfig;
    this.taskInfo = taskInfo;
    this.functionHistory = functionHistory;
    this.thirdEyeClient = thirdEyeClient;

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

    metricsRequiredByTask = getMetricsRequiredByTask(starTreeConfig, driverConfig, function);

    queryTimeRange = computeQueryTimeRange(taskInfo, function);

    // initialize function history
    functionHistory.init(queryTimeRange);
  }

  /**
   * Run anomaly detection
   */
  @Override
  public void run() {
    LOGGER.info("start executing AnomalyDetetcionTask : {}", taskInfo);
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
    LOGGER.info("done executing AnomalyDetetcionTask : {}", taskInfo);
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
        anomalyResults = function.analyze(dimensionKey, series, taskInfo.getTimeRange(),
            functionHistory.getHistoryForDimensionKey(dimensionKey));
        filterAnomalyResults(anomalyResults);
      } catch (FunctionDidNotEvaluateException e) {
        LOGGER.warn("failed to execute function - {}", function.toString(), e);
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

      LOGGER.info("finished analysis on {} between {} ({})", dimensionKey, taskInfo.getTimeRange(),
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
          String fixedDimensionValue = dimensionKey.getDimensionValue(starTreeConfig.getDimensions(),groupByDimension);
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
          LOGGER.error("encountered exception in {}", taskInfo, e);
        }
      }
    };
  }

  /**
   * @param starTreeConfig
   * @param driverConfig
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
   * @param anomalyResults
   *  The list of anomalyResults to be filtered
   */
  private void filterAnomalyResults(List<AnomalyResult> anomalyResults) {
    Iterator<AnomalyResult> it = anomalyResults.iterator();
    while (it.hasNext()) {
      AnomalyResult anomalyResult = it.next();

      boolean remove = false;

      if (anomalyResult.isAnomaly() == false) {
        remove = true;
      }

      if (taskInfo.getTimeRange().contains(anomalyResult.getTimeWindow()) == false) {
        LOGGER.debug("function produced anomaly result not in window {}", taskInfo.getTimeRange());
        remove = true;
      }

      if (remove) {
        it.remove();
      }
    }
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
      dimensions = DimensionSpecUtils.getDimensionNames(starTreeConfig.getDimensions());
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
        // this should not happen, because we already maxed over already fixed dimensions
        throw new IllegalStateException();
      }
      dimensionsToGroupBy.add(dimensionName);
    }
    return dimensionsToGroupBy;
  }

  /**
   * @param dimensionValues
   * @return
   * @throws Exception
   */
  private Map<DimensionKey, MetricTimeSeries> getDataset(Map<String, String> fixedDimensionValues,
      String groupByDimension) throws Exception {
    ThirdEyeRequest request = ThirdEyeRequestUtils.buildRequest(starTreeConfig.getCollection(),
        groupByDimension, fixedDimensionValues, metricsRequiredByTask, function.getAggregationTimeGranularity(),
        queryTimeRange);
    return thirdEyeClient.execute(request);
  }


  /**
   * Perform basic sanity check on the dataset and log warnings if failed.
   *
   * @param dataset
   */
  private void sanityCheckDataset(Map<DimensionKey, MetricTimeSeries> dataset) {
    TimeRange taskTimeRange = taskInfo.getTimeRange();
    for (DimensionKey dimensionKey : dataset.keySet()) {
      MetricTimeSeries metricTimeSeries = dataset.get(dimensionKey);
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
   * @param dimensionKeyContribution
   *  The estimated contribution of the dimension key
   * @param dimensionKey
   *  The dimension key that produced the anomaly results
   * @param anomalyResults
   *  List of anomaly results for current detection interval
   */
  private void handleAnomalyResults(DimensionKey dimensionKey,
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

      // handle the anomaly result
      try {
        handler.handle(taskInfo, dimensionKey, dimensionKeyContribution, metrics, anomalyResult);
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
  private static Map<DimensionKey, Double> computeDatasetProportions(Map<DimensionKey, MetricTimeSeries> dataset,
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
   * @param taskInfo
   * @param function
   * @return
   */
  private static TimeRange computeQueryTimeRange(AnomalyDetectionTaskInfo taskInfo, AnomalyDetectionFunction function) {
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
    return new TimeRange(start, end);
  }

}
