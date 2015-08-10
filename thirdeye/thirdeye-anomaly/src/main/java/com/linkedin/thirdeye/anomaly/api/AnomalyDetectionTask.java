package com.linkedin.thirdeye.anomaly.api;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyResult;
import com.linkedin.thirdeye.anomaly.server.ThirdEyeServerQueryUtils;
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
public abstract class AnomalyDetectionTask implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyDetectionTask.class);

  /** dimension value SqlUtils uses to express a group by. */
  protected static final String GROUP_BY_VALUE = "!";

  /** the name of the metric to use when estimating a dimension key's contribution to the total metric */
  protected final String dimensionKeyContributionMetric;

  protected final StarTreeConfig starTreeConfig;
  protected final AnomalyDetectionDriverConfig collectionDriverConfig;
  protected final AnomalyDetectionTaskInfo taskInfo;
  protected final AnomalyDetectionFunction function;
  protected final AnomalyResultHandler handler;
  protected final AnomalyDetectionFunctionHistory functionHistory;

  /** the time range of data that the driver needs to provide to the function */
  protected final TimeRange queryTimeRange;

  /** the metric specs that the driver queries the third-eye server for */
  protected final List<MetricSpec> metricsRequiredByTask;

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
   * @param functionHistory
   *  A history of all anomaly results produced by this function
   */
  public AnomalyDetectionTask(StarTreeConfig starTreeConfig, AnomalyDetectionDriverConfig collectionDriverConfig,
      AnomalyDetectionTaskInfo taskInfo, AnomalyDetectionFunction function, AnomalyResultHandler handler,
      AnomalyDetectionFunctionHistory functionHistory) {
    this.function = function;
    this.handler = handler;
    this.starTreeConfig = starTreeConfig;
    this.collectionDriverConfig = collectionDriverConfig;
    this.taskInfo = taskInfo;
    this.functionHistory = functionHistory;

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

    queryTimeRange = computeQueryTimeRange(taskInfo, function);

    // initialize function history
    functionHistory.init(queryTimeRange);
  }

  /**
   * @param anomalyResults
   *  The list of anomalyResults to be filtered
   */
  protected void filterAnomalyResults(List<AnomalyResult> anomalyResults) {
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
  protected Set<String> getDimensionKeysToGroupBy(Map<String, String> dimensionValues) {
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
   * @param dimensionValues
   * @return
   * @throws IOException
   */
  protected AnomalyDetectionDataset getDataset(Map<String, String> dimensionValues) throws IOException {
    return ThirdEyeServerQueryUtils.runQuery(collectionDriverConfig, dimensionValues, starTreeConfig.getDimensions(),
        metricsRequiredByTask, function.getAggregationTimeGranularity(), queryTimeRange);
  }


  /**
   * Perform basic sanity check on the dataset and log warnings if failed.
   *
   * @param dataset
   */
  protected void sanityCheckDataset(AnomalyDetectionDataset dataset) {
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
  protected static Map<DimensionKey, Double> computeDatasetProportions(AnomalyDetectionDataset dataset,
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
