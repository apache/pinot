package com.linkedin.thirdeye.anomaly.api.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDriverConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionHistory;
import com.linkedin.thirdeye.anomaly.api.AnomalyResultHandler;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.client.ThirdEyeClient;

/**
 * This implementation executes a work list of subtasks, which are pairs of a dimensionKey and a contribution estimate.
 */
public class WorkListAnomalyDetectionTask extends CallableAnomalyDetectionTask<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalDriverAnomalyDetectionTask.class);

  private final AnomalyResultHandler handler;
  private final List<SeriesSubtask> subtasks;

  /**
   * @param starTreeConfig
   * @param driverConfig
   * @param taskInfo
   * @param function
   * @param handler
   * @param functionHistory
   * @param thirdEyeClient
   */
  public WorkListAnomalyDetectionTask(
      StarTreeConfig starTreeConfig,
      AnomalyDetectionDriverConfig driverConfig,
      AnomalyDetectionTaskInfo taskInfo,
      AnomalyDetectionFunction function,
      AnomalyResultHandler handler,
      AnomalyDetectionFunctionHistory functionHistory,
      ThirdEyeClient thirdEyeClient,
      List<SeriesSubtask> subtasks)
  {
    super(starTreeConfig, taskInfo, function, functionHistory, thirdEyeClient, null);
    this.handler = handler;
    this.subtasks = subtasks;
  }

  /**
   * {@inheritDoc}
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public Void call() throws Exception {
    // explore phase
//    List<SeriesSubtask> subtasks = explore(new HashMap<String, String>(), driverConfig.getDimensionPrecedence(), 0, 1.0);

    // run phase
    for (SeriesSubtask subtask : subtasks) {
      try {
        runFunctionForSeries(subtask.getDimensionKey(), subtask.getContributionEstimate());
      } catch (Exception e) {
        LOGGER.error("problem failed to complete {} for {}", getTaskInfo(), subtask.getDimensionKey(), e);
      }
    }
    return null;
  }

  /**
   * Class representing a series to run.
   */
  public class SeriesSubtask {

    private final DimensionKey dimensionKey;
    private final double contributionEstimate;

    public SeriesSubtask(DimensionKey dimensionKey, double contributionEstimate) {
      super();
      this.dimensionKey = dimensionKey;
      this.contributionEstimate = contributionEstimate;
    }

    public double getContributionEstimate() {
      return contributionEstimate;
    }

    public DimensionKey getDimensionKey() {
      return dimensionKey;
    }

  }

  /**
   * @return
   *  List of subtasks to complete
   * @throws Exception
   */
//  private List<SeriesSubtask> explore(Map<String, String> dimensionValues, List<String> dimensionNames,
//      int groupByDimensionIndex, double proportionMultiplier) throws Exception {
//    List<SeriesSubtask> subtasks = new ArrayList<SeriesSubtask>();
//
//    long taskTimeRangeStart = getTaskInfo().getTimeRange().getStart();
//
//    for (int i = groupByDimensionIndex; i < dimensionNames.size(); i++) {
//      String groupByDimension = dimensionNames.get(i);
//      ThirdEyeRequest request = ThirdEyeRequestUtils.buildRequest(
//          driverConfig.getCollectionName(),
//          groupByDimension,
//          dimensionValues,
//          Collections.singletonList(driverConfig.getContributionEstimateMetric()),
//          new TimeGranularity(1, TimeUnit.HOURS),
//          new TimeRange(taskTimeRangeStart - TimeUnit.DAYS.toMillis(1), taskTimeRangeStart));
//      Map<DimensionKey, MetricTimeSeries> dataset = thirdEyeClient.execute(request);
//      Map<DimensionKey, Double> proportions = computeDatasetProportions(dataset,
//          driverConfig.getContributionEstimateMetric());
//
//      for (DimensionKey dimensionKey : dataset.keySet()) {
//        double scaledEstimateProportion = proportions.get(dimensionKey) * proportionMultiplier;
//        if (scaledEstimateProportion < driverConfig.getContributionMinProportion()) {
//          continue;
//        }
//
//        BigInteger dimensionKeyHash = new BigInteger(dimensionKey.toMD5());
//        if (dimensionKeyHash.mod(BigInteger.valueOf(getTaskInfo().getNumPartitions()))
//            .equals(BigInteger.valueOf(getTaskInfo().getPartitionId()))) {
//          subtasks.add(new SeriesSubtask(dimensionKey, scaledEstimateProportion));
//        }
//
//        if (dimensionValues.size() + 1 < driverConfig.getMaxExplorationDepth()
//          && scaledEstimateProportion > driverConfig.getContributionMinProportion()) {
//          dimensionValues.put(groupByDimension, dimensionKey.getDimensionValue(getStarTreeConfig().getDimensions(),
//              groupByDimension));
//          subtasks.addAll(explore(dimensionValues, dimensionNames, i + 1, scaledEstimateProportion));
//          dimensionValues.remove(groupByDimension);
//        }
//      }
//
//    }
//    return subtasks;
//  }

  /**
   * @param dimensionKey
   *  The specific dimension combination to run
   * @param contributionProportion
   *  The estimated proportion that this dimension contributes to the overall metric
   * @throws Exception
   */
  private void runFunctionForSeries(DimensionKey dimensionKey, double contributionProportion) throws Exception {
    Map<String, String> fixedDimensionValues = new HashMap<String, String>();
    List<DimensionSpec> dimensionSpecs = getStarTreeConfig().getDimensions();
    for (int i = 0; i < dimensionSpecs.size(); i++) {
      String dimensionValue = dimensionKey.getDimensionValues()[i];
      if (!dimensionValue.equals("*")) {
        fixedDimensionValues.put(dimensionSpecs.get(i).getName(), dimensionValue);
      }
    }

    Map<DimensionKey, MetricTimeSeries> dataset = getDataset(fixedDimensionValues, null);
    if (dataset.size() != 1) {
      throw new IllegalStateException("fixed dimension query should just return 1 series");
    }
    Entry<DimensionKey, MetricTimeSeries> entry = dataset.entrySet().iterator().next();

    for (AnomalyResult anomaly : analyze(entry.getKey(), entry.getValue())) {
      handler.handle(getTaskInfo(), dimensionKey, contributionProportion, getFunction().getMetrics(), anomaly);
    }
  }

  /**
   * @param dataset
   * @param metricName
   * @return
   *  The proportion that each dimension key contributes to the metric for the timeseries.
   */
//  private Map<DimensionKey, Double> computeDatasetProportions(Map<DimensionKey, MetricTimeSeries> dataset,
//      String metricName) {
//    Map<DimensionKey, Double> result = new HashMap<>();
//    double totalSum = 0;
//    for (Entry<DimensionKey, MetricTimeSeries> entry : dataset.entrySet()) {
//      int metricIndex = entry.getValue().getSchema().getNames().indexOf(metricName);
//      double dkValue = entry.getValue().getMetricSums()[metricIndex].doubleValue();
//      result.put(entry.getKey(), dkValue);
//      totalSum += dkValue;
//    }
//    for (DimensionKey dk : dataset.keySet()) {
//      result.put(dk, result.get(dk) / totalSum);
//    }
//    return result;
//  }

}
