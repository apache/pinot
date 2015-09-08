package com.linkedin.thirdeye.anomaly.api.task;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionHistory;
import com.linkedin.thirdeye.anomaly.api.AnomalyResultHandler;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;
import com.linkedin.thirdeye.anomaly.driver.DimensionKeySeries;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.client.ThirdEyeClient;

/**
 * This implementation executes a work list of dimensionKeySeries, which are pairs of a dimensionKey and a contribution estimate.
 */
public class WorkListAnomalyDetectionTask extends AbstractBaseAnomalyDetectionTask implements Runnable {

  /**
   * Shared thread pool for evaluating anomalies
   */
  private static final ExecutorService SHARED_EXECUTORS = Executors.newFixedThreadPool(
      Runtime.getRuntime().availableProcessors(), new ThreadFactoryBuilder().setDaemon(true).build());

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkListAnomalyDetectionTask.class);

  private final AnomalyResultHandler handler;
  private final List<DimensionKeySeries> dimensionKeySeries;

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
   * @param dimensionKeySeries
   *  List of dimensionKey and meta-info pairs.
   */
  public WorkListAnomalyDetectionTask(
      StarTreeConfig starTreeConfig,
      AnomalyDetectionTaskInfo taskInfo,
      AnomalyDetectionFunction function,
      AnomalyResultHandler handler,
      AnomalyDetectionFunctionHistory functionHistory,
      ThirdEyeClient thirdEyeClient,
      List<DimensionKeySeries> dimensionKeySeries)
  {
    super(starTreeConfig, taskInfo, function, functionHistory, thirdEyeClient);
    this.handler = handler;
    this.dimensionKeySeries = dimensionKeySeries;
  }

  /**
   * {@inheritDoc}
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public void run() {
    LOGGER.info("begin executing AnomalyDetectionTask : {}", getTaskInfo());
    List<Future<?>> futures = new LinkedList<>();

    for (final DimensionKeySeries dimensionKeySeries : dimensionKeySeries) {
      futures.add(SHARED_EXECUTORS.submit(new Runnable() {
        @Override
        public void run() {
          try {
            LOGGER.info("analyzing {}", dimensionKeySeries);
            runFunctionForSeries(dimensionKeySeries.getDimensionKey(), dimensionKeySeries.getContributionEstimate());
          } catch (Exception e) {
            LOGGER.error("problem failed to complete {} for {}", getTaskInfo(), dimensionKeySeries, e);
          }
        }
      }));
    }

    try {
      for (Future<?> f : futures) {
        f.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      for (Future<?> f : futures) {
        f.cancel(false);
      }
    }

    LOGGER.info("done executing AnomalyDetectionTask : {}", getTaskInfo());
  }

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

}
