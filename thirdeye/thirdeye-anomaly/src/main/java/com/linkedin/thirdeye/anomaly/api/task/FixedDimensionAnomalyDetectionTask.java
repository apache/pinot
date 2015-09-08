package com.linkedin.thirdeye.anomaly.api.task;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionHistoryNoOp;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.client.ThirdEyeClient;

/**
 * Run anomaly detection on a fixed dimension combination
 */
public class FixedDimensionAnomalyDetectionTask extends AbstractBaseAnomalyDetectionTask
  implements Callable<List<AnomalyResult>> {

  private final Map<String, String> fixedDimensionValues;

  /**
   * @param starTreeConfig
   * @param taskInfo
   * @param function
   * @param thirdEyeClient
   * @param dimensionValues
   */
  public FixedDimensionAnomalyDetectionTask(
      StarTreeConfig starTreeConfig,
      AnomalyDetectionTaskInfo taskInfo,
      AnomalyDetectionFunction function,
      ThirdEyeClient thirdEyeClient,
      Map<String, String> fixedDimensionValues)
  {
    super(starTreeConfig, taskInfo, function, AnomalyDetectionFunctionHistoryNoOp.sharedInstance(), thirdEyeClient);
    this.fixedDimensionValues = fixedDimensionValues;
  }

  /**
   * {@inheritDoc}
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public List<AnomalyResult> call() throws Exception {
    Map<DimensionKey, MetricTimeSeries> dataset = getDataset(fixedDimensionValues, null);
    if (dataset.size() != 1) {
      throw new IllegalStateException("fixed dimension query should just return 1 series");
    }
    Entry<DimensionKey, MetricTimeSeries> entry = dataset.entrySet().iterator().next();
    return analyze(entry.getKey(), entry.getValue());
  }
}
