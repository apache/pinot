package com.linkedin.thirdeye.anomaly.api.task;

import java.util.Set;
import java.util.concurrent.Callable;

import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionHistory;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.client.ThirdEyeClient;

/**
 * @param <T>
 *  The results returned by the anomaly detection task.
 */
public abstract class CallableAnomalyDetectionTask<T> extends AbstractBaseAnomalyDetectionTask implements Callable<T>
{

  public CallableAnomalyDetectionTask(
      StarTreeConfig starTreeConfig,
      AnomalyDetectionTaskInfo taskInfo,
      AnomalyDetectionFunction function,
      AnomalyDetectionFunctionHistory functionHistory,
      ThirdEyeClient thirdEyeClient)
  {
    super(starTreeConfig, taskInfo, function, functionHistory, thirdEyeClient);
  }

  public CallableAnomalyDetectionTask(
      StarTreeConfig starTreeConfig,
      AnomalyDetectionTaskInfo taskInfo,
      AnomalyDetectionFunction function,
      AnomalyDetectionFunctionHistory functionHistory,
      ThirdEyeClient thirdEyeClient,
      Set<String> additionalMetricsRequiredByTask)
  {
    super(starTreeConfig, taskInfo, function, functionHistory, thirdEyeClient, additionalMetricsRequiredByTask);
  }

}
