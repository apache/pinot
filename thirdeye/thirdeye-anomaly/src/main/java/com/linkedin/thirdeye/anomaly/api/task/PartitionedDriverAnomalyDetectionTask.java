package com.linkedin.thirdeye.anomaly.api.task;

import java.util.concurrent.Callable;

import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionHistory;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.client.ThirdEyeClient;

/**
 *
 */
public class PartitionedDriverAnomalyDetectionTask extends AbstractBaseAnomalyDetectionTask implements Callable<Void>{

  /**
   * @param starTreeConfig
   * @param taskInfo
   * @param function
   * @param functionHistory
   * @param thirdEyeClient
   */
  public PartitionedDriverAnomalyDetectionTask(
      StarTreeConfig starTreeConfig,
      AnomalyDetectionTaskInfo taskInfo,
      AnomalyDetectionFunction function,
      AnomalyDetectionFunctionHistory functionHistory,
      ThirdEyeClient thirdEyeClient)
  {
    super(starTreeConfig, taskInfo, function, functionHistory, thirdEyeClient, null);
    // TODO Auto-generated constructor stub
  }

  /**
   * {@inheritDoc}
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public Void call() throws Exception {
    // TODO Auto-generated method stub
    return null;
  }


}
