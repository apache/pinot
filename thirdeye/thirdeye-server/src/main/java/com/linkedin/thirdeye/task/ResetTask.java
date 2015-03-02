package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.managed.AnomalyDetectionTaskManager;
import com.linkedin.thirdeye.managed.KafkaConsumerManager;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;

public class ResetTask extends Task
{
  private final AnomalyDetectionTaskManager anomalyDetectionTaskManager;
  private final KafkaConsumerManager kafkaConsumer;

  public ResetTask(AnomalyDetectionTaskManager anomalyDetectionTaskManager, KafkaConsumerManager kafkaConsumer)
  {
    super("reset");
    this.anomalyDetectionTaskManager = anomalyDetectionTaskManager;
    this.kafkaConsumer = kafkaConsumer;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception
  {
    anomalyDetectionTaskManager.reset();
    kafkaConsumer.reset();
  }
}
