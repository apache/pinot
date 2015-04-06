package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.managed.ThirdEyeKafkaConsumerManager;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.Collection;

public class KafkaStopTask extends Task
{
  private final ThirdEyeKafkaConsumerManager kafkaConsumerManager;

  public KafkaStopTask(ThirdEyeKafkaConsumerManager kafkaConsumerManager)
  {
    super("kafkaStop");
    this.kafkaConsumerManager = kafkaConsumerManager;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception
  {
    Collection<String> collections = params.get("collection");

    if (collections == null || collections.isEmpty())
    {
      printWriter.println("Stopping all collections ...");
      printWriter.flush();
      kafkaConsumerManager.stop(); // all
    }
    else
    {
      for (String collection : collections)
      {
        printWriter.println("Stopping " + collection + " ...");
        printWriter.flush();
        kafkaConsumerManager.stop(collection);
      }
    }

    printWriter.println("Done!");
    printWriter.flush();
  }
}
