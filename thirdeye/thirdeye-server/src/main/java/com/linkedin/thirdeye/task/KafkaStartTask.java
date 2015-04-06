package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.managed.ThirdEyeKafkaConsumerManager;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.Collection;

public class KafkaStartTask extends Task
{
  private final ThirdEyeKafkaConsumerManager kafkaConsumerManager;

  public KafkaStartTask(ThirdEyeKafkaConsumerManager kafkaConsumerManager)
  {
    super("kafkaStart");
    this.kafkaConsumerManager = kafkaConsumerManager;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception
  {
    Collection<String> collections = params.get("collection");

    if (collections == null || collections.isEmpty())
    {
      printWriter.println("Starting all collections ...");
      printWriter.flush();
      kafkaConsumerManager.start(); // all
    }
    else
    {
      for (String collection : collections)
      {
        printWriter.println("Starting " + collection + " ...");
        printWriter.flush();
        kafkaConsumerManager.start(collection);
      }
    }

    printWriter.println("Done!");
    printWriter.flush();
  }
}
