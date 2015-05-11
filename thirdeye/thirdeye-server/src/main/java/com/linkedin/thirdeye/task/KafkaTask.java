package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.managed.KafkaConsumerManager;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.Collection;

public class KafkaTask extends Task {
  private final KafkaConsumerManager manager;

  public KafkaTask(KafkaConsumerManager manager) {
    super("kafka");
    this.manager = manager;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception {
    Collection<String> collections = params.get("collection");
    Collection<String> actions = params.get("action");

    if (actions.size() != 1) {
      throw new IllegalArgumentException("Must provide one and only one action");
    }

    String action = actions.iterator().next();

    if ("stop".equals(action)) {
      if (collections.isEmpty()) {
        printWriter.println("Stopping all collections...");
        printWriter.flush();
        manager.stop();
      } else {
        for (String collection : collections) {
          printWriter.println("Stopping " + collection + "...");
          printWriter.flush();
          manager.stop(collection);
        }
      }
    } else if ("start".equals(action)) {
      if (collections.isEmpty()) {
        printWriter.println("Starting all collections...");
        printWriter.flush();
        manager.start();
      } else {
        for (String collection : collections) {
          printWriter.println("Starting " + collection + "...");
          printWriter.flush();
          manager.start(collection);
        }
      }
    } else {
      throw new IllegalArgumentException("Invalid action " + action);
    }

    printWriter.println("Done!");
    printWriter.flush();
  }
}
