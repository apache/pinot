package com.linkedin.thirdeye.reporting.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.reporting.api.ReportScheduler;

import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.Collection;

public class ReportingTask extends Task {
  private final ReportScheduler scheduler;

  public ReportingTask(ReportScheduler scheduler) {
    super("reporting");
    this.scheduler = scheduler;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception {

    Collection<String> actions = params.get("action");

    if (actions.size() != 1) {
      throw new IllegalArgumentException("Must provide one and only one action");
    }

    String action = actions.iterator().next();

    if ("stop".equals(action)) {
      if (scheduler.getQuartzScheduler() == null) {
        printWriter.println("Scheduler already stopped");
      } else {
        scheduler.stop();
        scheduler.setQuartzScheduler(null);
      }

    } else if ("start".equals(action)) {
      if (scheduler.getQuartzScheduler() != null) {
        printWriter.println("Scheduler already running");
      } else {
        scheduler.start();
      }

    } else {
      throw new IllegalArgumentException("Invalid action " + action);
    }

    printWriter.println("Done!");
    printWriter.flush();
  }
}
