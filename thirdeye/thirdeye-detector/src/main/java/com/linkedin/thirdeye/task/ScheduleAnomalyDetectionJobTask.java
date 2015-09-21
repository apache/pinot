package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.driver.AnomalyDetectionJobManager;
import io.dropwizard.servlets.tasks.Task;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;

import java.io.PrintWriter;

public class ScheduleAnomalyDetectionJobTask extends Task {
  private final AnomalyDetectionJobManager jobManager;

  public ScheduleAnomalyDetectionJobTask(AnomalyDetectionJobManager jobManager) {
    super("schedule");
    this.jobManager = jobManager;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception {
    String action = params.get("action").asList().get(0);
    Long id = Long.valueOf(params.get("id").asList().get(0));

    if ("start".equalsIgnoreCase(action)) {
      jobManager.start(id);
    } else if ("stop".equalsIgnoreCase(action)) {
      jobManager.stop(id);
    }
  }
}
