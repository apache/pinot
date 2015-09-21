package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.driver.AnomalyDetectionJobManager;
import io.dropwizard.servlets.tasks.Task;
import org.quartz.Scheduler;

import java.io.PrintWriter;

public class AdHocAnomalyDetectionJobTask extends Task {
  private final AnomalyDetectionJobManager jobManager;

  public AdHocAnomalyDetectionJobTask(AnomalyDetectionJobManager jobManager) {
    super("adHoc");
    this.jobManager = jobManager;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception {
    Long id = Long.valueOf(params.get("id").asList().get(0));
    String time = params.get("time").asList().get(0);
    jobManager.runAdHoc(id, time);
  }
}
