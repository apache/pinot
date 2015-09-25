package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.driver.AnomalyDetectionJobManager;
import io.dropwizard.servlets.tasks.Task;
import org.quartz.Scheduler;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;

public class AdHocAnomalyDetectionJobTask extends Task {
  private final AnomalyDetectionJobManager jobManager;

  public AdHocAnomalyDetectionJobTask(AnomalyDetectionJobManager jobManager) {
    super("adHoc");
    this.jobManager = jobManager;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception {
    Long id = Long.valueOf(params.get("id").asList().get(0));
    String windowStart = getOrNull(params, "windowStart");
    String windowEnd = getOrNull(params, "windowEnd");
    jobManager.runAdHoc(id, windowStart, windowEnd);
  }

  private static String getOrNull(ImmutableMultimap<String, String> params, String key) {
    ImmutableCollection<String> values = params.get(key);
    if (values.isEmpty()) {
      return null;
    }
    return values.asList().get(0);
  }
}
