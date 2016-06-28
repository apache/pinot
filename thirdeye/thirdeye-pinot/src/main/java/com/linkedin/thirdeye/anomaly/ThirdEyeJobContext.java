package com.linkedin.thirdeye.anomaly;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.linkedin.thirdeye.detector.function.AnomalyFunction;

public class ThirdEyeJobContext implements Job {

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    TaskGenerator taskGenerator = new TaskGenerator();
//    AnomalyFunction anomalyFunction =
//        (AnomalyFunction) context.getJobDetail().getJobDataMap().get(FUNCTION);
//
//    taskGenerator.createTasks(spec);
    // write tasks to DB
  }

}
