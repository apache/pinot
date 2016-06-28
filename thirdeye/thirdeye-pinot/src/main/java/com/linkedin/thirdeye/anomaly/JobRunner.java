package com.linkedin.thirdeye.anomaly;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class JobRunner implements Job{

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    //write tasks to DB
  }

}
