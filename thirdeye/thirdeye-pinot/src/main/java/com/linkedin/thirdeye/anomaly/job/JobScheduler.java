package com.linkedin.thirdeye.anomaly.job;

import java.util.List;

import org.quartz.SchedulerException;

public interface JobScheduler {

  public List<String> getActiveJobs() throws SchedulerException;

  public void start() throws SchedulerException;

  public void stop() throws SchedulerException;

  public void startJob(Long id) throws SchedulerException;

  public void stopJob(Long id) throws SchedulerException;


}
