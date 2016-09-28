package com.linkedin.thirdeye.anomaly.job;

import java.util.List;

import org.quartz.SchedulerException;

public interface JobScheduler {

  public List<String> getScheduledJobs() throws SchedulerException;

  public void start() throws SchedulerException;

  public void shutdown() throws SchedulerException;

  public void startJob(Long id) throws SchedulerException;

  public void stopJob(Long id) throws SchedulerException;


}
