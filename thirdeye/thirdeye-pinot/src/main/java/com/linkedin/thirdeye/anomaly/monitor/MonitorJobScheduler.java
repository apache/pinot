package com.linkedin.thirdeye.anomaly.monitor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;

public class MonitorJobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobScheduler.class);

  private ScheduledExecutorService scheduledExecutorService;

  private JobManager anomalyJobDAO;
  private TaskManager anomalyTaskDAO;
  private MonitorConfiguration monitorConfiguration;
  private MonitorJobRunner monitorJobRunner;
  private MonitorJobContext monitorJobContext;

  public MonitorJobScheduler(JobManager anomalyJobDAO, TaskManager anomalyTaskDAO,
      MonitorConfiguration monitorConfiguration) {
    this.anomalyJobDAO = anomalyJobDAO;
    this.anomalyTaskDAO = anomalyTaskDAO;
    this.monitorConfiguration = monitorConfiguration;
    scheduledExecutorService = Executors.newScheduledThreadPool(10);
  }

  public void start() {
    LOG.info("Starting monitor service");

    monitorJobContext = new MonitorJobContext();
    monitorJobContext.setAnomalyJobDAO(anomalyJobDAO);
    monitorJobContext.setAnomalyTaskDAO(anomalyTaskDAO);
    monitorJobContext.setMonitorConfiguration(monitorConfiguration);

    monitorJobRunner = new MonitorJobRunner(monitorJobContext);
    scheduledExecutorService
      .scheduleWithFixedDelay(monitorJobRunner, 0, monitorConfiguration.getMonitorFrequencyHours(), TimeUnit.HOURS);
  }

  public void stop() {
    LOG.info("Stopping monitor service");
    scheduledExecutorService.shutdown();
  }
}
