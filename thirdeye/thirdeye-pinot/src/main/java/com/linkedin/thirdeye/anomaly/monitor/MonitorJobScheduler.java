package com.linkedin.thirdeye.anomaly.monitor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.detector.db.dao.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.dao.AnomalyTaskSpecDAO;

public class MonitorJobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobScheduler.class);

  private ScheduledExecutorService scheduledExecutorService;

  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private SessionFactory sessionFactory;
  private MonitorConfiguration monitorConfiguration;
  private MonitorJobRunner monitorJobRunner;
  private MonitorJobContext monitorJobContext;

  public MonitorJobScheduler(AnomalyJobSpecDAO anomalyJobSpecDAO, AnomalyTaskSpecDAO anomalyTaskSpecDAO,
      SessionFactory sessionFactory, MonitorConfiguration monitorConfiguration) {
    this.anomalyJobSpecDAO = anomalyJobSpecDAO;
    this.anomalyTaskSpecDAO = anomalyTaskSpecDAO;
    this.sessionFactory = sessionFactory;
    this.monitorConfiguration = monitorConfiguration;
    scheduledExecutorService = Executors.newScheduledThreadPool(10);
  }

  public void start() {
    LOG.info("Starting monitor service");

    monitorJobContext = new MonitorJobContext();
    monitorJobContext.setAnomalyJobSpecDAO(anomalyJobSpecDAO);
    monitorJobContext.setAnomalyTaskSpecDAO(anomalyTaskSpecDAO);
    monitorJobContext.setSessionFactory(sessionFactory);
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
