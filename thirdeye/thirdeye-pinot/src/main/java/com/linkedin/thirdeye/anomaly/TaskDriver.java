package com.linkedin.thirdeye.anomaly;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.JobRunner.JobStatus;
import com.linkedin.thirdeye.detector.api.AnomalyTaskSpec;
import com.linkedin.thirdeye.detector.db.AnomalyTaskSpecDAO;

public class TaskDriver {

  private static final Logger LOG = LoggerFactory.getLogger(TaskDriver.class);

  private ScheduledExecutorService pollService;
  private SchedulerFactory schedulerFactory;
  private Scheduler quartzScheduler;

  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private SessionFactory sessionFactory;

  public TaskDriver(AnomalyTaskSpecDAO anomalyTaskSpecDAO, SessionFactory sessionFactory) {
    this.anomalyTaskSpecDAO = anomalyTaskSpecDAO;
    this.sessionFactory = sessionFactory;

    pollService = Executors.newSingleThreadScheduledExecutor();
    schedulerFactory = new StdSchedulerFactory();
    try {
      quartzScheduler = schedulerFactory.getScheduler();
    } catch (SchedulerException e) {
      LOG.error("Exception while starting quartz scheduler", e);
    }
  }

  public void start() throws SchedulerException{
    quartzScheduler.start();

    pollService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        // read the tasks from DB
        System.out.println("Running selectAndUpdate");
        List<AnomalyTaskSpec> statusUpdatedTasks = selectAndUpdate();
        for (AnomalyTaskSpec anomalyTaskSpec : statusUpdatedTasks) {
          // create taskRunner

          // call execute method
          // get the results and write them to database
        }
      }
    }, 0, 1, TimeUnit.MINUTES);

  }

  public void stop() throws SchedulerException{
    pollService.shutdown();
    quartzScheduler.shutdown();
  }


  private List<AnomalyTaskSpec> selectAndUpdate() {
    System.out.println("Starting selectAndUpdate");
    Session session = sessionFactory.openSession();
    List<AnomalyTaskSpec> updatedTasks = null;
    try {
      ManagedSessionContext.bind(session);
      Transaction transaction = session.beginTransaction();
      try {
        System.out.println("executing");
        List<AnomalyTaskSpec> anomalyTasks = anomalyTaskSpecDAO.findByStatusForUpdate(JobStatus.WAITING);
        System.out.println("Results======" + anomalyTasks);
        updatedTasks = anomalyTaskSpecDAO.updateStatus();
        if (!transaction.wasCommitted()) {
          transaction.commit();
        }
      } catch (Exception e) {
        transaction.rollback();
        throw new RuntimeException(e);
      }
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
    System.out.println("returning");
    return updatedTasks;
  }

}