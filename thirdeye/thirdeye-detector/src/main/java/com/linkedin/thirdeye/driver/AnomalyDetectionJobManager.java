package com.linkedin.thirdeye.driver;

import com.linkedin.thirdeye.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.db.AnomalyResultDAO;
import com.linkedin.thirdeye.function.AnomalyFunction;
import com.linkedin.thirdeye.function.AnomalyFunctionFactory;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class AnomalyDetectionJobManager {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionJobManager.class);
  private final Scheduler quartzScheduler;
  private final ThirdEyeClient thirdEyeClient;
  private final AnomalyFunctionSpecDAO specDAO;
  private final AnomalyResultDAO resultDAO;
  private final SessionFactory sessionFactory;
  private final Object sync;
  private final Map<Long, String> jobKeys;

  public AnomalyDetectionJobManager(Scheduler quartzScheduler,
                                    ThirdEyeClient thirdEyeClient,
                                    AnomalyFunctionSpecDAO specDAO,
                                    AnomalyResultDAO resultDAO,
                                    SessionFactory sessionFactory) {
    this.quartzScheduler = quartzScheduler;
    this.thirdEyeClient = thirdEyeClient;
    this.specDAO = specDAO;
    this.resultDAO = resultDAO;
    this.sessionFactory = sessionFactory;
    this.sync = new Object();
    this.jobKeys = new HashMap<>();
  }

  private AnomalyFunctionSpec getAnomalyFunctionSpec(Long id) {
    Session session = sessionFactory.openSession();
    try {
      ManagedSessionContext.bind(session);
      Transaction transaction = session.beginTransaction();
      try {
        AnomalyFunctionSpec spec = specDAO.findById(id);
        if (spec == null) {
          throw new IllegalArgumentException("No function with id " + id);
        }
        transaction.commit();
        return spec;
      } catch (Exception e) {
        transaction.rollback();
        throw new RuntimeException(e);
      }
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
  }

  public void runAdHoc(Long id, String windowEndISO8601String) throws Exception {
    synchronized (sync) {
      AnomalyFunctionSpec spec = getAnomalyFunctionSpec(id);
      AnomalyFunction anomalyFunction = AnomalyFunctionFactory.fromSpec(spec);

      String triggerKey = String.format("ad_hoc_anomaly_function_trigger_%d", spec.getId());
      Trigger trigger = TriggerBuilder.newTrigger()
          .withIdentity(triggerKey)
          .startNow()
          .build();

      String jobKey = String.format("ad_hoc_anomaly_function_job_%d", spec.getId());
      JobDetail job = JobBuilder.newJob(AnomalyDetectionJob.class)
          .withIdentity(jobKey)
          .build();

      job.getJobDataMap().put(AnomalyDetectionJob.FUNCTION, anomalyFunction);
      job.getJobDataMap().put(AnomalyDetectionJob.CLIENT, thirdEyeClient);
      job.getJobDataMap().put(AnomalyDetectionJob.WINDOW_END, windowEndISO8601String);
      job.getJobDataMap().put(AnomalyDetectionJob.RESULT_DAO, resultDAO);
      job.getJobDataMap().put(AnomalyDetectionJob.SESSION_FACTORY, sessionFactory);

      quartzScheduler.scheduleJob(job, trigger);

      LOG.info("Started {}: {}", jobKey, spec);
    }
  }

  public void start(Long id) throws Exception {
    synchronized (sync) {
      AnomalyFunctionSpec spec = getAnomalyFunctionSpec(id);
      AnomalyFunction anomalyFunction = AnomalyFunctionFactory.fromSpec(spec);

      String triggerKey = String.format("scheduled_anomaly_function_trigger_%d", spec.getId());
      CronTrigger trigger = TriggerBuilder.newTrigger()
          .withIdentity(triggerKey)
          .withSchedule(CronScheduleBuilder.cronSchedule(spec.getCron()))
          .build();

      String jobKey = String.format("scheduled_anomaly_function_job_%d", spec.getId());
      jobKeys.put(id, jobKey);
      JobDetail job = JobBuilder.newJob(AnomalyDetectionJob.class)
          .withIdentity(jobKey)
          .build();

      job.getJobDataMap().put(AnomalyDetectionJob.FUNCTION, anomalyFunction);
      job.getJobDataMap().put(AnomalyDetectionJob.CLIENT, thirdEyeClient);
      job.getJobDataMap().put(AnomalyDetectionJob.RESULT_DAO, resultDAO);
      job.getJobDataMap().put(AnomalyDetectionJob.SESSION_FACTORY, sessionFactory);

      quartzScheduler.scheduleJob(job, trigger);

      LOG.info("Started {}: {}", jobKey, spec);
    }
  }

  public void stop(Long id) throws Exception {
    synchronized (sync) {
      String jobKey = jobKeys.get(id);
      if (jobKey == null) {
        throw new IllegalArgumentException("No scheduled job for function id " + id);
      }

      quartzScheduler.deleteJob(JobKey.jobKey(jobKey));

      LOG.info("Stopped {}", jobKey);
    }
  }
}
