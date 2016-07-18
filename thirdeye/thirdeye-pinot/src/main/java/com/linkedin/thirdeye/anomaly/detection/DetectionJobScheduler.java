package com.linkedin.thirdeye.anomaly.detection;

import java.util.List;

import org.hibernate.SessionFactory;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class DetectionJobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobScheduler.class);
  private SchedulerFactory schedulerFactory;
  private Scheduler quartzScheduler;
  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private SessionFactory sessionFactory;

  private JobContext jobContext;

  public DetectionJobScheduler(AnomalyJobSpecDAO anomalyJobSpecDAO, AnomalyTaskSpecDAO anomalyTaskSpecDAO,
      AnomalyFunctionSpecDAO anomalyFunctionSpecDAO,
      SessionFactory sessionFactory) {
    this.anomalyJobSpecDAO = anomalyJobSpecDAO;
    this.anomalyTaskSpecDAO = anomalyTaskSpecDAO;
    this.anomalyFunctionSpecDAO = anomalyFunctionSpecDAO;
    this.sessionFactory = sessionFactory;

    schedulerFactory = new StdSchedulerFactory();
    try {
      quartzScheduler = schedulerFactory.getScheduler();
    } catch (SchedulerException e) {
      LOG.error("Exception while starting quartz scheduler", e);
    }
  }

  public void start() throws SchedulerException{
    quartzScheduler.start();

    //read the anomaly function specs
    List<AnomalyFunctionSpec>  functionSpecs = readAnomalyFunctionSpecs();
    for (AnomalyFunctionSpec anomalyFunctionSpec : functionSpecs) {
      if (anomalyFunctionSpec.getIsActive()) {
        jobContext = new JobContext();
        jobContext.setAnomalyFunctionSpecDAO(anomalyFunctionSpecDAO);
        jobContext.setAnomalyJobSpecDAO(anomalyJobSpecDAO);
        jobContext.setAnomalyTaskSpecDAO(anomalyTaskSpecDAO);
        jobContext.setAnomalyFunctionFactory(anomalyFunctionFactory);
        jobContext.setSessionFactory(sessionFactory);
        jobContext.setAnomalyFunctionId(anomalyFunctionSpec.getId());
        String jobName = String.format("%s_%d", anomalyFunctionSpec.getFunctionName(), anomalyFunctionSpec.getId());
        jobContext.setJobName(jobName);

        scheduleJob(jobContext, anomalyFunctionSpec);
      }
    }
  }

  private void scheduleJob(JobContext jobContext, AnomalyFunctionSpec anomalyFunctionSpec) {

    String triggerKey = String.format("anomaly_scheduler_trigger_%d", anomalyFunctionSpec.getId());
    CronTrigger trigger =
        TriggerBuilder.newTrigger().withIdentity(triggerKey)
            .withSchedule(CronScheduleBuilder.cronSchedule(anomalyFunctionSpec.getCron())).build();

    String jobKey = String.format("%s_%d", anomalyFunctionSpec.getFunctionName(), anomalyFunctionSpec.getId());
    JobDetail job = JobBuilder.newJob(DetectionJobRunner.class).withIdentity(jobContext.getJobName()).build();

    job.getJobDataMap().put(DetectionJobRunner.THIRDEYE_JOB_CONTEXT, jobContext);

    try {
      quartzScheduler.scheduleJob(job, trigger);
    } catch (SchedulerException e) {
      LOG.error("Exception while scheduling job", e);
    }

    LOG.info("Started {}: {}", jobKey, anomalyFunctionSpec);
  }

  private List<AnomalyFunctionSpec> readAnomalyFunctionSpecs() {
    return anomalyFunctionSpecDAO.findAll();
  }

  public void stop() throws SchedulerException{
    quartzScheduler.shutdown();

  }
}
