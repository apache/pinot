package com.linkedin.thirdeye.anomaly;

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

import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class JobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(JobScheduler.class);
  private SchedulerFactory schedulerFactory;
  private Scheduler quartzScheduler;
  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private SessionFactory sessionFactory;

  private ThirdEyeJobContext thirdEyeJobContext;

  public JobScheduler(AnomalyJobSpecDAO anomalyJobSpecDAO, AnomalyTaskSpecDAO anomalyTaskSpecDAO,
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
        thirdEyeJobContext = new ThirdEyeJobContext();
        thirdEyeJobContext.setAnomalyFunctionSpecDAO(anomalyFunctionSpecDAO);
        thirdEyeJobContext.setAnomalyJobSpecDAO(anomalyJobSpecDAO);
        thirdEyeJobContext.setAnomalyTaskSpecDAO(anomalyTaskSpecDAO);
        thirdEyeJobContext.setAnomalyFunctionFactory(anomalyFunctionFactory);
        thirdEyeJobContext.setSessionFactory(sessionFactory);
        thirdEyeJobContext.setAnomalyFunctionId(anomalyFunctionSpec.getId());
        String jobName = String.format("%s_%d", anomalyFunctionSpec.getFunctionName(), anomalyFunctionSpec.getId());
        thirdEyeJobContext.setJobName(jobName);

        scheduleJob(thirdEyeJobContext, anomalyFunctionSpec);
      }
    }
  }

  private void scheduleJob(ThirdEyeJobContext thirdEyeJobContext, AnomalyFunctionSpec anomalyFunctionSpec) {

    String triggerKey = String.format("anomaly_scheduler_trigger_%d", anomalyFunctionSpec.getId());
    CronTrigger trigger =
        TriggerBuilder.newTrigger().withIdentity(triggerKey)
            .withSchedule(CronScheduleBuilder.cronSchedule(anomalyFunctionSpec.getCron())).build();

    String jobKey = String.format("%s_%d", anomalyFunctionSpec.getFunctionName(), anomalyFunctionSpec.getId());
    JobDetail job = JobBuilder.newJob(JobRunner.class).withIdentity(thirdEyeJobContext.getJobName()).build();

    job.getJobDataMap().put(JobRunner.THIRDEYE_JOB_CONTEXT, thirdEyeJobContext);

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
