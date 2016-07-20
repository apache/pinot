package com.linkedin.thirdeye.anomaly.detection;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.SessionFactory;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

/**
 * Scheduler for anomaly detection jobs
 */
public class DetectionJobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobScheduler.class);
  private SchedulerFactory schedulerFactory;
  private Scheduler quartzScheduler;
  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private SessionFactory sessionFactory;

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

  public List<String> getActiveJobs() throws SchedulerException {
    List<String> activeJobKeys = new ArrayList<>();
    for (String groupName : quartzScheduler.getJobGroupNames()) {
      for (JobKey jobKey : quartzScheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
        activeJobKeys.add(jobKey.getName());
      }
    }
    return activeJobKeys;
  }

  public void start() throws SchedulerException{
    quartzScheduler.start();

    // start all active anomaly functions
    List<AnomalyFunctionSpec>  functionSpecs = readAnomalyFunctionSpecs();
    for (AnomalyFunctionSpec anomalyFunctionSpec : functionSpecs) {
      if (anomalyFunctionSpec.getIsActive()) {
        JobContext jobContext = new JobContext();
        jobContext.setAnomalyFunctionSpecDAO(anomalyFunctionSpecDAO);
        jobContext.setAnomalyJobSpecDAO(anomalyJobSpecDAO);
        jobContext.setAnomalyTaskSpecDAO(anomalyTaskSpecDAO);
        jobContext.setAnomalyFunctionFactory(anomalyFunctionFactory);
        jobContext.setSessionFactory(sessionFactory);
        jobContext.setAnomalyFunctionId(anomalyFunctionSpec.getId());
        String jobKey = getJobKey(anomalyFunctionSpec.getId(), anomalyFunctionSpec.getFunctionName());
        jobContext.setJobName(jobKey);

        scheduleJob(jobContext, anomalyFunctionSpec);
      }
    }
  }

  public void stop() throws SchedulerException{
    quartzScheduler.shutdown();
  }

  public void start(Long id) throws SchedulerException {
    AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(id);
    if (anomalyFunctionSpec == null) {
      throw new IllegalArgumentException("No function with id " + id);
    }
    if (!anomalyFunctionSpec.getIsActive()) {
      throw new IllegalStateException("Anomaly function spec with id " + id + " is not active");
    }
    String jobKey = getJobKey(anomalyFunctionSpec.getId(), anomalyFunctionSpec.getFunctionName());
    if (quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Anomaly function with id " + id + " is already scheduled");
    }

    JobContext jobContext = new JobContext();
    jobContext.setAnomalyFunctionSpecDAO(anomalyFunctionSpecDAO);
    jobContext.setAnomalyJobSpecDAO(anomalyJobSpecDAO);
    jobContext.setAnomalyTaskSpecDAO(anomalyTaskSpecDAO);
    jobContext.setAnomalyFunctionFactory(anomalyFunctionFactory);
    jobContext.setSessionFactory(sessionFactory);
    jobContext.setAnomalyFunctionId(anomalyFunctionSpec.getId());
    jobContext.setJobName(jobKey);

    scheduleJob(jobContext, anomalyFunctionSpec);
  }

  public void stop(Long id) throws SchedulerException {
    AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(id);
    String functionName = anomalyFunctionSpec.getFunctionName();
    String jobKey = getJobKey(id, functionName);
    if (!quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Cannot stop anomaly function with id " + id + ", it has not been scheduled");
    }
    quartzScheduler.deleteJob(JobKey.jobKey(jobKey));
    LOG.info("Stopped function {}", id);
  }

  public void runAdHoc(Long id, String windowStartIso, String windowEndIso) {
    AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(id);
    if (anomalyFunctionSpec == null) {
      throw new IllegalArgumentException("No function with id " + id);
    }
    String triggerKey = String.format("anomaly_adhoc_trigger_%d", anomalyFunctionSpec.getId());
    Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).startNow().build();

    String jobKey = "adhoc_" + getJobKey(anomalyFunctionSpec.getId(), anomalyFunctionSpec.getFunctionName());
    JobDetail job = JobBuilder.newJob(DetectionJobRunner.class).withIdentity(jobKey).build();

    JobContext jobContext = new JobContext();
    jobContext.setAnomalyFunctionSpecDAO(anomalyFunctionSpecDAO);
    jobContext.setAnomalyJobSpecDAO(anomalyJobSpecDAO);
    jobContext.setAnomalyTaskSpecDAO(anomalyTaskSpecDAO);
    jobContext.setAnomalyFunctionFactory(anomalyFunctionFactory);
    jobContext.setSessionFactory(sessionFactory);
    jobContext.setAnomalyFunctionId(anomalyFunctionSpec.getId());
    jobContext.setJobName(jobKey);
    jobContext.setWindowStartIso(windowStartIso);
    jobContext.setWindowEndIso(windowEndIso);

    job.getJobDataMap().put(DetectionJobRunner.DETECTION_JOB_CONTEXT, jobContext);

    try {
      quartzScheduler.scheduleJob(job, trigger);
    } catch (SchedulerException e) {
      LOG.error("Exception while scheduling job", e);
    }

    LOG.info("Started {}: {}", jobKey, anomalyFunctionSpec);
  }


  private void scheduleJob(JobContext jobContext, AnomalyFunctionSpec anomalyFunctionSpec) {

    String triggerKey = String.format("anomaly_scheduler_trigger_%d", anomalyFunctionSpec.getId());
    CronTrigger trigger =
        TriggerBuilder.newTrigger().withIdentity(triggerKey)
            .withSchedule(CronScheduleBuilder.cronSchedule(anomalyFunctionSpec.getCron())).build();

    String jobKey = jobContext.getJobName();
    JobDetail job = JobBuilder.newJob(DetectionJobRunner.class).withIdentity(jobKey).build();

    job.getJobDataMap().put(DetectionJobRunner.DETECTION_JOB_CONTEXT, jobContext);

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

  private String getJobKey(Long id, String functionName) {
    String jobKey = String.format("%s_%d", functionName, id);
    return jobKey;
  }
}
