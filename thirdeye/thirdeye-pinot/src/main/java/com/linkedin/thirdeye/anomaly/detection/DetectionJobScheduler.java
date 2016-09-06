package com.linkedin.thirdeye.anomaly.detection;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
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
import com.linkedin.thirdeye.anomaly.job.JobScheduler;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

/**
 * Scheduler for anomaly detection jobs
 */
public class DetectionJobScheduler implements JobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobScheduler.class);
  private SchedulerFactory schedulerFactory;
  private Scheduler quartzScheduler;
  private JobManager anomalyJobDAO;
  private TaskManager anomalyTaskDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;

  public DetectionJobScheduler(JobManager anomalyJobDAO, TaskManager anomalyTaskDAO,
      AnomalyFunctionManager anomalyFunctionDAO) {
    this.anomalyJobDAO = anomalyJobDAO;
    this.anomalyTaskDAO = anomalyTaskDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;

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
    List<AnomalyFunctionDTO>  functionSpecs = readAnomalyFunctionSpecs();
    for (AnomalyFunctionDTO anomalyFunctionSpec : functionSpecs) {
      if (anomalyFunctionSpec.getIsActive()) {
        DetectionJobContext detectionJobContext = new DetectionJobContext();
        detectionJobContext.setAnomalyFunctionDAO(anomalyFunctionDAO);
        detectionJobContext.setAnomalyJobDAO(anomalyJobDAO);
        detectionJobContext.setAnomalyTaskDAO(anomalyTaskDAO);
        detectionJobContext.setAnomalyFunctionFactory(anomalyFunctionFactory);
        detectionJobContext.setAnomalyFunctionId(anomalyFunctionSpec.getId());
        String jobKey = getJobKey(anomalyFunctionSpec.getId(), anomalyFunctionSpec.getFunctionName());
        detectionJobContext.setJobName(jobKey);

        scheduleJob(detectionJobContext, anomalyFunctionSpec);
      }
    }
  }

  public void stop() throws SchedulerException{
    quartzScheduler.shutdown();
  }

  public void startJob(Long id) throws SchedulerException {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
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

    DetectionJobContext detectionJobContext = new DetectionJobContext();
    detectionJobContext.setAnomalyFunctionDAO(anomalyFunctionDAO);
    detectionJobContext.setAnomalyJobDAO(anomalyJobDAO);
    detectionJobContext.setAnomalyTaskDAO(anomalyTaskDAO);
    detectionJobContext.setAnomalyFunctionFactory(anomalyFunctionFactory);
    detectionJobContext.setAnomalyFunctionId(anomalyFunctionSpec.getId());
    detectionJobContext.setJobName(jobKey);

    scheduleJob(detectionJobContext, anomalyFunctionSpec);
  }

  public void stopJob(Long id) throws SchedulerException {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
    String functionName = anomalyFunctionSpec.getFunctionName();
    String jobKey = getJobKey(id, functionName);
    if (!quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Cannot stop anomaly function with id " + id + ", it has not been scheduled");
    }
    quartzScheduler.deleteJob(JobKey.jobKey(jobKey));
    LOG.info("Stopped function {}", id);
  }

  public void runAdHoc(Long id, DateTime windowStartTime, DateTime windowEndTime) {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
    if (anomalyFunctionSpec == null) {
      throw new IllegalArgumentException("No function with id " + id);
    }
    String triggerKey = String.format("anomaly_adhoc_trigger_%d", anomalyFunctionSpec.getId());
    Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).startNow().build();

    String jobKey = "adhoc_" + getJobKey(anomalyFunctionSpec.getId(), anomalyFunctionSpec.getFunctionName());
    JobDetail job = JobBuilder.newJob(DetectionJobRunner.class).withIdentity(jobKey).build();

    DetectionJobContext detectionJobContext = new DetectionJobContext();
    detectionJobContext.setAnomalyFunctionDAO(anomalyFunctionDAO);
    detectionJobContext.setAnomalyJobDAO(anomalyJobDAO);
    detectionJobContext.setAnomalyTaskDAO(anomalyTaskDAO);
    detectionJobContext.setAnomalyFunctionFactory(anomalyFunctionFactory);
    detectionJobContext.setAnomalyFunctionId(anomalyFunctionSpec.getId());
    detectionJobContext.setJobName(jobKey);
    detectionJobContext.setWindowStartTime(windowStartTime);
    detectionJobContext.setWindowEndTime(windowEndTime);

    job.getJobDataMap().put(DetectionJobRunner.DETECTION_JOB_CONTEXT, detectionJobContext);

    try {
      quartzScheduler.scheduleJob(job, trigger);
    } catch (SchedulerException e) {
      LOG.error("Exception while scheduling job", e);
    }

    LOG.info("Started {}: {}", jobKey, anomalyFunctionSpec);
  }


  private void scheduleJob(JobContext jobContext, AnomalyFunctionDTO anomalyFunctionSpec) {

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

  private List<AnomalyFunctionDTO> readAnomalyFunctionSpecs() {
    return anomalyFunctionDAO.findAll();
  }

  private String getJobKey(Long id, String functionName) {
    String jobKey = String.format("%s_%d", functionName, id);
    return jobKey;
  }
}
