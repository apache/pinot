package com.linkedin.thirdeye.anomaly.detection;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

/**
 * Scheduler for anomaly detection jobs
 */
public class DetectionJobScheduler implements JobScheduler, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobScheduler.class);
  private SchedulerFactory schedulerFactory;
  private Scheduler quartzScheduler;
  private ScheduledExecutorService scheduledExecutorService;
  private JobManager anomalyJobDAO;
  private TaskManager anomalyTaskDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;

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
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  public List<String> getScheduledJobs() throws SchedulerException {
    List<String> activeJobKeys = new ArrayList<>();
    for (String groupName : quartzScheduler.getJobGroupNames()) {
      for (JobKey jobKey : quartzScheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
        activeJobKeys.add(jobKey.getName());
      }
    }
    return activeJobKeys;
  }

  public void start() throws SchedulerException {
    quartzScheduler.start();
    scheduledExecutorService.scheduleWithFixedDelay(this, 0, 30, TimeUnit.MINUTES);
  }

  public void run() {

    try {
      // read all anomaly functions
      LOG.info("Reading all anomaly functions");
      List<AnomalyFunctionDTO> anomalyFunctions = readAnomalyFunctionSpecs();

      // get active jobs
      List<String> scheduledJobs = getScheduledJobs();
      LOG.info("Scheduled jobs {}", scheduledJobs);

      for (AnomalyFunctionDTO anomalyFunction : anomalyFunctions) {
        Long id = anomalyFunction.getId();
        String jobKey = getJobKey(id, anomalyFunction.getFunctionName());
        boolean isActive = anomalyFunction.getIsActive();
        boolean isScheduled = scheduledJobs.contains(jobKey);

        // for all jobs with isActive, but not in scheduled jobs,
        // schedule them with quartz, as function is newly created, or newly activated
        if (isActive && !isScheduled) {
          LOG.info("Found active but not scheduled {}", id);
          startJob(anomalyFunction, jobKey);
        }
        // for all jobs with not isActive, but in scheduled jobs,
        // remove them from quartz, as function is newly deactivated
        else if (!isActive && isScheduled) {
          LOG.info("Found inactive but scheduled {}", id);
          stopJob(jobKey);
        }

        // for all jobs with isActive, and isScheduled,
        // updates to a function will be picked up automatically by the next run
        // but check for cron updates
        else if (isActive && isScheduled) {
          String cronInDatabase = anomalyFunction.getCron();
          List<Trigger> triggers = (List<Trigger>) quartzScheduler.getTriggersOfJob(JobKey.jobKey(jobKey));
          CronTrigger cronTrigger = (CronTrigger) triggers.get(0);
          String cronInSchedule = cronTrigger.getCronExpression();
          // cron expression has been updated, restart this job
          if (!cronInDatabase.equals(cronInSchedule)) {
            LOG.info("Cron expression for function {} with jobKey {} has been changed from {}  to {}. "
                + "Restarting schedule", id, jobKey, cronInSchedule, cronInDatabase);
            stopJob(jobKey);
            startJob(anomalyFunction, jobKey);
          }
        }

        // for all jobs with not isActive, and not isScheduled, no change required

      }

      // for any scheduled jobs, not having a function in the database,
      // stop the schedule, as function has been deleted
      for (String scheduledJobKey : scheduledJobs) {
        Long functionId = getIdFromJobKey(scheduledJobKey);
        AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(functionId);
        if (anomalyFunctionSpec == null) {
          LOG.info("Found scheduled, but not in database {}", functionId);
          stopJob(scheduledJobKey);
        }
      }
    } catch (SchedulerException e) {
      LOG.error("Exception in reading active jobs", e);
    }
  }

  public void shutdown() throws SchedulerException{
    scheduledExecutorService.shutdown();
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
    startJob(anomalyFunctionSpec, jobKey);
  }

  public void startJob(AnomalyFunctionDTO anomalyFunctionSpec, String jobKey) throws SchedulerException {
    if (quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Anomaly function " + jobKey + " is already scheduled");
    }
    DetectionJobContext detectionJobContext = new DetectionJobContext();
    detectionJobContext.setAnomalyFunctionDAO(anomalyFunctionDAO);
    detectionJobContext.setAnomalyJobDAO(anomalyJobDAO);
    detectionJobContext.setAnomalyTaskDAO(anomalyTaskDAO);
    detectionJobContext.setAnomalyFunctionId(anomalyFunctionSpec.getId());
    detectionJobContext.setJobName(jobKey);

    scheduleJob(detectionJobContext, anomalyFunctionSpec);
  }

  public void stopJob(Long id) throws SchedulerException {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
    String functionName = anomalyFunctionSpec.getFunctionName();
    String jobKey = getJobKey(id, functionName);
    stopJob(jobKey);
  }

  public void stopJob(String jobKey) throws SchedulerException {
    if (!quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Cannot stop anomaly function " + jobKey + ", it has not been scheduled");
    }
    quartzScheduler.deleteJob(JobKey.jobKey(jobKey));
    LOG.info("Stopped function {}", jobKey);
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
            .withSchedule(CronScheduleBuilder.cronSchedule(anomalyFunctionSpec.getCron())
                .inTimeZone(TimeZone.getTimeZone("UTC"))).build();

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

  private Long getIdFromJobKey(String jobKey) {
    String[] tokens = jobKey.split("_");
    String id = tokens[tokens.length - 1];
    return Long.valueOf(id);
  }
}
