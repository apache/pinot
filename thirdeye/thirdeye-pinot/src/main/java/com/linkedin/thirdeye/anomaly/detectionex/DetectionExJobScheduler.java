package com.linkedin.thirdeye.anomaly.detectionex;

import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.anomaly.job.JobScheduler;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExFactory;
import com.linkedin.thirdeye.detector.functionex.impl.AlwaysAnomalyDummy;
import com.linkedin.thirdeye.detector.functionex.impl.MultiColumnConditionals;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.CronExpression;
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


/**
 * Scheduler for anomaly detection jobs
 */
public class DetectionExJobScheduler implements JobScheduler, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionExJobScheduler.class);
  private SchedulerFactory schedulerFactory;
  private Scheduler quartzScheduler;
  private ScheduledExecutorService scheduledExecutorService;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private static final int BACKFILL_MAX_RETRY = 3;
  private static final int BACKFILL_TASK_POLL_TIME = 5_000; // Period to check if a task is finished
  private static final int BACKFILL_RESCHEDULE_TIME = 15_000; // Pause before reschedule a failed job
  private final Map<BackfillKey, Thread> existingBackfillJobs = new ConcurrentHashMap<>();

  public DetectionExJobScheduler() {
    this.schedulerFactory = new StdSchedulerFactory();
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
    scheduledExecutorService.scheduleWithFixedDelay(this, 0, 1, TimeUnit.MINUTES);
  }

  public void run() {

    try {
      // read all anomaly functions
      LOG.info("Reading all anomaly functions");
      List<AnomalyFunctionExDTO> anomalyFunctions = readAnomalyFunctionSpecs();

      // TODO remove debug
      if(anomalyFunctions.isEmpty()) {
        Map<String, String> config = new HashMap<>();
        AnomalyFunctionExDTO dto = new AnomalyFunctionExDTO();
        dto.setName("ex_test_always");
        dto.setClassName(AlwaysAnomalyDummy.class.getName());
        dto.setActive(true);
        dto.setCron("0 * * * * ?");
        dto.setConfig(config);

        DAO_REGISTRY.getAnomalyFunctionExDAO().save(dto);
        anomalyFunctions = readAnomalyFunctionSpecs();
      }

      // get active jobs
      List<String> scheduledJobs = getScheduledJobs();
      LOG.info("Scheduled jobs {}", scheduledJobs);

      for (AnomalyFunctionExDTO spec : anomalyFunctions) {
        Long id = spec.getId();
        String jobKey = getJobKey(id, spec.getName());
        boolean isActive = spec.isActive();
        boolean isScheduled = scheduledJobs.contains(jobKey);

        // for all jobs with isActive, but not in scheduled jobs,
        // schedule them with quartz, as function is newly created, or newly activated
        if (isActive && !isScheduled) {
          LOG.info("Found active but not scheduled {}", id);
          startJob(spec, jobKey);
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
          String cronInDatabase = spec.getCron();
          List<Trigger> triggers = (List<Trigger>) quartzScheduler.getTriggersOfJob(JobKey.jobKey(jobKey));
          CronTrigger cronTrigger = (CronTrigger) triggers.get(0);
          String cronInSchedule = cronTrigger.getCronExpression();
          // cron expression has been updated, restart this job
          if (!cronInDatabase.equals(cronInSchedule)) {
            LOG.info("Cron expression for function {} with jobKey {} has been changed from {}  to {}. "
                + "Restarting schedule", id, jobKey, cronInSchedule, cronInDatabase);
            stopJob(jobKey);
            startJob(spec, jobKey);
          }
        }

        // for all jobs with not isActive, and not isScheduled, no change required

      }

      // for any scheduled jobs, not having a function in the database,
      // stop the schedule, as function has been deleted
      for (String scheduledJobKey : scheduledJobs) {
        Long functionId = getIdFromJobKey(scheduledJobKey);
        AnomalyFunctionDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionDAO().findById(functionId);
        if (anomalyFunctionSpec == null) {
          LOG.info("Found scheduled, but not in database {}", functionId);
          stopJob(scheduledJobKey);
        }
      }
    } catch (Exception e) {
      LOG.error("Exception in reading active jobs", e);
    }
  }

  public void shutdown() throws SchedulerException{
    scheduledExecutorService.shutdown();
    quartzScheduler.shutdown();
  }

  public void startJob(Long id) throws SchedulerException {
    AnomalyFunctionExDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionExDAO().findById(id);
    if (anomalyFunctionSpec == null) {
      throw new IllegalArgumentException("No function with id " + id);
    }
    if (!anomalyFunctionSpec.isActive()) {
      throw new IllegalStateException("Anomaly function spec with id " + id + " is not active");
    }
    String jobKey = getJobKey(anomalyFunctionSpec.getId(), anomalyFunctionSpec.getName());
    startJob(anomalyFunctionSpec, jobKey);
  }

  public void startJob(AnomalyFunctionExDTO anomalyFunctionSpec, String jobKey) throws SchedulerException {
    if (quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Anomaly function " + jobKey + " is already scheduled");
    }
    DetectionExJobContext detectionJobContext = new DetectionExJobContext();
    detectionJobContext.setAnomalyFunctionId(anomalyFunctionSpec.getId());
    detectionJobContext.setJobName(jobKey);

    scheduleJob(detectionJobContext, anomalyFunctionSpec);
  }

  public void stopJob(Long id) throws SchedulerException {
    AnomalyFunctionDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionDAO().findById(id);
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

  /**
   * Performs a detection job, which is immediately triggered once, for the specified anomaly function on the given
   * monitoring window.
   *
   * @param id id of the specified anomaly function
   * @param windowStartTime start time of the given monitoring window
   * @param windowEndTime end time of the given monitoring window
   * @return the name of the detection job, which is composed of the prefix "adhoc", window start time, job scheduled
   * time, function name, and function id; which are separated by symbol "_".
   */
  public String runAdHoc(Long id, DateTime windowStartTime, DateTime windowEndTime) {
    AnomalyFunctionDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionDAO().findById(id);
    if (anomalyFunctionSpec == null) {
      throw new IllegalArgumentException("No function with id " + id);
    }
    String triggerKey = String.format("anomaly_ex_adhoc_trigger_%d_%d_%d", System.currentTimeMillis(), windowStartTime.getMillis(), anomalyFunctionSpec.getId());
    Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).startNow().build();

    String jobKey = "adhoc_" + windowStartTime.getMillis() + "_" + System.currentTimeMillis() + "_" + getJobKey(anomalyFunctionSpec.getId(), anomalyFunctionSpec.getFunctionName());
    JobDetail job = JobBuilder.newJob(DetectionExJobRunner.class).withIdentity(jobKey).build();

    DetectionExJobContext detectionJobContext = new DetectionExJobContext();
    detectionJobContext.setAnomalyFunctionId(anomalyFunctionSpec.getId());
    detectionJobContext.setJobName(jobKey);

    job.getJobDataMap().put(DetectionExJobRunner.DETECTION_EX_JOB_CONTEXT, detectionJobContext);
//    job.getJobDataMap().put(DetectionExJobRunner.DETECTION_JOB_MONITORING_WINDOW_START_TIME, windowStartTime);
//    job.getJobDataMap().put(DetectionExJobRunner.DETECTION_JOB_MONITORING_WINDOW_END_TIME, windowEndTime);

    try {
      quartzScheduler.scheduleJob(job, trigger);
      LOG.info("Started {}: {}", jobKey, anomalyFunctionSpec);
    } catch (SchedulerException e) {
      LOG.error("Exception while scheduling job", e);
    }

    return jobKey;
  }

  private void scheduleJob(JobContext jobContext, AnomalyFunctionExDTO spec) {

    String triggerKey = String.format("anomaly_ex_scheduler_trigger_%d", spec.getId());
    CronTrigger trigger =
        TriggerBuilder.newTrigger().withIdentity(triggerKey)
            .withSchedule(CronScheduleBuilder.cronSchedule(spec.getCron())
                .inTimeZone(TimeZone.getTimeZone("UTC"))).build();

    String jobKey = jobContext.getJobName();
    JobDetail job = JobBuilder.newJob(DetectionExJobRunner.class).withIdentity(jobKey).build();

    job.getJobDataMap().put(DetectionExJobRunner.DETECTION_EX_JOB_CONTEXT, jobContext);

    try {
      quartzScheduler.scheduleJob(job, trigger);
    } catch (SchedulerException e) {
      LOG.error("Exception while scheduling job", e);
    }

    LOG.info("Started {}: {}", jobKey, spec);
  }

  private List<AnomalyFunctionExDTO> readAnomalyFunctionSpecs() {
    return DAO_REGISTRY.getAnomalyFunctionExDAO().findAll();
  }

  private String getJobKey(Long id, String functionName) {
    String jobKey = String.format("anomalyex_%s_%d", functionName, id);
    return jobKey;
  }

  private Long getIdFromJobKey(String jobKey) {
    String[] tokens = jobKey.split("_");
    String id = tokens[tokens.length - 1];
    return Long.valueOf(id);
  }

  /**
   * Sequentially performs anomaly detection for all the monitoring windows that are located between backfillStartTime
   * and backfillEndTime. A lightweight job is performed right after each detection job and notified is set to false in
   * order to silence the mail alerts.
   *
   * NOTE: We assume that the backfill window for the same function DOES NOT overlap. In other words, this function
   * does not guarantees correctness of the detections result if it is invoked twice with the same parameters.
   *
   * @param functionId the id of the anomaly function, which has to be an active function
   * @param backfillStartTime the start time for backfilling
   * @param backfillEndTime the end time for backfilling
   * @param force set to false to resume from previous backfill if there exists any
   */
  public void runBackfill(long functionId, DateTime backfillStartTime, DateTime backfillEndTime, boolean force) {
    AnomalyFunctionDTO anomalyFunction = DAO_REGISTRY.getAnomalyFunctionDAO().findById(functionId);
    boolean isActive = anomalyFunction.getIsActive();
    if (!isActive) {
      LOG.info("Skipping function {}", functionId);
      return;
    }

    BackfillKey backfillKey = new BackfillKey(functionId, backfillStartTime, backfillEndTime);
    Thread returnedThread = existingBackfillJobs.putIfAbsent(backfillKey, Thread.currentThread());
    // If returned thread is not current thread, then a backfill job is already running
    if (returnedThread != null) {
      LOG.info("Aborting... An existing back-fill job is running...");
      return;
    }

    try {
      CronExpression cronExpression = null;
      try {
        cronExpression = new CronExpression(anomalyFunction.getCron());
      } catch (ParseException e) {
        LOG.error("Failed to parse cron expression for function {}", functionId);
        return;
      }

      long monitoringWindowSize = TimeUnit.MILLISECONDS.convert(anomalyFunction.getWindowSize(), anomalyFunction.getWindowUnit());
      DateTime currentStart;
      if (force) {
        currentStart = backfillStartTime;
      } else {
        currentStart = computeResumeStartTime(functionId, cronExpression, backfillStartTime, backfillEndTime);
      }
      DateTime currentEnd = currentStart.plus(monitoringWindowSize);

      // Make the end time inclusive
      DateTime endBoundary = new DateTime(cronExpression.getNextValidTimeAfter(backfillEndTime.toDate()));

      LOG.info("Begin regenerate anomalies for each monitoring window between {} and {}", currentStart, endBoundary);
      while (currentEnd.isBefore(endBoundary)) {
        String monitoringWindowStart = ISODateTimeFormat.dateHourMinute().print(currentStart);
        String monitoringWindowEnd = ISODateTimeFormat.dateHourMinute().print(currentEnd);
        LOG.info("Running adhoc function {} for range {} to {}", functionId, monitoringWindowStart, monitoringWindowEnd);

        String jobKey = runAdHoc(functionId, currentStart, currentEnd);

        // Synchronously and periodically check if job is done
        boolean status = waitUntilJobIsDone(jobKey);

        if (Thread.currentThread().isInterrupted()) {
          LOG.info("Terminating adhoc function {}. Last executed job ranges {} to {}.", functionId, currentStart,
              currentEnd);
          return;
        }

        if (!status) {
          // Reschedule the same job is it fails
          LOG.info("Failed to finish adhoc function {} for range {} to {}.", functionId, currentStart, currentEnd);
          sleepSilently(BACKFILL_RESCHEDULE_TIME);
          LOG.info("Rerunning adhoc function {} for range {} to {}.", functionId, currentStart, currentEnd);
        } else {
          // Start the next job if the current job is succeeded
          currentStart = new DateTime(cronExpression.getNextValidTimeAfter(currentStart.toDate()));
          currentEnd = currentStart.plus(monitoringWindowSize);
        }
      }
      LOG.info("Generated anomalies for each monitoring window whose start is located in range {} -- {}",
          backfillStartTime, currentStart);
    } finally {
      existingBackfillJobs.remove(backfillKey, Thread.currentThread());
    }
  }

  private JobDTO getPreviousJob(long functionId, long backfillWindowStart, long backfillWindowEnd) {
    return DAO_REGISTRY.getJobDAO().findLatestBackfillScheduledJobByFunctionId(functionId, backfillWindowStart, backfillWindowEnd);
  }

  /**
   * Returns the start time of the first detection job for the current backfill. The start time is determined in the
   * following:
   * 1. If there exists any previously left detection job, then start backfill from that job.
   *    1a. if that job is finished, then start a job next to it.
   *    1b. if that job is unfinished, then restart that job.
   * 2. If there exists no previous left job, then start the job from the beginning.
   *
   * @param cronExpression the cron expression that is used to calculate the alignment of start time.
   * @return the start time for the first detection job of this backfilling.
   */
  private DateTime computeResumeStartTime(long functionId, CronExpression cronExpression, DateTime backfillStartTime, DateTime backfillEndTime) {
    DateTime currentStart;
    JobDTO previousJob = getPreviousJob(functionId, backfillStartTime.getMillis(), backfillEndTime.getMillis());
    if (previousJob != null) {
      long previousStartTime = previousJob.getWindowStartTime();
      cleanUpJob(previousJob);
      if (previousJob.getStatus().equals(JobConstants.JobStatus.COMPLETED)) {
        // Schedule a job after previous job
        currentStart = new DateTime(cronExpression.getNextValidTimeAfter(new Date(previousStartTime)));
      } else {
        // Reschedule the previous incomplete job
        currentStart = new DateTime(previousStartTime);
      }
      LOG.info("Backfill starting from {} for functoin {} because a previous unfinished jobs is found.", currentStart,
          functionId);
    } else {
      // Schedule a job starting from the beginning
      currentStart = backfillStartTime;
    }
    return currentStart;
  }

  /**
   * Sets unfinished (i.e., RUNNING, WAITING) tasks and job's status to FAILED
   * @param job
   */
  private void cleanUpJob(JobDTO job) {
    if (!job.getStatus().equals(JobConstants.JobStatus.COMPLETED)) {
      List<TaskDTO> tasks = DAO_REGISTRY.getTaskDAO().findByJobIdStatusNotIn(job.getId(), TaskConstants.TaskStatus.COMPLETED);
      if (CollectionUtils.isNotEmpty(tasks)) {
        for (TaskDTO task : tasks) {
          task.setStatus(TaskConstants.TaskStatus.FAILED);
          DAO_REGISTRY.getTaskDAO().save(task);
        }
        job.setStatus(JobConstants.JobStatus.FAILED);
      } else {
        // This case happens when scheduler dies before it knows that all its tasks are actually finished
        job.setStatus(JobConstants.JobStatus.COMPLETED);
      }
      DAO_REGISTRY.getJobDAO().save(job);
    }
  }

  /**
   * Returns the job of the given name with retries. This method is used to get the job that is just inserted to database
   *
   * @param jobName
   * @return
   */
  private JobDTO tryToGetJob(String jobName) {
    JobDTO job = null;
    for (int i = 0; i < BACKFILL_MAX_RETRY; ++i) {
      job = DAO_REGISTRY.getJobDAO().findLatestScheduledJobByName(jobName);
      if (job == null) {
        sleepSilently(BACKFILL_TASK_POLL_TIME);
        if (Thread.currentThread().interrupted()) {
          break;
        }
      } else {
        break;
      }
    }
    return job;
  }

  /**
   * Sets a job's status to COMPLETED when all its tasks are COMPLETED.
   * @param jobName
   * @return false if any one of its tasks is FAILED or thread is interrupted.
   */
  private boolean waitUntilJobIsDone(String jobName) {
    // A new job may not be stored to database in time, so we try to read the job BACKFILL_MAX_RETRY times
    JobDTO job = tryToGetJob(jobName);

    if (job == null || job.getStatus() != JobConstants.JobStatus.SCHEDULED) {
      return false;
    } else {
      // Monitor task until it finishes. We assume that a worker never dies.
      boolean taskCompleted = waitUntilTasksFinished(job.getId());
      if (taskCompleted) {
        job.setStatus(JobConstants.JobStatus.COMPLETED);
        DAO_REGISTRY.getJobDAO().save(job);
      } else {
        cleanUpJob(job);
      }
      return taskCompleted;
    }
  }

  /**
   * Waits until all tasks of the job are COMPLETED
   * @param jobId
   * @return false if any one of its tasks is FAILED or thread is interrupted.
   */
  private boolean waitUntilTasksFinished(long jobId) {
    while (true) {
      List<TaskDTO> tasks = DAO_REGISTRY.getTaskDAO().findByJobIdStatusNotIn(jobId, TaskConstants.TaskStatus.COMPLETED);
      if (CollectionUtils.isEmpty(tasks)) {
        return true; // task finished
      } else {
        // If any one of the tasks of the job fails, the entire job fails
        for (TaskDTO task : tasks) {
          if (task.getStatus() == TaskConstants.TaskStatus.FAILED) {
            return false;
          }
        }
        // keep waiting
        sleepSilently(BACKFILL_TASK_POLL_TIME);
        if (Thread.currentThread().interrupted()) {
          return false;
        }
      }
    }
  }

  /**
   * Sleep for BACKFILL_TASK_POLL_TIME. Set interrupt flag if the thread is interrupted.
   */
  private void sleepSilently(long sleepDurationMillis) {
    try {
      Thread.currentThread().sleep(sleepDurationMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Use to check if the backfill jobs exists
   */
  static class BackfillKey {
    private long functionId;
    private DateTime backfillStartTime;
    private DateTime backfillEndTime;

    public BackfillKey(long functionId, DateTime backfillStartTime, DateTime backfillEndTime){
      this.functionId = functionId;
      this.backfillStartTime = backfillStartTime;
      this.backfillEndTime = backfillEndTime;
    }

    @Override
    public int hashCode() {
      return Objects.hash(functionId, backfillStartTime, backfillEndTime);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof BackfillKey) {
        BackfillKey other = (BackfillKey) o;
        return Objects.equals(this.functionId, other.functionId) && Objects.equals(this.backfillStartTime,
            other.backfillStartTime) && Objects.equals(this.backfillEndTime, other.backfillEndTime);
      } else {
        return false;
      }
    }
  }
}
