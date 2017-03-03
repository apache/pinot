package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.CronExpression;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;

/**
 * Scheduler for anomaly detection jobs
 */
public class DetectionJobScheduler implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobScheduler.class);
  private ScheduledExecutorService scheduledExecutorService;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY  = ThirdEyeCacheRegistry.getInstance();
  private DetectionJobRunner detectionJobRunner = new DetectionJobRunner();

  private static final int BACKFILL_MAX_RETRY = 3;
  private static final int BACKFILL_TASK_POLL_TIME = 5_000; // Period to check if a task is finished
  private static final int BACKFILL_RESCHEDULE_TIME = 15_000; // Pause before reschedule a failed job
  private final Map<BackfillKey, Thread> existingBackfillJobs = new ConcurrentHashMap<>();

  public DetectionJobScheduler() {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }


  public void start() throws SchedulerException {
    scheduledExecutorService.scheduleAtFixedRate(this, 0, 15, TimeUnit.MINUTES);
  }

  /**
   * Reads all active anomaly functions
   * For each function, finds all time periods for which detection needs to be run
   * Calls run anomaly function for all those periods, and updates detection status
   * {@inheritDoc}
   * @see java.lang.Runnable#run()
   */
  public void run() {

    // read all anomaly functions
    LOG.info("Reading all anomaly functions");
    List<AnomalyFunctionDTO> anomalyFunctions = DAO_REGISTRY.getAnomalyFunctionDAO().findAllActiveFunctions();

    // for each active anomaly function
    for (AnomalyFunctionDTO anomalyFunction : anomalyFunctions) {

      try {
        LOG.info("Function {}", anomalyFunction);
        long functionId = anomalyFunction.getId();
        String dataset = anomalyFunction.getCollection();
        DatasetConfigDTO datasetConfig = CACHE_REGISTRY.getDatasetConfigCache().get(dataset);
        DateTimeZone dateTimeZone = Utils.getDataTimeZone(dataset);
        DateTime currentDateTime = new DateTime(dateTimeZone);

        // find last entry into detectionStatus table, for this function
        DetectionStatusDTO lastEntryForFunction = DAO_REGISTRY.getDetectionStatusDAO().
            findLatestEntryForFunctionId(functionId);
        LOG.info("Last entry for function {} is {}", functionId, lastEntryForFunction);

        // calculate entries from last entry to current time
        Map<String, Long> newEntries = DetectionJobSchedulerUtils.getNewEntries(currentDateTime, lastEntryForFunction,
            anomalyFunction, datasetConfig, dateTimeZone);
        LOG.info("Creating {} new entries {}", newEntries.size(), newEntries);

        // create these entries
        for (Entry<String, Long> entry : newEntries.entrySet()) {
          DetectionStatusDTO detectionStatus = new DetectionStatusDTO();
          detectionStatus.setDataset(anomalyFunction.getCollection());
          detectionStatus.setFunctionId(functionId);
          detectionStatus.setDateToCheckInSDF(entry.getKey());
          detectionStatus.setDateToCheckInMS(entry.getValue());
          DAO_REGISTRY.getDetectionStatusDAO().save(detectionStatus);
        }

        // find all entries in the past 3 days, which are still isRun = false
        List<DetectionStatusDTO> entriesInLast3Days = DAO_REGISTRY.getDetectionStatusDAO().
            findAllInTimeRangeForFunctionAndDetectionRun(currentDateTime.minusDays(3).getMillis(),
                currentDateTime.getMillis(), functionId, false);
        Collections.sort(entriesInLast3Days);
        LOG.info("Entries in last 3 days {}", entriesInLast3Days);

        // for each entry, collect startTime and endTime
        List<Long> startTimes = new ArrayList<>();
        List<Long> endTimes = new ArrayList<>();
        List<DetectionStatusDTO> detectionStatusToUpdate = new ArrayList<>();

        for (DetectionStatusDTO detectionStatus : entriesInLast3Days) {

          try {
            LOG.info("Entry : {}", detectionStatus);

            long dateToCheck = detectionStatus.getDateToCheckInMS();
            // check availability for monitoring window - delay
            long endTime = dateToCheck - TimeUnit.MILLISECONDS.convert(anomalyFunction.getWindowDelay(), anomalyFunction.getWindowDelayUnit());
            long startTime = endTime - TimeUnit.MILLISECONDS.convert(anomalyFunction.getWindowSize(), anomalyFunction.getWindowUnit());
            LOG.info("Checking start:{} {} to end:{} {}", startTime, new DateTime(startTime, dateTimeZone), endTime, new DateTime(endTime, dateTimeZone));

            boolean pass = checkIfDetectionRunCriteriaMet(startTime, endTime, datasetConfig, anomalyFunction);
            if (pass) {
              startTimes.add(startTime);
              endTimes.add(endTime);
              detectionStatusToUpdate.add(detectionStatus);
            } else {
              LOG.warn("Data incomplete for monitoring window {} ({}) to {} ({}), skipping anomaly detection",
                  startTime, new DateTime(startTime), endTime, new DateTime(endTime));
              // TODO: Send email to owners/dev team
            }
          } catch (Exception e) {
            LOG.error("Exception in preparing entry {}", detectionStatus, e);
          }
        }

        // If any time periods found, for which detection needs to be run
        if (!startTimes.isEmpty() && !endTimes.isEmpty() && startTimes.size() == endTimes.size()) {
          long jobExecutionId = runAnomalyFunctionOnRanges(anomalyFunction, startTimes, endTimes);
          LOG.info("Created job {} for running anomaly function {} on ranges {} to {}",
              jobExecutionId, anomalyFunction, startTimes, endTimes);

          for (DetectionStatusDTO detectionStatus : detectionStatusToUpdate) {
            LOG.info("Updating detection run status {} to true", detectionStatus);
            detectionStatus.setDetectionRun(true);
            DAO_REGISTRY.getDetectionStatusDAO().update(detectionStatus);
          }
        }
      } catch (Exception e) {
        LOG.error("Exception in running anomaly function {}", anomalyFunction, e);
      }
    }
  }


  /**
   * Point of entry for rest endpoints calling adhoc anomaly functions
   * TODO: Not updating detection status in case of adhoc currently, reconsider
   * @param functionId
   * @param startTime
   * @param endTime
   * @return job execution id
   */
  public Long runAdhocAnomalyFunction(Long functionId, Long startTime, Long endTime) {
    Long jobExecutionId = null;

    AnomalyFunctionDTO anomalyFunction = DAO_REGISTRY.getAnomalyFunctionDAO().findById(functionId);

    String dataset = anomalyFunction.getCollection();
    DatasetConfigDTO datasetConfig = null;
    try {
      datasetConfig = CACHE_REGISTRY.getDatasetConfigCache().get(dataset);
    } catch (ExecutionException e) {
      LOG.error("Exception in fetching dataset config", e);
    }

    boolean pass = checkIfDetectionRunCriteriaMet(startTime, endTime, datasetConfig, anomalyFunction);
    if (pass) {
      jobExecutionId = runAnomalyFunctionOnRanges(anomalyFunction, Lists.newArrayList(startTime), Lists.newArrayList(endTime));
    } else {
      LOG.warn("Data incomplete for monitoring window {} ({}) to {} ({}), skipping anomaly detection",
          startTime, new DateTime(startTime), endTime, new DateTime(endTime));
      // TODO: Send email to owners/dev team
    }
    return jobExecutionId;
  }

  /**
   * Checks if a time range for a dataset meets data completeness criteria
   * @param startTime
   * @param endTime
   * @param datasetConfig
   * @param anomalyFunction
   * @return true if data completeness check is requested and passes, or data completeness check is not requested at all
   * false if data completeness check is requested and fails
   */
  private boolean checkIfDetectionRunCriteriaMet(Long startTime, Long endTime, DatasetConfigDTO datasetConfig, AnomalyFunctionDTO anomalyFunction) {
    boolean pass = false;
    String dataset = datasetConfig.getDataset();

    /**
     * Check is completeness check required is set at dataset level. That flag is false by default, so user will set as needed
     * Check also for same flag in function level. That flag is true by default, so dataset config's flag will have its way unless user has tampered with this flag
     * This flag would typically be unset, in backfill cases
     */
    if (datasetConfig.isRequiresCompletenessCheck() && anomalyFunction.isRequiresCompletenessCheck()) {

      LOG.info("Checking for completeness for dataset {} and time range {}({}) to {}({})", dataset,
          startTime, new DateTime(startTime), endTime, new DateTime(endTime));

      List<DataCompletenessConfigDTO> incompleteTimePeriods = DAO_REGISTRY.getDataCompletenessConfigDAO().
          findAllByDatasetAndInTimeRangeAndStatus(dataset, startTime, endTime, false);
      LOG.info("Incomplete periods {}", incompleteTimePeriods);

      if (incompleteTimePeriods.size() == 0) { // nothing incomplete
        // find complete buckets
        List<DataCompletenessConfigDTO> completeTimePeriods = DAO_REGISTRY.getDataCompletenessConfigDAO().
            findAllByDatasetAndInTimeRangeAndStatus(dataset, startTime, endTime, true);
        LOG.info("Complete periods {}", completeTimePeriods);
        long expectedCompleteBuckets = DetectionJobSchedulerUtils.getExpectedCompleteBuckets(datasetConfig, startTime, endTime);
        LOG.info("Num complete periods: {} Expected num buckets:{}", completeTimePeriods.size(), expectedCompleteBuckets);

        if (completeTimePeriods.size() == expectedCompleteBuckets) { // complete matches expected
          LOG.info("Found complete time range for dataset and time range {}({}) to {}({})", dataset,
              startTime, new DateTime(startTime), endTime, new DateTime(endTime));
          pass = true;
        }
      }
    } else { // no check required
      pass = true;
    }
    return pass;
  }

  /**
   * Creates detection context and runs anomaly job, returns jobExecutionId
   * @param anomalyFunction
   * @param startTimes
   * @param endTimes
   * @return
   */
  private Long runAnomalyFunctionOnRanges(AnomalyFunctionDTO anomalyFunction, List<Long> startTimes, List<Long> endTimes) {
    DetectionJobContext detectionJobContext = new DetectionJobContext();
    detectionJobContext.setAnomalyFunctionId(anomalyFunction.getId());
    detectionJobContext.setAnomalyFunctionSpec(anomalyFunction);
    detectionJobContext.setJobName(DetectionJobSchedulerUtils.createJobName(anomalyFunction, startTimes, endTimes));
    detectionJobContext.setStartTimes(startTimes);
    detectionJobContext.setEndTimes(endTimes);
    Long jobExecutionId = detectionJobRunner.run(detectionJobContext);
    LOG.info("Created job {}", jobExecutionId);
    return jobExecutionId;
  }

  public void shutdown() throws SchedulerException{
    scheduledExecutorService.shutdown();
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

        Long jobExecutionId = runAdhocAnomalyFunction(functionId, currentStart.getMillis(), currentEnd.getMillis());

        // Synchronously and periodically check if job is done
        boolean status = waitUntilJobIsDone(jobExecutionId);

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
   * @param jobExecutionId
   * @return
   */
  private JobDTO tryToGetJob(Long jobExecutionId) {
    JobDTO job = null;
    for (int i = 0; i < BACKFILL_MAX_RETRY; ++i) {
      job = DAO_REGISTRY.getJobDAO().findById(jobExecutionId);
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
  private boolean waitUntilJobIsDone(Long jobExecutionId) {
    // A new job may not be stored to database in time, so we try to read the job BACKFILL_MAX_RETRY times
    JobDTO job = tryToGetJob(jobExecutionId);

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
