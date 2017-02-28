package com.linkedin.thirdeye.anomaly.detectionex;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.anomaly.job.JobScheduler;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;
import com.linkedin.thirdeye.detector.functionex.impl.AlwaysAnomalyDummy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.quartz.SimpleTrigger;
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

        dto.setActive(false);
        dto.setCron("0 * * * * ?");
        dto.setConfig(config);
        dto.setMonitoringWindowAlignment(60000);
        dto.setMonitoringWindowLookback(300000);
        dto.setMergeWindow(300000);

        dto.setDisplayMetric("num_login_attempt");
        dto.setDisplayCollection("login_hourly_additive");

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
          startJobOnline(spec, jobKey);
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
            startJobOnline(spec, jobKey);
          }
        }

        // for all jobs with not isActive, and not isScheduled, no change required

      }

      // for any scheduled jobs, not having a function in the database,
      // stop the schedule, as function has been deleted
      for (String scheduledJobKey : scheduledJobs) {
        Long functionId = getIdFromJobKey(scheduledJobKey);
        AnomalyFunctionExDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionExDAO().findById(functionId);
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
    startJobOnline(anomalyFunctionSpec, jobKey);
  }

  public void startJobOnline(AnomalyFunctionExDTO spec, String jobKey) throws SchedulerException {
    if (quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Anomaly function " + jobKey + " is already scheduled");
    }
    DetectionExJobContext context = new DetectionExJobContext();
    context.setType(DetectionExJobContext.DetectionJobType.ONLINE);
    context.setAnomalyFunctionId(spec.getId());
    context.setJobName(jobKey);
    context.setMonitoringWindowAlignment(spec.getMonitoringWindowAlignment());
    context.setMonitoringWindowLookback(spec.getMonitoringWindowLookback());
    context.setMergeWindow(spec.getMergeWindow());

    scheduleJobRecurring(context, spec);
  }

  public void startJobBackfill(AnomalyFunctionExDTO spec, String jobKey, long start, long end) throws SchedulerException {
    if (quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Anomaly function " + jobKey + " is already scheduled");
    }
    DetectionExJobContext context = new DetectionExJobContext();
    context.setType(DetectionExJobContext.DetectionJobType.BACKFILL);
    context.setAnomalyFunctionId(spec.getId());
    context.setJobName(jobKey);
    context.setMonitoringWindowStart(start);
    context.setMonitoringWindowEnd(end);
    context.setMergeWindow(spec.getMergeWindow());

    scheduleJobOnce(context, spec);
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

  private void scheduleJobRecurring(JobContext jobContext, AnomalyFunctionExDTO spec) {

    String triggerKey = String.format("anomalyex_scheduler_trigger_%d", spec.getId());
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

    LOG.info("Started recurring {}: {}", jobKey, spec);
  }

  private void scheduleJobOnce(JobContext jobContext, AnomalyFunctionExDTO spec) {
    String jobKey = jobContext.getJobName();
    JobDetail job = JobBuilder.newJob(DetectionExJobRunner.class).withIdentity(jobKey).build();

    job.getJobDataMap().put(DetectionExJobRunner.DETECTION_EX_JOB_CONTEXT, jobContext);

    String triggerKey = String.format("anomalyex_scheduler_trigger_%d_oneoff", spec.getId());
    Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).startNow().build();

    try {
      quartzScheduler.scheduleJob(job, trigger);
    } catch (SchedulerException e) {
      LOG.error("Exception while scheduling job", e);
    }

    LOG.info("Started one-off {}: {}", jobKey, spec);
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
   */
  public void runBackfill(long functionId, DateTime backfillStartTime, DateTime backfillEndTime) throws Exception {
    AnomalyFunctionExDTO spec = DAO_REGISTRY.getAnomalyFunctionExDAO().findById(functionId);
    if (spec == null) {
      throw new IllegalArgumentException("No function with id " + functionId);
    }

    long windowStart = backfillStartTime.getMillis();
    long windowEnd = backfillEndTime.getMillis();

    String jobKey = String.format("%s_oneoff_%d_%d", getJobKey(functionId, spec.getName()), windowStart, windowEnd);

    startJobBackfill(spec, jobKey, windowStart, windowEnd);
  }
}
