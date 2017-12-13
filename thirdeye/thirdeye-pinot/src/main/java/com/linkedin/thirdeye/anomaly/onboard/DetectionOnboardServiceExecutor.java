package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.configuration.Configuration;

public class DetectionOnboardServiceExecutor implements DetectionOnboardService {

  private ConcurrentHashMap<Long, DetectionOnboardJobStatus> detectionOnboardJobStatus;
  private ConcurrentHashMap<String, Long> jobNameToId;
  private AtomicLong jobIdCounter = new AtomicLong();
  private ExecutorService executorService;

  public void start() {
    detectionOnboardJobStatus = new ConcurrentHashMap<>();
    jobNameToId = new ConcurrentHashMap<>();
    executorService = Executors.newCachedThreadPool();
  }

  public void shutdown() {
    AnomalyUtils.safelyShutdownExecutionService(executorService, DetectionOnboardServiceExecutor.class);
  }

  @Override
  public DetectionOnboardJobStatus createDetectionOnboardingJob(DetectionOnboardJob job, Map<String, String> properties) {
    Preconditions.checkNotNull(detectionOnboardJobStatus);
    Preconditions.checkNotNull(jobNameToId);
    Preconditions.checkNotNull(executorService);
    Preconditions.checkNotNull(job);
    Preconditions.checkNotNull(job.getName());
    Preconditions.checkNotNull(properties);

    long jobId = jobIdCounter.getAndIncrement();
    DetectionOnboardJobStatus jobStatus = new DetectionOnboardJobStatus();
    jobStatus.setJobId(jobId);
    detectionOnboardJobStatus.put(jobId, jobStatus);

    String jobName = job.getName();
    // Duplicated job name will not be executed, which could happen in the following two cases:
    // 1. A concurrent thread has insert the job before current thread.
    // 2. A job with the same name is submitted again after a while.
    Long previousJobId = jobNameToId.putIfAbsent(jobName, jobId);
    if (previousJobId != null) {
      jobStatus.setJobStatus(JobConstants.JobStatus.FAILED);
      jobStatus.setMessage(String.format("Duplicate detection onboard job name: %s", jobName));
    } else {
      // Initialize the job
      ImmutableMap<String, String> immutableProperties = ImmutableMap.copyOf(properties);
      job.initialize(immutableProperties);
      List<DetectionOnboardTask> tasks = job.getTasks();
      Configuration configuration = job.getTaskConfiguration();
      DetectionOnboardJobContext jobContext = new DetectionOnboardJobContext(jobName, jobId, configuration);

      try {
        // Submit the job to executor
        DetectionOnBoardJobRunner jobRunner = new DetectionOnBoardJobRunner(jobContext, tasks, jobStatus);
        jobStatus.setJobStatus(JobConstants.JobStatus.SCHEDULED);
        executorService.submit(jobRunner);
      } catch (RejectedExecutionException e) {
        jobStatus.setJobStatus(JobConstants.JobStatus.FAILED);
        jobStatus.setMessage(String.format("Failed to schedule job %s because executor has been shutdown.", jobName));
      }
    }
    return jobStatus;
  }

  @Override
  public DetectionOnboardJobStatus getDetectionOnboardingJobStatus(long jobId) {
    return detectionOnboardJobStatus.get(jobId);
  }
}
