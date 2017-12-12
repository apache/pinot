package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
  public Long createDetectionOnboardingJob(String jobName, Map<String, String> properties) {
    Preconditions.checkNotNull(jobName);
    Preconditions.checkNotNull(properties);

    if (jobNameToId.contains(jobName)) {
      return null;
    } else {
      long jobId = jobIdCounter.getAndIncrement();
      Long previousJobId = jobNameToId.putIfAbsent(jobName, jobId);
      if (previousJobId != null) { // A concurrent thread has insert the job before current thread
        return null;
      } else {
        // Initialize the job
        DetectionOnboardJob detectionOnboardJob = new DetectionOnboardJob();
        Configuration configuration = detectionOnboardJob.generateConfiguration(properties);
        DetectionOnboardJobContext detectionOnboardJobContext =
            new DetectionOnboardJobContext(jobName, jobId, configuration);
        List<DetectionOnboardTask> tasks = detectionOnboardJob.createTasks();

        DetectionOnboardJobStatus jobStatus = new DetectionOnboardJobStatus();
        jobStatus.setJobStatus(JobConstants.JobStatus.SCHEDULED);

        DetectionOnBoardJobRunner jobRunner =
            new DetectionOnBoardJobRunner(detectionOnboardJobContext, tasks, jobStatus);

        // Submit the job to executor
        executorService.submit(jobRunner);
        detectionOnboardJobStatus.put(jobId, jobStatus);

        return jobId;
      }
    }
  }

  @Override
  public DetectionOnboardJobStatus getDetectionOnboardingJobStatus(long jobId) {
    return detectionOnboardJobStatus.get(jobId);
  }
}
