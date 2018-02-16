package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.alert.util.EmailHelper;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.onboard.tasks.DefaultDetectionOnboardJob;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.codehaus.jackson.map.ObjectMapper;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DetectionOnBoardJobRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionOnBoardJobRunner.class);
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String ABORT_ON_FAILURE= "abortOnFailure";
  private final DetectionOnboardJobContext jobContext;
  private final List<DetectionOnboardTask> tasks;
  private final DetectionOnboardJobStatus jobStatus;
  private final int taskTimeOutSize;
  private final TimeUnit taskTimeOutUnit;
  private final SmtpConfiguration smtpConfiguration;
  private final String failureNotificationSender;
  private final String failureNotificationReceiver;
  private final boolean notifyIfFails;

  public DetectionOnBoardJobRunner(DetectionOnboardJobContext jobContext, List<DetectionOnboardTask> tasks,
      DetectionOnboardJobStatus jobStatus) {
    this(jobContext, tasks, jobStatus, 30, TimeUnit.MINUTES);
  }

  public DetectionOnBoardJobRunner(DetectionOnboardJobContext jobContext, List<DetectionOnboardTask> tasks,
      DetectionOnboardJobStatus jobStatus, int taskTimeOutSize, TimeUnit taskTimeOutUnit) {
    Preconditions.checkNotNull(jobContext);
    Preconditions.checkNotNull(tasks);
    Preconditions.checkNotNull(jobStatus);
    Preconditions.checkNotNull(taskTimeOutUnit);

    this.jobContext = jobContext;
    this.tasks = tasks;
    this.jobStatus = jobStatus;
    this.taskTimeOutSize = taskTimeOutSize;
    this.taskTimeOutUnit = taskTimeOutUnit;
    Configuration configuration = jobContext.getConfiguration();
    if (configuration.containsKey(DefaultDetectionOnboardJob.SMTP_HOST)) {
      smtpConfiguration = new SmtpConfiguration();
      smtpConfiguration.setSmtpHost(configuration.getString(DefaultDetectionOnboardJob.SMTP_HOST));
      smtpConfiguration.setSmtpPort(configuration.getInt(DefaultDetectionOnboardJob.SMTP_PORT));
    } else {
      smtpConfiguration = null;
    }
    failureNotificationSender = configuration.getString(DefaultDetectionOnboardJob.DEFAULT_ALERT_SENDER_ADDRESS);
    failureNotificationReceiver = configuration.getString(DefaultDetectionOnboardJob.DEFAULT_ALERT_RECEIVER_ADDRESS);
    notifyIfFails = configuration.getBoolean(DefaultDetectionOnboardJob.NOTIFY_IF_FAILS,
        DefaultDetectionOnboardJob.DEFAULT_NOTIFY_IF_FAILS);
  }

  @Override
  public void run() {
    Preconditions.checkNotNull(jobContext);
    Preconditions.checkNotNull(tasks);
    Preconditions.checkNotNull(jobStatus);

    for (DetectionOnboardTask task : tasks) {
      DetectionOnboardTaskStatus taskStatus =
          new DetectionOnboardTaskStatus(task.getTaskName(), TaskConstants.TaskStatus.WAITING, "");
      jobStatus.addTaskStatus(taskStatus);

      // Construct Task context and configuration
      Configuration taskConfig = jobContext.getConfiguration().subset(task.getTaskName());
      final boolean abortOnFailure = taskConfig.getBoolean(ABORT_ON_FAILURE, true);
      DetectionOnboardTaskContext taskContext = new DetectionOnboardTaskContext();
      taskContext.setConfiguration(taskConfig);
      taskContext.setExecutionContext(jobContext.getExecutionContext());

      // Submit task
      try {
        task.setTaskContext(taskContext);
        taskStatus.setTaskStatus(TaskConstants.TaskStatus.RUNNING);
        Future<DetectionOnboardTaskStatus> taskFuture = executorService.submit(new DetectionOnboardTaskRunner(task));
        // Wait until time out
        DetectionOnboardTaskStatus returnedTaskStatus = taskFuture.get(taskTimeOutSize, taskTimeOutUnit);
        taskStatus.setTaskStatus(returnedTaskStatus.getTaskStatus());
        taskStatus.setMessage(returnedTaskStatus.getMessage());
      } catch (TimeoutException e) {
        taskStatus.setTaskStatus(TaskConstants.TaskStatus.TIMEOUT);
        LOG.warn("Task {} timed out.", task.getTaskName());
      } catch (InterruptedException e) {
        taskStatus.setTaskStatus(TaskConstants.TaskStatus.FAILED);
        taskStatus.setMessage("Job execution is interrupted.");
        jobStatus.setJobStatus(JobConstants.JobStatus.FAILED);
        jobStatus.setMessage(String.format("Job execution is interrupted: %s", ExceptionUtils.getStackTrace(e)));
        LOG.error("Job execution is interrupted.", e);
        return; // Stop executing the job because the thread to execute the job is interrupted.
      } catch (Exception e) {
        taskStatus.setTaskStatus(TaskConstants.TaskStatus.FAILED);
        taskStatus.setMessage(String.format("Execution Error: %s", ExceptionUtils.getStackTrace(e)));
        LOG.error("Encountered unknown error while running job {}.", jobContext.getJobName(), e);
      }

      // Notify upon exception
      if (notifyIfFails && !TaskConstants.TaskStatus.COMPLETED.equals(taskStatus.getTaskStatus())) {
        if (smtpConfiguration == null || StringUtils.isBlank(failureNotificationSender)
            || StringUtils.isBlank(failureNotificationReceiver)) {
          LOG.warn("SmtpConfiguration, and email sender/recipients cannot be null or empty");
        } else {
          try {
            sendFailureEmail();
          } catch (JobExecutionException e) {
            LOG.warn("Unable to send failure emails");
          }
        }
      }

      if (abortOnFailure && !TaskConstants.TaskStatus.COMPLETED.equals(taskStatus.getTaskStatus())) {
        jobStatus.setJobStatus(JobConstants.JobStatus.FAILED);
        LOG.error("Failed to execute job {}.", jobContext.getJobName());
        return;
      }
    }
    jobStatus.setJobStatus(JobConstants.JobStatus.COMPLETED);
  }

  private void sendFailureEmail() throws JobExecutionException {
    HtmlEmail email = new HtmlEmail();
    String subject = String
        .format("[ThirdEye Onboarding Job] FAILED Onboarding Job Id=%d for config %s", jobContext.getJobId(),
            jobContext.getJobName());
    String jobStatusString;
    try {
      jobStatusString = OBJECT_MAPPER.writeValueAsString(jobStatus);
    } catch (IOException e) {
      LOG.warn("Unable to parse job context {}", jobContext);
      jobStatusString = jobStatus.toString();
    }
    String htmlBody = String
        .format("<h1>Job Status</h1><p>%s</p>", jobStatusString);
    try {
      EmailHelper
          .sendEmailWithHtml(email, smtpConfiguration, subject, htmlBody,
              failureNotificationSender, failureNotificationReceiver);
    } catch (EmailException e) {
      throw new JobExecutionException(e);
    }
  }
}
