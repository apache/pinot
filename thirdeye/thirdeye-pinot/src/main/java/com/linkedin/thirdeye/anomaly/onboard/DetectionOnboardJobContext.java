package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

public class DetectionOnboardJobContext {
  private String jobName;
  private long jobId;
  private Configuration configuration;
  private DetectionOnboardExecutionContext executionContext = new DetectionOnboardExecutionContext();

  public DetectionOnboardJobContext(long jobId, String jobName, Configuration configuration) {
    setJobName(jobName);
    setJobId(jobId);
    setConfiguration(configuration);
  }

  /**
   * Returns the unique name of the job.
   * @return the unique name of the job.
   */
  public String getJobName() {
    return jobName;
  }

  /**
   * Sets the name of the job. The name cannot be null or an empty string. Any white space before and after the name
   * will be trimmed.
   *
   * @param jobName the name of the job.
   */
  private void setJobName(String jobName) {
    Preconditions.checkNotNull(jobName);
    Preconditions.checkArgument(StringUtils.isNotBlank(jobName.trim()), "Job name cannot be empty.");
    this.jobName = jobName.trim();
  }

  /**
   * Returns the id of the job.
   *
   * @return the id of the job.
   */
  public long getJobId() {
    return jobId;
  }

  /**
   * Sets the id of the job.
   *
   * @param jobId the id of the job.
   */
  private void setJobId(long jobId) {
    this.jobId = jobId;
  }

  /**
   * Returns the configuration of the job.
   *
   * @return the configuration of the job.
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Sets the configuration of the job.
   *
   * @param configuration the configuration of the job.
   */
  private void setConfiguration(Configuration configuration) {
    Preconditions.checkNotNull(configuration);
    this.configuration = configuration;
  }

  /**
   * Returns the execution context (i.e., execution results from all tasks) of this job.
   *
   * @return the execution context of this job.
   */
  public DetectionOnboardExecutionContext getExecutionContext() {
    return executionContext;
  }

  /**
   * Sets the execution context (i.e., execution results from all tasks) of this job. The context cannot be null.
   *
   * @param executionContext the execution context.
   */
  public void setExecutionContext(DetectionOnboardExecutionContext executionContext) {
    Preconditions.checkNotNull(executionContext);
    this.executionContext = executionContext;
  }
}
