package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

public class DetectionOnboardJobContext {
  private String jobName;
  private long jobId;
  private Configuration configuration;
  private DetectionOnboardExecutionContext executionContext = new DetectionOnboardExecutionContext();

  public DetectionOnboardJobContext(String jobName, long jobId, Configuration configuration) {
    setJobName(jobName);
    setJobId(jobId);
    setConfiguration(configuration);
  }

  public String getJobName() {
    return jobName;
  }

  private void setJobName(String jobName) {
    Preconditions.checkNotNull(jobName);
    Preconditions.checkArgument(StringUtils.isNotBlank(jobName.trim()), "Job name cannot be empty.");
    this.jobName = jobName.trim();
  }

  public long getJobId() {
    return jobId;
  }

  private void setJobId(long jobId) {
    this.jobId = jobId;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  private void setConfiguration(Configuration configuration) {
    Preconditions.checkNotNull(configuration);
    this.configuration = configuration;
  }

  public DetectionOnboardExecutionContext getExecutionContext() {
    return executionContext;
  }

  public void setExecutionContext(DetectionOnboardExecutionContext executionContext) {
    Preconditions.checkNotNull(executionContext);
    this.executionContext = executionContext;
  }
}
