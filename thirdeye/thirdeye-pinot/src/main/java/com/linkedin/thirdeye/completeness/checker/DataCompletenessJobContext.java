package com.linkedin.thirdeye.completeness.checker;


import com.linkedin.thirdeye.anomaly.job.JobContext;

/**
 * job context for data completeness jobs
 */
public class DataCompletenessJobContext extends JobContext {

   private long checkDurationStartTime;
   private long checkDurationEndTime;

  public long getCheckDurationStartTime() {
    return checkDurationStartTime;
  }
  public void setCheckDurationStartTime(long checkDurationStartTime) {
    this.checkDurationStartTime = checkDurationStartTime;
  }
  public long getCheckDurationEndTime() {
    return checkDurationEndTime;
  }
  public void setCheckDurationEndTime(long checkDurationEndTime) {
    this.checkDurationEndTime = checkDurationEndTime;
  }

}
