package com.linkedin.thirdeye.completeness.checker;


import java.util.List;

import com.linkedin.thirdeye.anomaly.job.JobContext;

/**
 * job context for data completeness jobs
 */
public class DataCompletenessJobContext extends JobContext {

   private long checkDurationStartTime;
   private long checkDurationEndTime;
   private List<String> datasetsToCheck;

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
  public List<String> getDatasetsToCheck() {
    return datasetsToCheck;
  }
  public void setDatasetsToCheck(List<String> datasetsToCheck) {
    this.datasetsToCheck = datasetsToCheck;
  }

}
