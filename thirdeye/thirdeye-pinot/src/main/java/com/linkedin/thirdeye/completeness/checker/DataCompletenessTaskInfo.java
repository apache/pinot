package com.linkedin.thirdeye.completeness.checker;

import java.util.Objects;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessConstants.DataCompletenessType;

public class DataCompletenessTaskInfo implements TaskInfo {

  private DataCompletenessType dataCompletenessType;
  private long dataCompletenessStartTime;
  private long dataCompletenessEndTime;


  public DataCompletenessTaskInfo() {

  }

  public DataCompletenessType getDataCompletenessType() {
    return dataCompletenessType;
  }

  public void setDataCompletenessType(DataCompletenessType dataCompletenessType) {
    this.dataCompletenessType = dataCompletenessType;
  }

  public long getDataCompletenessStartTime() {
    return dataCompletenessStartTime;
  }

  public void setDataCompletenessStartTime(long dataCompletenessStartTime) {
    this.dataCompletenessStartTime = dataCompletenessStartTime;
  }

  public long getDataCompletenessEndTime() {
    return dataCompletenessEndTime;
  }

  public void setDataCompletenessEndTime(long dataCompletenessEndTime) {
    this.dataCompletenessEndTime = dataCompletenessEndTime;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DataCompletenessTaskInfo)) {
      return false;
    }
    DataCompletenessTaskInfo dc = (DataCompletenessTaskInfo) o;
    return Objects.equals(dataCompletenessType, dc.getDataCompletenessType())
        && Objects.equals(dataCompletenessStartTime, dc.getDataCompletenessStartTime())
        && Objects.equals(dataCompletenessEndTime, dc.getDataCompletenessEndTime());
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataCompletenessType, dataCompletenessStartTime, dataCompletenessEndTime);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("dataCompletenessType", dataCompletenessType)
        .add("startTime", dataCompletenessStartTime).add("endTime", dataCompletenessEndTime).toString();
  }
}
