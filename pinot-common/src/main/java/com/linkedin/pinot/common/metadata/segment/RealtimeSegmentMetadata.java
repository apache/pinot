package com.linkedin.pinot.common.metadata.segment;

import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;


public class RealtimeSegmentMetadata {

  private long _startTime;
  private long _endTime;
  private TimeUnit _timeUnit;
  private Status _status;

  public long getStartTime() {
    return _startTime;
  }

  public void setStartTime(long startTime) {
    _startTime = startTime;
  }

  public long getEndTime() {
    return _endTime;
  }

  public void setEndTime(long endTime) {
    _endTime = endTime;
  }

  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  public void setTimeUnit(TimeUnit timeUnit) {
    _timeUnit = timeUnit;
  }

  public Status getStatus() {
    return _status;
  }

  public void setStatus(Status status) {
    _status = status;
  }

}
