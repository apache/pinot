package com.linkedin.thirdeye.anomalydetection.model.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.joda.time.Interval;

public class NoopDataModel extends AbstractDataModel {
  @Override public List<Interval> getAllDataIntervals(long monitoringWindowStartTime,
      long monitoringWindowEndTime) {
    Interval interval = new Interval(monitoringWindowStartTime, monitoringWindowEndTime);
    List<Interval> ret = new ArrayList<>();
    ret.add(interval);
    return ret;
  }

  @Override public List<Interval> getTrainingDataIntervals(long monitoringWindowStartTime,
      long monitoringWindowEndTime) {
    return Collections.emptyList();
  }
}
