package com.linkedin.thirdeye.anomalydetection.model.data;

import java.util.Collections;
import java.util.List;
import org.joda.time.Interval;

public class NoopDataModel extends AbstractDataModel {
  @Override public List<Interval> getTrainingDataIntervals(long monitoringWindowStartTime,
      long monitoringWindowEndTime) {
    return Collections.emptyList();
  }
}
