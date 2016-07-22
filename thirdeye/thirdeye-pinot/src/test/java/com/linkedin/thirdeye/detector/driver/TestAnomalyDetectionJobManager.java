package com.linkedin.thirdeye.detector.driver;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAnomalyDetectionJobManager {

  @Test
  public void getSimulationIntervals() throws ParseException {
    // Date irrelevant. We want jobs from 00H - 01H window with cron @ 00min, 20min, and 45min
    // marks, with data size of an
    // hour.
    String cron = "0 00,20,45 * * * ?";
    DateTime start = new DateTime("2016-06-01T00:00:00");
    DateTime end = start.plusHours(1);
    long dataWindowMillis = TimeUnit.HOURS.toMillis(1);
    List<DateTime> fireTimes =
        Arrays.asList(new DateTime("2016-06-01T00:00:00"), new DateTime("2016-06-01T00:20:00"),
            new DateTime("2016-06-01T00:45:00"));
    List<Interval> expected = new ArrayList<>();
    for (DateTime endDate : fireTimes) {
      expected.add(new Interval(endDate.minus(dataWindowMillis), endDate));
    }

    List<Interval> actual =
        AnomalyDetectionJobManager.getSimulationIntervals(start, end, dataWindowMillis, cron);
    Assert.assertEquals(actual, expected);
  }
}
