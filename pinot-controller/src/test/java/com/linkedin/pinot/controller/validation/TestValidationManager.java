package com.linkedin.pinot.controller.validation;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the ValidationManager.
 *
 * @author jfim
 */
public class TestValidationManager {
  @Test
  public void testCountMissingSegments() {
    // Should not crash with an empty or one element arrays
    ValidationManager.countMissingSegments(new long[0], TimeUnit.DAYS);
    ValidationManager.countMissingSegments(new long[1], TimeUnit.DAYS);

    // Should be no missing segments on two consecutive days
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 2, 22, 0).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        0
    );

    // Should be no missing segments on five consecutive days
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 2, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 4, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 0).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        0
    );

    // Should be no missing segments on five consecutive days, even if the interval between them isn't exactly 24 hours
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 2, 21, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 23, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 4, 21, 5).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        0
    );

    // Should be no missing segments on five consecutive days, even if there is a duplicate segment
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 2, 21, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 23, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 4, 21, 5).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        0
    );

    // Should be exactly one missing segment
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 2, 21, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 4, 21, 5).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        1
    );

    // Should be one missing segment, even if there is a duplicate segment
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 23, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 4, 21, 5).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        1
    );

    // Should be two missing segments
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 23, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        2
    );

    // Should be three missing segments
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        3
    );

    // Should be three missing segments
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 25).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        3
    );
  }

  @Test
  public void testComputeMissingIntervals() {
    Interval jan1st = new Interval(new DateTime(2015, 1, 1, 0, 0, 0), new DateTime(2015, 1, 1, 23, 59, 59));
    Interval jan2nd = new Interval(new DateTime(2015, 1, 2, 0, 0, 0), new DateTime(2015, 1, 2, 23, 59, 59));
    Interval jan3rd = new Interval(new DateTime(2015, 1, 3, 0, 0, 0), new DateTime(2015, 1, 3, 23, 59, 59));
    Interval jan4th = new Interval(new DateTime(2015, 1, 4, 0, 0, 0), new DateTime(2015, 1, 4, 23, 59, 59));
    Interval jan5th = new Interval(new DateTime(2015, 1, 5, 0, 0, 0), new DateTime(2015, 1, 5, 23, 59, 59));

    ArrayList<Interval> jan1st2nd3rd = new ArrayList<Interval>();
    jan1st2nd3rd.add(jan1st);
    jan1st2nd3rd.add(jan2nd);
    jan1st2nd3rd.add(jan3rd);
    List<Interval> missingIntervalsForJan1st2nd3rd =
        ValidationManager.computeMissingIntervals(jan1st2nd3rd, Duration.standardDays(1));

    Assert.assertTrue(missingIntervalsForJan1st2nd3rd.isEmpty());

    ArrayList<Interval> jan1st2nd3rd5th = new ArrayList<Interval>(jan1st2nd3rd);
    jan1st2nd3rd5th.add(jan5th);
    List<Interval> missingIntervalsForJan1st2nd3rd5th =
        ValidationManager.computeMissingIntervals(jan1st2nd3rd5th, Duration.standardDays(1));

    Assert.assertEquals(missingIntervalsForJan1st2nd3rd5th.size(), 1);

    // Should also work if the intervals are in random order
    ArrayList<Interval> jan5th2nd1st = new ArrayList<Interval>();
    jan5th2nd1st.add(jan5th);
    jan5th2nd1st.add(jan2nd);
    jan5th2nd1st.add(jan1st);
    List<Interval> missingIntervalsForJan5th2nd1st =
        ValidationManager.computeMissingIntervals(jan5th2nd1st, Duration.standardDays(1));
    Assert.assertEquals(missingIntervalsForJan5th2nd1st.size(), 2);

    // Should also work if the intervals are of different sizes
    Interval jan1stAnd2nd = new Interval(new DateTime(2015, 1, 1, 0, 0, 0), new DateTime(2015, 1, 2, 23, 59, 59));
    ArrayList<Interval> jan1st2nd4th5th = new ArrayList<Interval>();
    jan1st2nd4th5th.add(jan1stAnd2nd);
    jan1st2nd4th5th.add(jan4th);
    jan1st2nd4th5th.add(jan5th);
    List<Interval> missingIntervalsForJan1st2nd4th5th =
        ValidationManager.computeMissingIntervals(jan1st2nd4th5th, Duration.standardDays(1));
    Assert.assertEquals(missingIntervalsForJan1st2nd4th5th.size(), 1);
  }
}
