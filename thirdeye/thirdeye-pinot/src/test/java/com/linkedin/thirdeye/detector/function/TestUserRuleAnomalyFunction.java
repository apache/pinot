package com.linkedin.thirdeye.detector.function;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.detector.api.AnomalyResult;

public class TestUserRuleAnomalyFunction {
  private static final UserRuleAnomalyFunction function = new UserRuleAnomalyFunction();

  @Test
  public void analyze() {
    // TODO implement full test
  }

  @Test(dataProvider = "filterTimeWindowSetProvider")
  public void filterTimeWindowSet(Set<Long> timeWindowSet, long windowStart, long windowEnd,
      Set<Long> expected) {
    SortedSet<Long> actual = function.filterTimeWindowSet(timeWindowSet, windowStart, windowEnd);
    Assert.assertEquals(actual, expected);
  }

  @Test(dataProvider = "getBaselineMillisProvider")
  public void getBaselineMillis(String baselineProp, long expected) {
    long actual = function.getBaselineMillis(baselineProp);
    Assert.assertEquals(actual, expected);
  }

  @Test(dataProvider = "isAnomalyProvider")
  public void isAnomaly(double currentValue, double baselineValue, double changeThreshold,
      boolean expected) {
    boolean actual = function.isAnomaly(currentValue, baselineValue, changeThreshold);
    Assert.assertEquals(actual, expected);
  }

  @Test(dataProvider = "getFilteredAndMergedAnomalyResultsProvider")
  public void getFilteredAndMergedAnomalyResults(List<AnomalyResult> anomalyResults,
      int minConsecutiveSize, long bucketMillis, List<AnomalyResult> expectedAnomalyResults) {
    List<AnomalyResult> actualAnomalyResults =
        function.getFilteredAndMergedAnomalyResults(anomalyResults, minConsecutiveSize,
            bucketMillis);
    Assert.assertEquals(actualAnomalyResults, expectedAnomalyResults);
  }

  @DataProvider(name = "filterTimeWindowSetProvider")
  public Object[][] filterTimeWindowSetProvider() {
    // test for min < windowStart, min > windowStart, max < windowEnd, max > windowEnd

    // vars in numerical order, all you need to know is a < b < c < d
    long a = 1, b = 5, c = 7, d = 10;

    // a < b < c < d
    return new Object[][] {
        // max < windowEnd
        // min < windowStart
        new Object[] {
            range(a, c), b, d, range(b, c)
        },
        // min > windowStart
        new Object[] {
            range(b, c), a, d, range(b, c)
        },
        // max > windowEnd
        new Object[] {
            range(a, d), b, c, range(b, c)
        },
        // min > windowStart
        new Object[] {
            range(b, d), a, c, range(b, c)
        }
    };
  }

  private Set<Long> range(long start, long end) {
    int expectedCapacity = (int) (end - start);
    HashSet<Long> result = new HashSet<Long>(expectedCapacity);
    for (long current = start; current < end; current++) {
      result.add(current);
    }
    return result;
  }

  @DataProvider(name = "getBaselineMillisProvider")
  public Object[][] getBaselineMillisProvider() {
    return new Object[][] {
        new Object[] {
            "w/w", TimeUnit.DAYS.toMillis(7)
        }, new Object[] {
            "w/2w", TimeUnit.DAYS.toMillis(14)
        }, new Object[] {
            "w/3w", TimeUnit.DAYS.toMillis(21)
        }
    };
  }

  @DataProvider(name = "isAnomalyProvider")
  public Object[][] isAnomalyProvider() {
    return new Object[][] {
        // increases
        new Object[] {
            150, 100, 0.5, false
        }, new Object[] {
            150, 100, 0.499999, true
        },
        // decreases
        new Object[] {
            100, 50, -0.5, false
        }, new Object[] {
            100, 50, -0.499, false
        },
        // 0 and negative baseline values return false
        new Object[] {
            100, 0, 0.0, false
        },
    };
  }

  private void setAnomalyResultFields(AnomalyResult anomalyResult) {
    anomalyResult.setCollection("collection");
    anomalyResult.setMetric("metric");
    anomalyResult.setDimensions("dimensions");
    anomalyResult.setFunctionId(1);
    anomalyResult.setProperties("properties");
    anomalyResult.setWeight(3);
    anomalyResult.setFilters("a=b;c=d");
  }

  @DataProvider(name = "getFilteredAndMergedAnomalyResultsProvider")
  public Object[][] getFilteredAndMergedAnomalyResultsProvider() {
    AnomalyResult anomalyResult1 = new AnomalyResult();
    anomalyResult1.setStartTimeUtc(100L);
    anomalyResult1.setEndTimeUtc(120L);
    anomalyResult1.setMessage("message1");
    anomalyResult1.setScore(2);
    setAnomalyResultFields(anomalyResult1);

    AnomalyResult anomalyResult2 = new AnomalyResult();
    anomalyResult2.setStartTimeUtc(120L);
    anomalyResult2.setEndTimeUtc(140L);
    anomalyResult2.setMessage("message2");
    anomalyResult2.setScore(7);
    setAnomalyResultFields(anomalyResult2);

    AnomalyResult anomalyResult3 = new AnomalyResult();
    anomalyResult3.setStartTimeUtc(140L);
    anomalyResult3.setEndTimeUtc(160L);
    anomalyResult3.setMessage("message3");
    anomalyResult3.setScore(6);
    setAnomalyResultFields(anomalyResult3);

    AnomalyResult anomalyResult4 = new AnomalyResult();
    anomalyResult4.setStartTimeUtc(160L);
    anomalyResult4.setEndTimeUtc(180L);
    anomalyResult4.setMessage("message4");
    anomalyResult4.setScore(5);
    setAnomalyResultFields(anomalyResult4);

    AnomalyResult anomalyResult5 = new AnomalyResult();
    anomalyResult5.setStartTimeUtc(500L);
    anomalyResult5.setEndTimeUtc(520L);
    anomalyResult5.setMessage("message5");
    anomalyResult5.setScore(12);
    setAnomalyResultFields(anomalyResult5);

    AnomalyResult anomalyResult6 = new AnomalyResult();
    anomalyResult6.setStartTimeUtc(125L);
    anomalyResult6.setEndTimeUtc(145L);
    anomalyResult6.setMessage("message6");
    anomalyResult6.setScore(15);
    setAnomalyResultFields(anomalyResult6);

    AnomalyResult anomalyResult7 = new AnomalyResult();
    anomalyResult7.setStartTimeUtc(10L);
    anomalyResult7.setEndTimeUtc(30L);
    anomalyResult7.setMessage("message7");
    anomalyResult7.setScore(3);
    setAnomalyResultFields(anomalyResult7);

    AnomalyResult anomalyResult8 = new AnomalyResult();
    anomalyResult8.setStartTimeUtc(520L);
    anomalyResult8.setEndTimeUtc(540L);
    anomalyResult8.setMessage("message8");
    anomalyResult8.setScore(14);
    setAnomalyResultFields(anomalyResult8);

    AnomalyResult anomalyResult9 = new AnomalyResult();
    anomalyResult9.setStartTimeUtc(540L);
    anomalyResult9.setEndTimeUtc(560L);
    anomalyResult9.setMessage("message9");
    anomalyResult9.setScore(16);
    setAnomalyResultFields(anomalyResult9);

    List<AnomalyResult> anomalyResults1 = new ArrayList<>();
    anomalyResults1.add(anomalyResult1);

    List<AnomalyResult> anomalyResults2 = new ArrayList<>();
    anomalyResults2.add(anomalyResult1);
    anomalyResults2.add(anomalyResult2);
    anomalyResults2.add(anomalyResult3);

    List<AnomalyResult> anomalyResults3 = new ArrayList<>();
    anomalyResults3.add(anomalyResult1);
    anomalyResults3.add(anomalyResult2);
    anomalyResults3.add(anomalyResult3);
    anomalyResults3.add(anomalyResult4);
    anomalyResults3.add(anomalyResult5); // Breaks the consecutive rule

    List<AnomalyResult> anomalyResults4 = new ArrayList<>();
    anomalyResults4.add(anomalyResult1);
    anomalyResults4.add(anomalyResult2);
    anomalyResults4.add(anomalyResult6); // Breaks the 2 consecutive rule
    anomalyResults4.add(anomalyResult3);
    anomalyResults4.add(anomalyResult4);
    anomalyResults4.add(anomalyResult5); // Breaks the 2 consecutive rule

    List<AnomalyResult> anomalyResults5 = new ArrayList<>();
    anomalyResults5.add(anomalyResult7);
    anomalyResults5.add(anomalyResult1); // Start of 3 consecutive result
    anomalyResults5.add(anomalyResult2);
    anomalyResults5.add(anomalyResult3);
    anomalyResults5.add(anomalyResult5); // Breaks the consecutive rule

    List<AnomalyResult> anomalyResults6 = new ArrayList<>();
    anomalyResults6.add(anomalyResult1); // Start of min 3 consecutive result
    anomalyResults6.add(anomalyResult2);
    anomalyResults6.add(anomalyResult3);
    anomalyResults6.add(anomalyResult4);
    anomalyResults6.add(anomalyResult5); // Breaks the consecutive rule. Start of min 3 consecutive
                                         // result.
    anomalyResults6.add(anomalyResult8);
    anomalyResults6.add(anomalyResult9);

    AnomalyResult mergedAnomalyResult1 = new AnomalyResult();
    mergedAnomalyResult1.setStartTimeUtc(100L);
    mergedAnomalyResult1.setEndTimeUtc(160L);
    mergedAnomalyResult1.setMessage("message1;message2;message3");
    mergedAnomalyResult1.setScore(5);
    setAnomalyResultFields(mergedAnomalyResult1);

    AnomalyResult mergedAnomalyResult2 = new AnomalyResult();
    mergedAnomalyResult2.setStartTimeUtc(100L);
    mergedAnomalyResult2.setEndTimeUtc(180L);
    mergedAnomalyResult2.setMessage("message1;message2;message3;message4");
    mergedAnomalyResult2.setScore(5);
    setAnomalyResultFields(mergedAnomalyResult2);

    AnomalyResult mergedAnomalyResult3 = new AnomalyResult();
    mergedAnomalyResult3.setStartTimeUtc(100L);
    mergedAnomalyResult3.setEndTimeUtc(140L);
    mergedAnomalyResult3.setMessage("message1;message2");
    mergedAnomalyResult3.setScore(4.5);
    setAnomalyResultFields(mergedAnomalyResult3);

    AnomalyResult mergedAnomalyResult4 = new AnomalyResult();
    mergedAnomalyResult4.setStartTimeUtc(140L);
    mergedAnomalyResult4.setEndTimeUtc(180L);
    mergedAnomalyResult4.setMessage("message3;message4");
    mergedAnomalyResult4.setScore(5.5);
    setAnomalyResultFields(mergedAnomalyResult4);

    AnomalyResult mergedAnomalyResult5 = new AnomalyResult();
    mergedAnomalyResult5.setStartTimeUtc(100L);
    mergedAnomalyResult5.setEndTimeUtc(160L);
    mergedAnomalyResult5.setMessage("message1;message2;message3");
    mergedAnomalyResult5.setScore(5);
    setAnomalyResultFields(mergedAnomalyResult5);

    AnomalyResult mergedAnomalyResult6 = new AnomalyResult();
    mergedAnomalyResult6.setStartTimeUtc(500L);
    mergedAnomalyResult6.setEndTimeUtc(560L);
    mergedAnomalyResult6.setMessage("message5;message8;message9");
    mergedAnomalyResult6.setScore(14);
    setAnomalyResultFields(mergedAnomalyResult6);

    List<AnomalyResult> filteredAndMergedAnomalyResults1 = new ArrayList<>();
    filteredAndMergedAnomalyResults1.add(mergedAnomalyResult1);

    List<AnomalyResult> filteredAndMergedAnomalyResults2 = new ArrayList<>();
    filteredAndMergedAnomalyResults2.add(mergedAnomalyResult2);

    List<AnomalyResult> filteredAndMergedAnomalyResults3 = new ArrayList<>();
    filteredAndMergedAnomalyResults3.add(mergedAnomalyResult3);
    filteredAndMergedAnomalyResults3.add(mergedAnomalyResult4);

    List<AnomalyResult> filteredAndMergedAnomalyResults4 = new ArrayList<>();
    filteredAndMergedAnomalyResults4.add(mergedAnomalyResult5);

    List<AnomalyResult> filteredAndMergedAnomalyResults5 = new ArrayList<>();
    filteredAndMergedAnomalyResults5.add(mergedAnomalyResult2);
    filteredAndMergedAnomalyResults5.add(mergedAnomalyResult6);

    return new Object[][] {
        {
            anomalyResults1, 1, 20, anomalyResults1
        },

        {
            anomalyResults1, 2, 20, new ArrayList<>()
        },

        {
            anomalyResults2, 3, 20, filteredAndMergedAnomalyResults1
        },

        {
            anomalyResults3, 3, 20, filteredAndMergedAnomalyResults2
        },

        {
            anomalyResults3, 4, 20, filteredAndMergedAnomalyResults2
        },

        {
            anomalyResults4, 2, 20, filteredAndMergedAnomalyResults3
        },

        {
            anomalyResults4, 3, 20, new ArrayList<>()
        },

        {
            anomalyResults5, 3, 20, filteredAndMergedAnomalyResults4
        },

        {
            anomalyResults6, 3, 20, filteredAndMergedAnomalyResults5
        }
    };
  }
}
