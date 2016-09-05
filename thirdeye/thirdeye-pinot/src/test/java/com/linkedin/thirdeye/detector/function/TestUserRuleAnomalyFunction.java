package com.linkedin.thirdeye.detector.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

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
  public void getFilteredAndMergedAnomalyResults(List<RawAnomalyResultDTO> anomalyResults,
      int minConsecutiveSize, long bucketMillis, List<Double> baselineValues,
      List<Double> currentValues, double threshold, String baselineProp,
      List<RawAnomalyResultDTO> expectedAnomalyResults) {
    List<RawAnomalyResultDTO> actualAnomalyResults =
        function.getFilteredAndMergedAnomalyResults(anomalyResults, minConsecutiveSize,
            bucketMillis, baselineValues, currentValues, threshold, baselineProp);
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

  private void setAnomalyResultFields(RawAnomalyResultDTO anomalyResult) {
    anomalyResult.setProperties("properties");
    anomalyResult.setWeight(3);
    anomalyResult.setDimensions("dimensions");
  }

  @DataProvider(name = "getFilteredAndMergedAnomalyResultsProvider")
  public Object[][] getFilteredAndMergedAnomalyResultsProvider() {
    RawAnomalyResultDTO anomalyResult1 = new RawAnomalyResultDTO();
    anomalyResult1.setStartTimeUtc(100L);
    anomalyResult1.setEndTimeUtc(120L);
    anomalyResult1.setMessage("message1");
    anomalyResult1.setScore(2);
    setAnomalyResultFields(anomalyResult1);

    RawAnomalyResultDTO anomalyResult2 = new RawAnomalyResultDTO();
    anomalyResult2.setStartTimeUtc(120L);
    anomalyResult2.setEndTimeUtc(140L);
    anomalyResult2.setMessage("message2");
    anomalyResult2.setScore(7);
    setAnomalyResultFields(anomalyResult2);

    RawAnomalyResultDTO anomalyResult3 = new RawAnomalyResultDTO();
    anomalyResult3.setStartTimeUtc(140L);
    anomalyResult3.setEndTimeUtc(160L);
    anomalyResult3.setMessage("message3");
    anomalyResult3.setScore(6);
    setAnomalyResultFields(anomalyResult3);

    RawAnomalyResultDTO anomalyResult4 = new RawAnomalyResultDTO();
    anomalyResult4.setStartTimeUtc(160L);
    anomalyResult4.setEndTimeUtc(180L);
    anomalyResult4.setMessage("message4");
    anomalyResult4.setScore(5);
    setAnomalyResultFields(anomalyResult4);

    RawAnomalyResultDTO anomalyResult5 = new RawAnomalyResultDTO();
    anomalyResult5.setStartTimeUtc(500L);
    anomalyResult5.setEndTimeUtc(520L);
    anomalyResult5.setMessage("message5");
    anomalyResult5.setScore(12);
    setAnomalyResultFields(anomalyResult5);

    RawAnomalyResultDTO anomalyResult6 = new RawAnomalyResultDTO();
    anomalyResult6.setStartTimeUtc(125L);
    anomalyResult6.setEndTimeUtc(145L);
    anomalyResult6.setMessage("message6");
    anomalyResult6.setScore(15);
    setAnomalyResultFields(anomalyResult6);

    RawAnomalyResultDTO anomalyResult7 = new RawAnomalyResultDTO();
    anomalyResult7.setStartTimeUtc(10L);
    anomalyResult7.setEndTimeUtc(30L);
    anomalyResult7.setMessage("message7");
    anomalyResult7.setScore(3);
    setAnomalyResultFields(anomalyResult7);

    RawAnomalyResultDTO anomalyResult8 = new RawAnomalyResultDTO();
    anomalyResult8.setStartTimeUtc(520L);
    anomalyResult8.setEndTimeUtc(540L);
    anomalyResult8.setMessage("message8");
    anomalyResult8.setScore(14);
    setAnomalyResultFields(anomalyResult8);

    RawAnomalyResultDTO anomalyResult9 = new RawAnomalyResultDTO();
    anomalyResult9.setStartTimeUtc(540L);
    anomalyResult9.setEndTimeUtc(560L);
    anomalyResult9.setMessage("message9");
    anomalyResult9.setScore(16);
    setAnomalyResultFields(anomalyResult9);

    List<RawAnomalyResultDTO> anomalyResults1 = new ArrayList<>();
    anomalyResults1.add(anomalyResult1);
    List<Double> baselineValues1 = Arrays.asList(10.0);
    List<Double> currentValues1 = Arrays.asList(5.0);
    double threshold1 = -0.1;
    String baselineProp1 = "w/w";

    List<RawAnomalyResultDTO> anomalyResults2 = new ArrayList<>();
    anomalyResults2.add(anomalyResult1);
    anomalyResults2.add(anomalyResult2);
    anomalyResults2.add(anomalyResult3);
    List<Double> baselineValues2 = Arrays.asList(10.0, 20.0, 30.0);
    List<Double> currentValues2 = Arrays.asList(5.0, 10.0, 20.0);
    double threshold2 = -0.1;
    String baselineProp2 = "w/w";

    List<RawAnomalyResultDTO> anomalyResults3 = new ArrayList<>();
    anomalyResults3.add(anomalyResult1);
    anomalyResults3.add(anomalyResult2);
    anomalyResults3.add(anomalyResult3);
    anomalyResults3.add(anomalyResult4);
    anomalyResults3.add(anomalyResult5); // Breaks the consecutive rule
    List<Double> baselineValues3 = Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0);
    List<Double> currentValues3 = Arrays.asList(5.0, 10.0, 20.0, 30.0, 40.0);
    double threshold3 = -0.15;
    String baselineProp3 = "w/2w";

    List<RawAnomalyResultDTO> anomalyResults4 = new ArrayList<>();
    anomalyResults4.add(anomalyResult1);
    anomalyResults4.add(anomalyResult2);
    anomalyResults4.add(anomalyResult6); // Breaks the 2 consecutive rule
    anomalyResults4.add(anomalyResult3);
    anomalyResults4.add(anomalyResult4);
    anomalyResults4.add(anomalyResult5); // Breaks the 2 consecutive rule
    List<Double> baselineValues4 = Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0, 60.0);
    List<Double> currentValues4 = Arrays.asList(5.0, 10.0, 20.0, 30.0, 40.0, 50.0);
    double threshold4 = -0.2;
    String baselineProp4 = "w/2w";

    List<RawAnomalyResultDTO> anomalyResults5 = new ArrayList<>();
    anomalyResults5.add(anomalyResult7);
    anomalyResults5.add(anomalyResult1); // Start of 3 consecutive result
    anomalyResults5.add(anomalyResult2);
    anomalyResults5.add(anomalyResult3);
    anomalyResults5.add(anomalyResult5); // Breaks the consecutive rule
    List<Double> baselineValues5 = Arrays.asList(20.0, 30.0, 40.0, 50.0, 60.0);
    List<Double> currentValues5 = Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0);
    double threshold5 = -0.1;
    String baselineProp5 = "w/w";

    List<RawAnomalyResultDTO> anomalyResults6 = new ArrayList<>();
    anomalyResults6.add(anomalyResult1); // Start of min 3 consecutive result
    anomalyResults6.add(anomalyResult2);
    anomalyResults6.add(anomalyResult3);
    anomalyResults6.add(anomalyResult4);
    anomalyResults6.add(anomalyResult5); // Breaks the consecutive rule. Start of min 3 consecutive
                                         // result.
    anomalyResults6.add(anomalyResult8);
    anomalyResults6.add(anomalyResult9);
    List<Double> baselineValues6 = Arrays.asList(20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0);
    List<Double> currentValues6 = Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0);
    double threshold6 = -0.05;
    String baselineProp6 = "w/2w";

    RawAnomalyResultDTO mergedAnomalyResult1 = new RawAnomalyResultDTO();
    mergedAnomalyResult1.setStartTimeUtc(100L);
    mergedAnomalyResult1.setEndTimeUtc(160L);
    mergedAnomalyResult1.setMessage("threshold=-10%, w/w values: -50%,-50%,-33.33% (20.0 / 30.0)");
    mergedAnomalyResult1.setScore(5);
    setAnomalyResultFields(mergedAnomalyResult1);

    RawAnomalyResultDTO mergedAnomalyResult2 = new RawAnomalyResultDTO();
    mergedAnomalyResult2.setStartTimeUtc(100L);
    mergedAnomalyResult2.setEndTimeUtc(180L);
    mergedAnomalyResult2
        .setMessage("threshold=-15%, w/2w values: -50%,-50%,-33.33%,-25% (30.0 / 40.0)");
    mergedAnomalyResult2.setScore(5);
    setAnomalyResultFields(mergedAnomalyResult2);

    RawAnomalyResultDTO mergedAnomalyResult3 = new RawAnomalyResultDTO();
    mergedAnomalyResult3.setStartTimeUtc(100L);
    mergedAnomalyResult3.setEndTimeUtc(140L);
    mergedAnomalyResult3.setMessage("threshold=-20%, w/2w values: -50%,-50% (10.0 / 20.0)");
    mergedAnomalyResult3.setScore(4.5);
    setAnomalyResultFields(mergedAnomalyResult3);

    RawAnomalyResultDTO mergedAnomalyResult4 = new RawAnomalyResultDTO();
    mergedAnomalyResult4.setStartTimeUtc(140L);
    mergedAnomalyResult4.setEndTimeUtc(180L);
    mergedAnomalyResult4.setMessage("threshold=-20%, w/2w values: -25%,-20% (40.0 / 50.0)");
    mergedAnomalyResult4.setScore(5.5);
    setAnomalyResultFields(mergedAnomalyResult4);

    RawAnomalyResultDTO mergedAnomalyResult5 = new RawAnomalyResultDTO();
    mergedAnomalyResult5.setStartTimeUtc(100L);
    mergedAnomalyResult5.setEndTimeUtc(160L);
    mergedAnomalyResult5.setMessage("threshold=-10%, w/w values: -33.33%,-25%,-20% (40.0 / 50.0)");
    mergedAnomalyResult5.setScore(5);
    setAnomalyResultFields(mergedAnomalyResult5);

    RawAnomalyResultDTO mergedAnomalyResult6 = new RawAnomalyResultDTO();
    mergedAnomalyResult6.setStartTimeUtc(100L);
    mergedAnomalyResult6.setEndTimeUtc(180L);
    mergedAnomalyResult6
        .setMessage("threshold=-5%, w/2w values: -50%,-33.33%,-25%,-20% (40.0 / 50.0)");
    mergedAnomalyResult6.setScore(5);
    setAnomalyResultFields(mergedAnomalyResult6);

    RawAnomalyResultDTO mergedAnomalyResult7 = new RawAnomalyResultDTO();
    mergedAnomalyResult7.setStartTimeUtc(500L);
    mergedAnomalyResult7.setEndTimeUtc(560L);
    mergedAnomalyResult7
        .setMessage("threshold=-5%, w/2w values: -16.67%,-14.29%,-12.5% (70.0 / 80.0)");
    mergedAnomalyResult7.setScore(14);
    setAnomalyResultFields(mergedAnomalyResult7);

    List<RawAnomalyResultDTO> filteredAndMergedAnomalyResults1 = new ArrayList<>();
    filteredAndMergedAnomalyResults1.add(mergedAnomalyResult1);

    List<RawAnomalyResultDTO> filteredAndMergedAnomalyResults2 = new ArrayList<>();
    filteredAndMergedAnomalyResults2.add(mergedAnomalyResult2);

    List<RawAnomalyResultDTO> filteredAndMergedAnomalyResults3 = new ArrayList<>();
    filteredAndMergedAnomalyResults3.add(mergedAnomalyResult3);
    filteredAndMergedAnomalyResults3.add(mergedAnomalyResult4);

    List<RawAnomalyResultDTO> filteredAndMergedAnomalyResults4 = new ArrayList<>();
    filteredAndMergedAnomalyResults4.add(mergedAnomalyResult5);

    List<RawAnomalyResultDTO> filteredAndMergedAnomalyResults5 = new ArrayList<>();
    filteredAndMergedAnomalyResults5.add(mergedAnomalyResult6);
    filteredAndMergedAnomalyResults5.add(mergedAnomalyResult7);

    return new Object[][] {
        {
            anomalyResults1, 1, 20, baselineValues1, currentValues1, threshold1, baselineProp1,
            anomalyResults1
        },

        {
            anomalyResults1, 2, 20, baselineValues1, currentValues1, threshold1, baselineProp1,
            new ArrayList<>()
        },

        {
            anomalyResults2, 3, 20, baselineValues2, currentValues2, threshold2, baselineProp2,
            filteredAndMergedAnomalyResults1
        },

        {
            anomalyResults3, 3, 20, baselineValues3, currentValues3, threshold3, baselineProp3,
            filteredAndMergedAnomalyResults2
        },

        {
            anomalyResults3, 4, 20, baselineValues3, currentValues3, threshold3, baselineProp3,
            filteredAndMergedAnomalyResults2
        },

        {
            anomalyResults4, 2, 20, baselineValues4, currentValues4, threshold4, baselineProp4,
            filteredAndMergedAnomalyResults3
        },

        {
            anomalyResults4, 3, 20, baselineValues4, currentValues4, threshold4, baselineProp4,
            new ArrayList<>()
        },

        {
            anomalyResults5, 3, 20, baselineValues5, currentValues5, threshold5, baselineProp5,
            filteredAndMergedAnomalyResults4
        },

        {
            anomalyResults6, 3, 20, baselineValues6, currentValues6, threshold6, baselineProp6,
            filteredAndMergedAnomalyResults5
        }
    };
  }
}
