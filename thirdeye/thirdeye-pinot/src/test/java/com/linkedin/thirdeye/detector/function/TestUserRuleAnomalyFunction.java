package com.linkedin.thirdeye.detector.function;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
}
