package com.linkedin.thirdeye.anomaly.lib.scanstatistics;

import java.util.HashSet;

import org.apache.commons.math3.util.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.anomaly.lib.scanstatistics.ScanIntervalIterator;

/**
 *
 */
public class ScanIntervalIteratorTest {

  @Test
  public void simpleTest() {
    testSingle(1, 100, 1, 1000);
    testSingle(1, 100, 6, 50);
    testSingle(1, 100, 1, 5);
    testSingle(1, 100, 98, 99);
    testSingle(1, 100, 3, 3);
    testSingle(-100, 100, 1, 200);
  }

  private void testSingle(int min, int max, int minWidth, int maxWidth) {
    ScanIntervalIterator it = new ScanIntervalIterator(min, max, minWidth, maxWidth, 1);
    HashSet<Range<Integer>> found = new HashSet<>();
    while(it.hasNext()) {
      Range<Integer> curr = it.next();
      found.add(curr);
    }

    HashSet<Range<Integer>> foundByBruteForce = bruteForce(min, max, minWidth, maxWidth);

    Assert.assertEquals(found.size(), foundByBruteForce.size());
    Assert.assertEquals(found, foundByBruteForce);
  }

  private HashSet<Range<Integer>> bruteForce(int min, int max, int minWidth, int maxWidth) {
    HashSet<Range<Integer>> found = new HashSet<>();
    for (int i = min; i <= max; i++) {
      for (int j = min; j < i; j++) {
        int width = i - j;
        if (width >= minWidth && width <= maxWidth) {
          found.add(Range.closedOpen(j,i));
        }
      }
    }
    return found;
  }

}
