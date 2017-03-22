package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataFrameBenchmark {
  // TODO: validate benchmarking method - Dead Code Elimination, etc. may be playing tricks on us.

  private static final Logger LOG = LoggerFactory.getLogger(DataFrameBenchmark.class);

  private static final int N_ROUNDS = 15;
  private static final int N_ROUNDS_SLOW = 3;
  private static final int N_ELEMENTS = 10_000_000;

  long tStart;
  List<Long> times = new ArrayList<>();

  public void testDoubleMapSeries() {
    for (int r = 0; r < N_ROUNDS; r++) {
      double[] doubleValues = generateDoubleData(N_ELEMENTS);
      final double delta = r;

      // data frame
      startTimer();
      DoubleSeries s = DoubleSeries.buildFrom(doubleValues);
      DoubleSeries sResult = s.map(new Series.DoubleFunction() {
        @Override
        public double apply(double... values) {
          return values[0] + delta;
        }
      });
      stopTimer();
      Assert.assertEquals(N_ELEMENTS, sResult.size());
    }

    logResults("testDoubleMapSeries");
  }

  public void testDoubleMapArray() {
    for(int r=0; r<N_ROUNDS; r++) {
      double[] doubleValues = generateDoubleData(N_ELEMENTS);
      final double delta = r;

      // array
      startTimer();
      double[] results = new double[doubleValues.length];
      for (int i = 0; i < doubleValues.length; i++) {
        results[i] = doubleValues[i] + delta;
      }
      stopTimer();
      Assert.assertEquals(N_ELEMENTS, results.length);
    }

    logResults("testDoubleMapArray");
  }

  public void testLongMapSeries() {
    for (int r = 0; r < N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      final long delta = r;

      // data frame
      startTimer();
      LongSeries s = LongSeries.buildFrom(longValues);
      LongSeries sResult = s.map(new Series.LongFunction() {
        @Override
        public long apply(long... values) {
          return values[0] + delta;
        }
      });
      Assert.assertEquals(N_ELEMENTS, sResult.size());
      stopTimer();
    }

    logResults("testLongMapSeries");
  }

  public void testLongMapArray() {
    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      final long delta = r;

      // array
      startTimer();
      long[] results = new long[longValues.length];
      for (int i = 0; i < longValues.length; i++) {
        results[i] = longValues[i] + delta;
      }
      stopTimer();
      Assert.assertEquals(N_ELEMENTS, results.length);
    }

    logResults("testLongMapArray");
  }

  public void testDataFrameMapExpression() {
    for(int r=0; r<N_ROUNDS_SLOW; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      double[] doubleValues = generateDoubleData(N_ELEMENTS);

      DataFrame df = new DataFrame();
      df.addSeries("long", longValues);
      df.addSeries("double", doubleValues);

      startTimer();
      Series res = df.map("long * double");
      stopTimer();

      Assert.assertEquals(N_ELEMENTS, res.size());
    }

    logResults("testDataFrameMapExpression");
  }

  public void testDataFrameMapDouble() {
    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      double[] doubleValues = generateDoubleData(N_ELEMENTS);

      DataFrame df = new DataFrame();
      df.addSeries("long", longValues);
      df.addSeries("double", doubleValues);

      startTimer();
      Series res = df.map(new Series.DoubleFunction() {
        @Override
        public double apply(double... values) {
          return values[0] * values[1];
        }
      }, "long", "double");
      stopTimer();

      Assert.assertEquals(N_ELEMENTS, res.size());
    }

    logResults("testDataFrameMapDouble");
  }

  public void testLongMinMaxSeries() {
    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);

      LongSeries s = LongSeries.buildFrom(longValues);

      startTimer();
      long min = s.min();
      long max = s.max();
      stopTimer();

      Assert.assertTrue(min != max);
    }

    logResults("testLongMinMaxSeries");
  }

  public void testLongMinMaxArray() {
    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);

      startTimer();
      long min = longValues[0];
      long max = longValues[0];

      for (long v : longValues) {
        if (min > v) min = v;
        if (max < v) max = v;
      }
      stopTimer();

      Assert.assertTrue(min != max);
    }

    logResults("testLongMinMaxArray");
  }

  public void testAll() {
    testDoubleMapSeries();
    testDoubleMapArray();
    testLongMapSeries();
    testLongMapArray();
    testDataFrameMapExpression();
    testDataFrameMapDouble();
    testLongMinMaxSeries();
    testLongMinMaxArray();
  }

  void startTimer() {
    this.tStart = System.nanoTime();
  }

  void stopTimer() {
    long tDelta = System.nanoTime() - this.tStart;
    this.times.add(tDelta);
  }

  void logResults(String name) {
    Collections.sort(this.times);
    long tMid = this.times.get(this.times.size() / 2);
    long tMin = Collections.min(this.times);
    long tMax = Collections.max(this.times);
    LOG.info("{}: min/mid/max = {}ms {}ms {}ms", name, tMin / 1000 / 1000.0, tMid / 1000 / 1000.0, tMax / 1000 / 1000.0);
    this.times = new ArrayList<>();
  }

  public static void main(String[] args) {
    DataFrameBenchmark b = new DataFrameBenchmark();
    b.testAll();
  }

  static double[] generateDoubleData(int n) {
    Random r = new Random();
    r.setSeed(System.nanoTime());
    double[] values = new double[n];
    for(int i=0; i<n; i++) {
      values[i] = r.nextDouble();
    }
    return values;
  }

  static long[] generateLongData(int n) {
    Random r = new Random();
    r.setSeed(System.nanoTime());
    long[] values = new long[n];
    for(int i=0; i<n; i++) {
      values[i] = r.nextLong();
    }
    return values;
  }

}
