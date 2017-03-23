package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
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

  void benchmarkMapDoubleSeries() {
    long checksum = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      double[] doubleValues = generateDoubleData(N_ELEMENTS);
      final double delta = r;

      startTimer();
      DoubleSeries s = DoubleSeries.buildFrom(doubleValues);
      DoubleSeries sResult = s.map(new Series.DoubleFunction() {
        @Override
        public double apply(double... values) {
          return values[0] + delta;
        }
      });
      stopTimer();

      checksum ^= checksum(sResult.values());
    }

    logResults("benchmarkMapDoubleSeries", checksum);
  }

  void benchmarkMapDoubleArray() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      double[] doubleValues = generateDoubleData(N_ELEMENTS);
      final double delta = r;

      startTimer();
      double[] results = new double[doubleValues.length];
      for (int i = 0; i < doubleValues.length; i++) {
        results[i] = doubleValues[i] + delta;
      }
      stopTimer();

      checksum ^= checksum(results);
    }

    logResults("benchmarkMapDoubleArray", checksum);
  }

  void benchmarkMapLongSeries() {
    long checksum = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      final long delta = r;

      startTimer();
      LongSeries s = LongSeries.buildFrom(longValues);
      LongSeries sResult = s.map(new Series.LongFunction() {
        @Override
        public long apply(long... values) {
          return values[0] + delta;
        }
      });
      stopTimer();

      checksum ^= checksum(sResult.values());
    }

    logResults("benchmarkMapLongSeries", checksum);
  }

  void benchmarkMapLongArray() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      final long delta = r;

      startTimer();
      long[] results = new long[longValues.length];
      for (int i = 0; i < longValues.length; i++) {
        results[i] = longValues[i] + delta;
      }
      stopTimer();

      checksum ^= checksum(results);
    }

    logResults("benchmarkMapLongArray", checksum);
  }

  void benchmarkMapTwoSeriesExpression() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS_SLOW; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      double[] doubleValues = generateDoubleData(N_ELEMENTS);

      DataFrame df = new DataFrame();
      df.addSeries("long", longValues);
      df.addSeries("double", doubleValues);

      startTimer();
      DoubleSeries res = df.map("long * double");
      stopTimer();

      checksum ^= checksum(res.values());
    }

    logResults("benchmarkMapTwoSeriesExpression", checksum);
  }

  void benchmarkMapTwoSeries() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      double[] doubleValues = generateDoubleData(N_ELEMENTS);

      DataFrame df = new DataFrame();
      df.addSeries("long", longValues);
      df.addSeries("double", doubleValues);

      startTimer();
      DoubleSeries res = df.map(new Series.DoubleFunction() {
        @Override
        public double apply(double... values) {
          return values[0] * values[1];
        }
      }, "long", "double");
      stopTimer();

      checksum ^= checksum(res.values());
    }

    logResults("benchmarkMapTwoSeries", checksum);
  }

  void benchmarkMapTwoArrays() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      double[] doubleValues = generateDoubleData(N_ELEMENTS);
      double[] results = new double[N_ELEMENTS];

      startTimer();
      for(int i=0; i<N_ELEMENTS; i++) {
        results[i] = longValues[i] * doubleValues[i];
      }
      stopTimer();

      checksum ^= checksum(results);
    }

    logResults("benchmarkMapTwoArrays", checksum);
  }

  void benchmarkMapThreeSeries() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      double[] doubleValues = generateDoubleData(N_ELEMENTS);
      long[] otherValues = generateLongData(N_ELEMENTS);

      DataFrame df = new DataFrame();
      df.addSeries("long", longValues);
      df.addSeries("double", doubleValues);
      df.addSeries("other", otherValues);

      startTimer();
      DoubleSeries res = df.map(new Series.DoubleFunction() {
        @Override
        public double apply(double... values) {
          return values[0] * values[1] + values[2];
        }
      }, "long", "double", "other");
      stopTimer();

      checksum ^= checksum(res.values());
    }

    logResults("benchmarkMapThreeSeries", checksum);
  }

  void benchmarkMapThreeArrays() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      double[] doubleValues = generateDoubleData(N_ELEMENTS);
      long[] otherValues = generateLongData(N_ELEMENTS);

      double[] results = new double[N_ELEMENTS];

      startTimer();
      for(int i=0; i<N_ELEMENTS; i++) {
        results[i] = longValues[i] * doubleValues[i] + otherValues[i];
      }
      stopTimer();

      checksum ^= checksum(results);
    }

    logResults("benchmarkMapThreeArrays", checksum);
  }

  void benchmarkMinMaxLongSeries() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);

      LongSeries s = LongSeries.buildFrom(longValues);

      startTimer();
      long min = s.min();
      long max = s.max();
      stopTimer();

      checksum ^= checksum(min, max);
    }

    logResults("benchmarkMinMaxLongSeries", checksum);
  }

  void benchmarkMinMaxLongArray() {
    long checksum = 0;

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

      checksum ^= checksum(min, max);
    }

    logResults("benchmarkMinMaxLongArray", checksum);
  }

  void benchmarkEqualsLongArray() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      long[] otherValues = Arrays.copyOf(longValues, longValues.length);

      startTimer();
      if(!Arrays.equals(longValues, otherValues))
        throw new IllegalStateException("Arrays must be equal");
      stopTimer();

      checksum ^= checksum(longValues);
      checksum ^= checksum(otherValues);
    }

    logResults("benchmarkEqualsLongArray", checksum);
  }

  void benchmarkEqualsLongSeries() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      long[] otherValues = Arrays.copyOf(longValues, longValues.length);

      LongSeries series = LongSeries.buildFrom(longValues);
      LongSeries other = LongSeries.buildFrom(otherValues);

      startTimer();
      if(!series.equals(other))
        throw new IllegalStateException("Series must be equal");
      stopTimer();

      checksum ^= checksum(series.values());
      checksum ^= checksum(other.values());
    }

    logResults("benchmarkEqualsLongSeries", checksum);
  }

  void benchmarkAll() {
    benchmarkMapDoubleSeries();
    benchmarkMapDoubleArray();
    benchmarkMapLongSeries();
    benchmarkMapLongArray();
    benchmarkMinMaxLongSeries();
    benchmarkMinMaxLongArray();
    benchmarkEqualsLongSeries();
    benchmarkEqualsLongArray();
    benchmarkMapTwoSeries();
    benchmarkMapTwoArrays();
    benchmarkMapThreeSeries();
    benchmarkMapThreeArrays();
    //benchmarkMapTwoSeriesExpression();
  }

  void startTimer() {
    this.tStart = System.nanoTime();
  }

  void stopTimer() {
    long tDelta = System.nanoTime() - this.tStart;
    this.times.add(tDelta);
  }

  void logResults(String name, long checksum) {
    Collections.sort(this.times);
    long tMid = this.times.get(this.times.size() / 2);
    long tMin = Collections.min(this.times);
    long tMax = Collections.max(this.times);
    LOG.info("{}: min/mid/max = {}ms {}ms {}ms [xor {}]", name, tMin / 1000000, tMid / 1000000, tMax / 1000000, (checksum >= 0 ? checksum : -checksum) % 1000);
    this.times = new ArrayList<>();
  }

  public static void main(String[] args) throws Exception {
    LOG.info("Press Enter key to start.");
    System.in.read();

    LOG.info("Running DataFrame benchmark ...");
    DataFrameBenchmark b = new DataFrameBenchmark();
    b.benchmarkAll();
    LOG.info("done.");
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

  static long checksum(long... values) {
    long bits = 0;
    for(long v : values) {
      bits ^= v;
    }
    return bits;
  }

  static long checksum(double... values) {
    long bits = 0;
    for(double v : values) {
      bits ^= Double.doubleToLongBits(v);
    }
    return bits;
  }

}
