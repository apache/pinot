package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataFrameBenchmark {
  // TODO: validate benchmarking method - Dead Code Elimination, etc. may be playing tricks on us.

  private static final Logger LOG = LoggerFactory.getLogger(DataFrameBenchmark.class);

  private static final int N_ROUNDS = 15;
  private static final int N_ROUNDS_SLOW = 3;
  private static final int N_ELEMENTS = 10_000_000;

  private static final String[] SERIES_NAMES = new String[] { "task", "min", "mid", "max", "checksum", "samples" };

  long tStart;
  List<Long> times = new ArrayList<>();
  DataFrame.Builder results = DataFrame.builder(SERIES_NAMES);

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

  void benchmarkMapDoubleSeriesOperation() {
    long checksum = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      double[] doubleValues = generateDoubleData(N_ELEMENTS);
      final double delta = r;

      startTimer();
      DoubleSeries s = DoubleSeries.buildFrom(doubleValues);
      DoubleSeries sResult = s.add(delta);
      stopTimer();

      checksum ^= checksum(sResult.values());
    }

    logResults("benchmarkMapDoubleSeriesOperation", checksum);
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

  void benchmarkMapLongSeriesOperation() {
    long checksum = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      final long delta = r;

      startTimer();
      LongSeries s = LongSeries.buildFrom(longValues);
      LongSeries sResult = s.add(delta);
      stopTimer();

      checksum ^= checksum(sResult.values());
    }

    logResults("benchmarkMapLongSeriesOperation", checksum);
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

  void benchmarkMapTwoSeriesOperation() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      double[] doubleValues = generateDoubleData(N_ELEMENTS);

      DataFrame df = new DataFrame();
      df.addSeries("long", longValues);
      df.addSeries("double", doubleValues);

      startTimer();
      LongSeries l = df.getLongs("long");
      DoubleSeries d = df.getDoubles("double");
      DoubleSeries res = d.multiply(l);
      stopTimer();

      checksum ^= checksum(res.values());
    }

    logResults("benchmarkMapTwoSeriesOperation", checksum);
  }

  void benchmarkMapTwoArrays() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      double[] doubleValues = generateDoubleData(N_ELEMENTS);

      startTimer();
      double[] results = new double[N_ELEMENTS];
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

      startTimer();
      double[] results = new double[N_ELEMENTS];
      for(int i=0; i<N_ELEMENTS; i++) {
        results[i] = longValues[i] * doubleValues[i] + otherValues[i];
      }
      stopTimer();

      checksum ^= checksum(results);
    }

    logResults("benchmarkMapThreeArrays", checksum);
  }

  void benchmarkMapFourSeriesGeneric() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      double[] doubleValues = generateDoubleData(N_ELEMENTS);
      long[] otherValues = generateLongData(N_ELEMENTS);
      double[] anotherValues = generateDoubleData(N_ELEMENTS);

      DataFrame df = new DataFrame();
      df.addSeries("long", longValues);
      df.addSeries("double", doubleValues);
      df.addSeries("other", otherValues);
      df.addSeries("another", anotherValues);

      startTimer();
      DoubleSeries res = df.map(new Series.DoubleFunction() {
        @Override
        public double apply(double... values) {
          return values[0] * values[1] + values[2] / values[3];
        }
      }, "long", "double", "other", "another");
      stopTimer();

      checksum ^= checksum(res.values());
    }

    logResults("benchmarkMapFourSeriesGeneric", checksum);
  }

  void benchmarkMapFourArrays() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      double[] doubleValues = generateDoubleData(N_ELEMENTS);
      long[] otherValues = generateLongData(N_ELEMENTS);
      double[] anotherValues = generateDoubleData(N_ELEMENTS);

      startTimer();
      double[] results = new double[N_ELEMENTS];
      for(int i=0; i<N_ELEMENTS; i++) {
        results[i] = longValues[i] * doubleValues[i] + otherValues[i] / anotherValues[i];
      }
      stopTimer();

      checksum ^= checksum(results);
    }

    logResults("benchmarkMapFourArrays", checksum);
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

  void benchmarkEqualsLongSeriesOperation() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      long[] otherValues = Arrays.copyOf(longValues, longValues.length);
      LongSeries series = LongSeries.buildFrom(longValues);
      LongSeries other = LongSeries.buildFrom(otherValues);

      startTimer();
      BooleanSeries res = series.eq(other);
      stopTimer();

      if(res.hasFalse())
        throw new IllegalStateException("Series must be equal");

      checksum ^= checksum(series.values());
      checksum ^= checksum(other.values());
    }

    logResults("benchmarkEqualsLongSeriesOperation", checksum);
  }

  void benchmarkSortLongArray() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS_SLOW; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);

      startTimer();
      Arrays.sort(longValues);
      stopTimer();

      checksum ^= checksum(longValues);
    }

    logResults("benchmarkSortLongArray", checksum);
  }

  void benchmarkSortLongSeries() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS_SLOW; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      LongSeries series = LongSeries.buildFrom(longValues);

      startTimer();
      LongSeries out = series.sorted();
      stopTimer();

      checksum ^= checksum(out.values());
    }

    logResults("benchmarkSortLongSeries", checksum);
  }

  void benchmarkUniqueLongArrayWithObjects() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS_SLOW; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);

      startTimer();
      Set<Long> set = new HashSet<>();
      for(long l : longValues)
        set.add(l);
      long[] out = ArrayUtils.toPrimitive(set.toArray(new Long[set.size()]));
      stopTimer();

      checksum ^= checksum(out);
    }

    logResults("benchmarkUniqueLongArrayWithObjects", checksum);
  }

  void benchmarkUniqueLongSeries() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS_SLOW; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      LongSeries series = LongSeries.buildFrom(longValues);

      startTimer();
      LongSeries out = series.unique();
      stopTimer();

      checksum ^= checksum(out.values());
    }

    logResults("benchmarkUniqueLongSeries", checksum);
  }

  void benchmarkShiftLongArray() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);

      startTimer();
      long[] values = new long[N_ELEMENTS];
      System.arraycopy(longValues, 0, values, N_ELEMENTS / 2, N_ELEMENTS / 2);
      Arrays.fill(values, 0, N_ELEMENTS / 2, Long.MIN_VALUE);
      stopTimer();

      checksum ^= checksum(values);
    }

    logResults("benchmarkShiftLongArray", checksum);
  }

  void benchmarkShiftLongSeries() {
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      LongSeries series = LongSeries.buildFrom(longValues);

      startTimer();
      LongSeries out = series.shift(N_ELEMENTS / 2);
      stopTimer();

      checksum ^= checksum(out.values());
    }

    logResults("benchmarkShiftLongSeries", checksum);
  }

  void benchmarkAll() {
    benchmarkMinMaxLongSeries();
    benchmarkMinMaxLongArray();
    benchmarkEqualsLongSeries();
    benchmarkEqualsLongSeriesOperation();
    benchmarkEqualsLongArray();
    benchmarkShiftLongSeries();
    benchmarkShiftLongArray();
    benchmarkSortLongSeries();
    benchmarkSortLongArray();
    benchmarkUniqueLongSeries();
    benchmarkUniqueLongArrayWithObjects();
    benchmarkMapDoubleSeries();
    benchmarkMapDoubleSeriesOperation();
    benchmarkMapDoubleArray();
    benchmarkMapLongSeries();
    benchmarkMapLongSeriesOperation();
    benchmarkMapLongArray();
    benchmarkMapTwoSeries();
    benchmarkMapTwoSeriesOperation();
    benchmarkMapTwoArrays();
    benchmarkMapThreeSeries();
    benchmarkMapThreeArrays();
    benchmarkMapFourSeriesGeneric();
    benchmarkMapFourArrays();
    benchmarkMapTwoSeriesExpression();
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
    this.results.append(name, tMin, tMid, tMax, checksum, this.times.size());

    // reset timer stats
    this.times = new ArrayList<>();
  }

  public static void main(String[] args) throws Exception {
    LOG.info("Press Enter key to start.");
    System.in.read();

    LOG.info("Running DataFrame benchmark ...");
    DataFrameBenchmark b = new DataFrameBenchmark();
    b.benchmarkAll();

    DataFrame df = b.results.build();
    df.mapInPlace(new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] / 1000000;
      }
    }, "min");
    df.mapInPlace(new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] / 1000000;
      }
    }, "mid");
    df.mapInPlace(new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] / 1000000;
      }
    }, "max");
    df.mapInPlace(new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] % 1000;
      }
    }, "checksum");

    LOG.info("Summary:\n{}", df.toString(40, SERIES_NAMES));
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
