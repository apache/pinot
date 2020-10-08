/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataFrameBenchmark {
  // TODO: validate benchmarking method - Dead Code Elimination, etc. may be playing tricks on us.

  private static final Logger LOG = LoggerFactory.getLogger(DataFrameBenchmark.class);

  private static final int N_ROUNDS = 15;
  private static final int N_ROUNDS_SLOW = 3;
  private static final int N_ELEMENTS = 10_000_000;
  private static final int N_NULLS = 100_000;
  private static final int N_WINDOW = 1000;
  private static final int N_GROUPS = 1000;

  private static final String[] SERIES_NAMES = new String[] { "task", "min", "mid", "max", "outer", "checksum", "samples" };

  private static final long SEED = System.nanoTime();

  private long tStart;
  private long tStartOuter;
  private List<Long> times = new ArrayList<>();
  private long timeOuter;
  private DataFrame.Builder results = DataFrame.builder(SERIES_NAMES);

  private void benchmarkMapDoubleSeries() {
    startTimerOuter();
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

  private void benchmarkMapDoubleSeriesOperation() {
    startTimerOuter();
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

  private void benchmarkMapDoubleArray() {
    startTimerOuter();
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

  private void benchmarkMapLongSeries() {
    startTimerOuter();
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

  private void benchmarkMapLongSeriesOperation() {
    startTimerOuter();
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

  private void benchmarkMapLongArray() {
    startTimerOuter();
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

  private void benchmarkMapLongObjectSeriesOperation() {
    startTimerOuter();
    long checksum = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      Long[] longValues = generateLongObjectData(N_ELEMENTS);
      final long delta = r;

      startTimer();
      ObjectSeries s = ObjectSeries.buildFrom((Object[])longValues);
      ObjectSeries sResult = s.map(new Series.ObjectFunction() {
        @Override
        public Object apply(Object... values) {
          return ((Long)values[0]) + delta;
        }
      });
      stopTimer();

      // to long array
      long[] values = new long[N_ELEMENTS];
      for(int i=0; i<N_ELEMENTS; i++)
        values[i] = (Long)sResult.getObject(i);

      checksum ^= checksum(values);
    }

    logResults("benchmarkMapLongObjectSeriesOperation", checksum);
  }

  private void benchmarkMapLongObjectArray() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      Long[] longValues = generateLongObjectData(N_ELEMENTS);
      final long delta = r;

      startTimer();
      Long[] results = new Long[longValues.length];
      for (int i = 0; i < longValues.length; i++) {
        results[i] = longValues[i] + delta;
      }
      stopTimer();

      checksum ^= checksum(ArrayUtils.toPrimitive(results));
    }

    logResults("benchmarkMapLongObjectArray", checksum);
  }

  private void benchmarkMapTwoSeriesExpression() {
    startTimerOuter();
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

  private void benchmarkMapTwoSeries() {
    startTimerOuter();
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

  private void benchmarkMapTwoSeriesOperation() {
    startTimerOuter();
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

  private void benchmarkMapTwoArrays() {
    startTimerOuter();
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

  private void benchmarkMapThreeSeries() {
    startTimerOuter();
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

  private void benchmarkMapThreeArrays() {
    startTimerOuter();
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

  private void benchmarkMapFourSeriesGeneric() {
    startTimerOuter();
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

  private void benchmarkMapFourArrays() {
    startTimerOuter();
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

  private void benchmarkMinMaxLongSeries() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      LongSeries s = LongSeries.buildFrom(longValues);

      startTimer();
      long min = s.min().value();
      long max = s.max().value();
      stopTimer();

      checksum ^= checksum(min, max);
    }

    logResults("benchmarkMinMaxLongSeries", checksum);
  }

  private void benchmarkMinMaxLongArray() {
    startTimerOuter();
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

  private void benchmarkEqualsLongArray() {
    startTimerOuter();
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

  private void benchmarkEqualsLongSeries() {
    startTimerOuter();
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

  private void benchmarkEqLongSeries() {
    startTimerOuter();
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

    logResults("benchmarkEqLongSeries", checksum);
  }

  private void benchmarkSortLongArray() {
    startTimerOuter();
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

  private void benchmarkSortLongSeries() {
    startTimerOuter();
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

  private void benchmarkUniqueLongArrayWithObjects() {
    startTimerOuter();
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

  private void benchmarkUniqueLongSeries() {
    startTimerOuter();
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

  private void benchmarkShiftLongArray() {
    startTimerOuter();
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

  private void benchmarkShiftLongSeries() {
    startTimerOuter();
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

  private void benchmarkDropNullLongSeries() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      for(int i : randomIndices(N_ELEMENTS, N_NULLS)) {
        longValues[i] = LongSeries.NULL;
      }
      LongSeries series = LongSeries.buildFrom(longValues);

      startTimer();
      LongSeries out = series.dropNull();
      stopTimer();

      checksum ^= checksum(out.values());
    }

    logResults("benchmarkDropNullLongSeries", checksum);
  }

  private void benchmarkDropNullLongArray() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      for(int i : randomIndices(N_ELEMENTS, N_NULLS)) {
        longValues[i] = LongSeries.NULL;
      }

      startTimer();
      int count = 0;
      for(int i=0; i<N_ELEMENTS; i++) {
        if(longValues[i] != LongSeries.NULL) {
          longValues[count] = longValues[i];
          count++;
        }
      }
      long[] out = Arrays.copyOf(longValues, count);
      stopTimer();

      checksum ^= checksum(out);
    }

    logResults("benchmarkDropNullLongArray", checksum);
  }

  private void benchmarkAggregateLongSeries() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      LongSeries series = LongSeries.buildFrom(longValues);

      startTimer();
      long out = series.sum().value();
      stopTimer();

      checksum ^= checksum(out);
    }

    logResults("benchmarkAggregateLongSeries", checksum);
  }

  private void benchmarkAggregateLongArray() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);

      startTimer();
      long sum = 0;
      for(int i=0; i<N_ELEMENTS; i++) {
        sum += longValues[i];
      }
      stopTimer();

      checksum ^= checksum(sum);
    }

    logResults("benchmarkAggregateLongArray", checksum);
  }

  private void benchmarkHasNullLongSeries() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      LongSeries series = LongSeries.buildFrom(longValues);

      startTimer();
      boolean out = series.hasNull();
      stopTimer();

      checksum ^= checksum(out ? 1 : 0);
    }

    logResults("benchmarkHasNullLongSeries", checksum);
  }

  private void benchmarkExpandingWindowSumLongSeries() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      LongSeries series = LongSeries.buildFrom(longValues);

      startTimer();
      LongSeries out = series.groupByExpandingWindow().sum().getValues().getLongs();
      stopTimer();

      checksum ^= checksum(out.values());
    }

    logResults("benchmarkExpandingWindowSumLongSeries", checksum);
  }

  private void benchmarkExpandingWindowSumLongArray() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);

      startTimer();
      long sum = 0;
      long[] out = new long[N_ELEMENTS];
      for(int i=0; i<N_ELEMENTS; i++) {
        sum += longValues[i];
        out[i] = sum;
      }
      stopTimer();

      checksum ^= checksum(out);
    }

    logResults("benchmarkExpandingWindowSumLongArray", checksum);
  }

  private void benchmarkExpandingWindowMaxLongSeries() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      LongSeries series = LongSeries.buildFrom(longValues);

      startTimer();
      LongSeries out = series.groupByExpandingWindow().max().getValues().getLongs();
      stopTimer();

      checksum ^= checksum(out.values());
    }

    logResults("benchmarkExpandingWindowMaxLongSeries", checksum);
  }

  private void benchmarkExpandingWindowMaxLongArray() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);

      startTimer();
      long max = longValues[0];
      long[] out = new long[N_ELEMENTS];
      for(int i=0; i<N_ELEMENTS; i++) {
        max = Math.max(longValues[i], max);
        out[i] = max;
      }
      stopTimer();

      checksum ^= checksum(out);
    }

    logResults("benchmarkExpandingWindowMaxLongArray", checksum);
  }

  private void benchmarkMovingWindowSumLongSeries() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      LongSeries series = LongSeries.buildFrom(longValues);

      startTimer();
      LongSeries out = series.groupByMovingWindow(N_WINDOW).sum().getValues().getLongs();
      stopTimer();

      checksum ^= checksum(out.values());
    }

    logResults("benchmarkMovingWindowSumLongSeries", checksum);
  }

  private void benchmarkMovingWindowSumLongArray() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);

      startTimer();
      long sum = 0;
      long[] out = new long[N_ELEMENTS];
      for(int i=0; i<N_WINDOW - 1; i++) {
        sum += longValues[i];
        out[i] = LongSeries.NULL;
      }
      sum += longValues[N_WINDOW - 1];
      out[N_WINDOW - 1] = sum;
      for(int i=N_WINDOW; i<N_ELEMENTS; i++) {
        sum += longValues[i] - longValues[i - N_WINDOW];
        out[i] = sum;
      }
      stopTimer();

      checksum ^= checksum(out);
    }

    logResults("benchmarkMovingWindowSumLongArray", checksum);
  }

  private void benchmarkHashJoinOuterLongSeries() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS_SLOW; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      LongSeries series = LongSeries.buildFrom(longValues);
      LongSeries other = LongSeries.buildFrom(shuffle(longValues));

      startTimer();
      Series.JoinPairs pairs = Series.hashJoinOuter(new Series[] { series }, new Series[] { other });
      stopTimer();

      if(pairs.size() != N_ELEMENTS)
        throw new IllegalStateException(String.format("Join incorrect (got %d pairs, should be %d)", pairs.size(), N_ELEMENTS));

      checksum ^= checksum(pairs);
    }

    logResults("benchmarkHashJoinOuterLongSeries", checksum);
  }

  private void benchmarkHashJoinOuterGuavaLongSeries() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS_SLOW; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      LongSeries series = LongSeries.buildFrom(longValues);
      LongSeries other = LongSeries.buildFrom(shuffle(longValues));

      startTimer();
      Series.JoinPairs pairs = Series.hashJoinOuterGuava(new Series[] { series }, new Series[] { other });
      stopTimer();

      if(pairs.size() != N_ELEMENTS)
        throw new IllegalStateException(String.format("Join incorrect (got %d pairs, should be %d)", pairs.size(), N_ELEMENTS));

      checksum ^= checksum(pairs);
    }

    logResults("benchmarkHashJoinOuterGuavaLongSeries", checksum);
  }

  private void benchmarkHashJoinInnerLongSeries() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS_SLOW; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      LongSeries series = LongSeries.buildFrom(longValues);
      LongSeries other = LongSeries.buildFrom(shuffle(longValues));

      startTimer();
      Series.JoinPairs pairs = Series.hashJoinInner(new Series[] { series }, new Series[] { other });
      stopTimer();

      if(pairs.size() != N_ELEMENTS)
        throw new IllegalStateException(String.format("Join incorrect (got %d pairs, should be %d)", pairs.size(), N_ELEMENTS));

      checksum ^= checksum(pairs);
    }

    logResults("benchmarkHashJoinInnerLongSeries", checksum);
  }

  private void benchmarkGroupByValueLongSeries() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      long[] keyValues = new long[N_ELEMENTS];
      for(int i=0; i<N_ELEMENTS; i++) {
        keyValues[i] = i % N_GROUPS;
      }

      DataFrame df = new DataFrame();
      df.addSeries("key", keyValues);
      df.addSeries("value", longValues);

      startTimer();
      Grouping.GroupingDataFrame result = df.groupByValue("key").sum("value");
      stopTimer();

      if(result.size() != N_GROUPS)
        throw new IllegalStateException(String.format("GroupBy incorrect (got %d keys, should be %d)", result.size(), N_GROUPS));

      checksum ^= checksum(result.getValues().getLongs().values());
    }

    logResults("benchmarkGroupByValueLongSeries", checksum);
  }

  private void benchmarkGroupByValueMultipleSeries() {
    startTimerOuter();
    long checksum = 0;

    for(int r=0; r<N_ROUNDS_SLOW; r++) {
      long[] longValues = generateLongData(N_ELEMENTS);
      long[] longKeyValues = new long[N_ELEMENTS];
      double[] doubleKeyValues = new double[N_ELEMENTS];
      for(int i=0; i<N_ELEMENTS; i++) {
        longKeyValues[i] = i % N_GROUPS;
        doubleKeyValues[i] = i % N_GROUPS;
      }

      DataFrame df = new DataFrame();
      df.addSeries("longKey", longKeyValues);
      df.addSeries("doubleKey", doubleKeyValues);
      df.addSeries("value", longValues);

      startTimer();
      Grouping.GroupingDataFrame result = df.groupByValue("longKey", "doubleKey").sum("value");
      stopTimer();

      if(result.size() != N_GROUPS)
        throw new IllegalStateException(String.format("GroupBy incorrect (got %d keys, should be %d)", result.size(), N_GROUPS));

      checksum ^= checksum(result.getValues().getLongs().values());
    }

    logResults("benchmarkGroupByValueMultipleSeries", checksum);
  }

  private void benchmarkAll() {
    benchmarkGroupByValueLongSeries();
    benchmarkGroupByValueMultipleSeries();
    benchmarkHashJoinOuterLongSeries();
    benchmarkHashJoinOuterGuavaLongSeries();
    benchmarkHashJoinInnerLongSeries();
    benchmarkHasNullLongSeries();
    benchmarkDropNullLongSeries();
    benchmarkDropNullLongArray();
    benchmarkAggregateLongSeries();
    benchmarkAggregateLongArray();
    benchmarkMinMaxLongSeries();
    benchmarkMinMaxLongArray();
    benchmarkEqualsLongSeries();
    benchmarkEqualsLongArray();
    benchmarkEqLongSeries();
    benchmarkShiftLongSeries();
    benchmarkShiftLongArray();
    benchmarkSortLongSeries();
    benchmarkSortLongArray();
    benchmarkUniqueLongSeries();
    benchmarkUniqueLongArrayWithObjects();
    benchmarkExpandingWindowSumLongSeries();
    benchmarkExpandingWindowSumLongArray();
    benchmarkExpandingWindowMaxLongSeries();
    benchmarkExpandingWindowMaxLongArray();
    benchmarkMovingWindowSumLongSeries();
    benchmarkMovingWindowSumLongArray();
    benchmarkMapDoubleSeries();
    benchmarkMapDoubleSeriesOperation();
    benchmarkMapDoubleArray();
    benchmarkMapLongSeries();
    benchmarkMapLongSeriesOperation();
    benchmarkMapLongArray();
    benchmarkMapLongObjectSeriesOperation();
    benchmarkMapLongObjectArray();
    benchmarkMapTwoSeries();
    benchmarkMapTwoSeriesOperation();
    benchmarkMapTwoArrays();
    benchmarkMapThreeSeries();
    benchmarkMapThreeArrays();
    benchmarkMapFourSeriesGeneric();
    benchmarkMapFourArrays();
    benchmarkMapTwoSeriesExpression();
  }

  private void startTimer() {
    this.tStart = System.nanoTime();
  }

  private void stopTimer() {
    long tDelta = System.nanoTime() - this.tStart;
    this.times.add(tDelta);
  }

  private void startTimerOuter() {
    this.tStartOuter = System.nanoTime();
  }

  private void stopTimerOuter() {
    this.timeOuter = System.nanoTime() - this.tStartOuter;
  }

  private void logResults(String name, long checksum) {
    stopTimerOuter();
    Collections.sort(this.times);
    long tMid = this.times.get(this.times.size() / 2);
    long tMin = Collections.min(this.times);
    long tMax = Collections.max(this.times);
    LOG.info("{}: min/mid/max = {}ms {}ms {}ms [all={}ms, chk={}, cnt={}]", name, tMin / 1000000, tMid / 1000000, tMax / 1000000, timeOuter / 1000000, checksum % 1000, this.times.size());
    this.results.append(name, tMin, tMid, tMax, this.timeOuter, checksum, this.times.size());

    // reset timer stats
    this.times = new ArrayList<>();
  }

  public static void main(String[] args) throws Exception {
    LOG.info("Press Enter key to start.");
    System.in.read();

    LOG.info("Running DataFrame benchmark ...");
    DataFrameBenchmark b = new DataFrameBenchmark();
    b.benchmarkAll();

    Series.LongFunction toMillis = new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] / 1000000;
      }
    };

    DataFrame df = b.results.build();
    df.mapInPlace(toMillis, "min");
    df.mapInPlace(toMillis, "mid");
    df.mapInPlace(toMillis, "max");
    df.mapInPlace(toMillis, "outer");
    df.mapInPlace(new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] % 1000;
      }
    }, "checksum");

    LOG.info("Summary:\n{}", df.toString(40, SERIES_NAMES));
    LOG.info("done.");
  }

  private static int[] randomIndices(int upperBound, int n) {
    Random r = new Random();
    r.setSeed(SEED);
    int[] values = new int[n];
    for(int i=0; i<n; i++) {
      values[i] = r.nextInt(upperBound);
    }
    return values;
  }

  private static double[] generateDoubleData(int n) {
    Random r = new Random();
    r.setSeed(SEED);
    double[] values = new double[n];
    for(int i=0; i<n; i++) {
      values[i] = r.nextDouble();
    }
    return values;
  }

  private static long[] generateLongData(int n) {
    Random r = new Random();
    r.setSeed(SEED);
    long[] values = new long[n];
    for(int i=0; i<n; i++) {
      values[i] = r.nextLong();
    }
    return values;
  }

  private static Long[] generateLongObjectData(int n) {
    Random r = new Random();
    r.setSeed(SEED);
    Long[] values = new Long[n];
    for(int i=0; i<n; i++) {
      values[i] = r.nextLong();
    }
    return values;
  }

  private static long checksum(double... values) {
    long bits = 0;
    for(double v : values) {
      bits ^= Double.doubleToLongBits(v);
    }
    return bits;
  }

  private static long checksum(long... values) {
    long bits = 0;
    for(long v : values) {
      bits ^= v;
    }
    return bits;
  }

  private static long checksum(int... values) {
    long bits = 0;
    for(int v : values) {
      bits ^= v;
    }
    return bits;
  }

  private static long checksum(Series.JoinPairs pairs) {
    long bits = 0;
    for(int i=0; i<pairs.size(); i++) {
      bits ^= pairs.left(i);
    }
    for(int i=0; i<pairs.size(); i++) {
      bits ^= pairs.right(i);
    }
    return bits;
  }

  // from: https://stackoverflow.com/questions/1519736/random-shuffling-of-an-array
  private static long[] shuffle(long[] arr) {
    arr = Arrays.copyOf(arr, arr.length);
    Random rnd = ThreadLocalRandom.current();
    for (int i=arr.length-1; i>0; i--) {
      int index = rnd.nextInt(i + 1);
      long a = arr[index];
      arr[index] = arr[i];
      arr[i] = a;
    }
    return arr;
  }
}
