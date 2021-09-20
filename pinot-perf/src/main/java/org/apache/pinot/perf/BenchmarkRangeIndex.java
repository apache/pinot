/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.perf;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitSlicedRangeIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.RangeIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.BitSlicedRangeIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.RangeIndexReaderImpl;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.creator.RawValueBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;

public class BenchmarkRangeIndex {

  private static final String COLUMN_NAME = "col";

  public enum Distribution {
    NORMAL {
      @Override
      public DoubleSupplier createDouble(long seed, double... params) {
        Random random = new Random(seed);
        return () -> random.nextGaussian() * params[1] + params[0];
      }
    },
    UNIFORM {
      @Override
      public DoubleSupplier createDouble(long seed, double... params) {
        Random random = new Random(seed);
        return () -> (params[1] - params[0]) * random.nextDouble() + params[0];
      }
    },
    EXP {
      @Override
      public DoubleSupplier createDouble(long seed, double... params) {
        Random random = new Random(seed);
        return () -> -(Math.log(random.nextDouble()) / params[0]);
      }
    },
    POWER {
      @Override
      public DoubleSupplier createDouble(long seed, double... params) {
        long min = (long) params[0];
        long max = (long) params[1];
        double alpha = params[2];
        SplittableRandom random = new SplittableRandom(seed);
        return () -> (Math.pow((Math.pow(max, alpha + 1)
            - Math.pow(min, alpha + 1) * (random.nextDouble() + 1)), 1D / (alpha + 1)));
      }
    };

    public LongSupplier createLong(long seed, double... params) {
      DoubleSupplier source = createDouble(seed, params);
      return () -> (long) source.getAsDouble();
    }

    public abstract DoubleSupplier createDouble(long seed, double... params);

    public static LongSupplier createLongSupplier(long seed, String spec) {
      Pair<Distribution, double[]> parsed = parse(spec);
      return parsed.getKey().createLong(seed, parsed.getValue());
    }

    public static DoubleSupplier createDoubleSupplier(long seed, String spec) {
      Pair<Distribution, double[]> parsed = parse(spec);
      return parsed.getKey().createDouble(seed, parsed.getValue());
    }

    private static Pair<Distribution, double[]> parse(String spec) {
      int paramsStart = spec.indexOf('(');
      int paramsEnd = spec.indexOf(')');
      double[] params = Arrays.stream(spec.substring(paramsStart + 1, paramsEnd).split(","))
          .mapToDouble(s -> Double.parseDouble(s.trim()))
          .toArray();
      String dist = spec.substring(0, paramsStart).toUpperCase();
      return Pair.of(Distribution.valueOf(dist), params);
    }
  }

  @State(Scope.Benchmark)
  public static class BaseState {
    @Param({"INT", "LONG", "FLOAT", "DOUBLE"})
    protected FieldSpec.DataType _dataType;

    @Param({"NORMAL(0,1)", "NORMAL(10000000,1000)",
        "EXP(0.0001)", "EXP(0.5)",
        "UNIFORM(0,100000000000)", "UNIFORM(100000000000, 100000000100)",
        "POWER(0,1000000,3)", "POWER(0,1000000000,1)"})
    protected String _scenario;

    @Param({"1000000", "10000000", "100000000"})
    protected int _numDocs;

    @Param("42")
    long _seed;

    protected FieldSpec _fieldSpec;

    protected File _indexDir;

    protected Object _values;

    public void setup() throws IOException {
      _fieldSpec = new DimensionFieldSpec(COLUMN_NAME, _dataType, true);
      _indexDir = new File(FileUtils.getTempDirectory(), "BenchmarkRangeIndex");
      FileUtils.forceMkdir(_indexDir);
      switch (_dataType) {
        case INT: {
          LongSupplier supplier = Distribution.createLongSupplier(_seed, _scenario);
          int[] values = new int[_numDocs];
          for (int i = 0; i < values.length; i++) {
            values[i] = (int) supplier.getAsLong();
          }
          _values = values;
          break;
        }
        case LONG: {
          LongSupplier supplier = Distribution.createLongSupplier(_seed, _scenario);
          long[] values = new long[_numDocs];
          for (int i = 0; i < values.length; i++) {
            values[i] = supplier.getAsLong();
          }
          _values = values;
          break;
        }
        case FLOAT: {
          DoubleSupplier supplier = Distribution.createDoubleSupplier(_seed, _scenario);
          float[] values = new float[_numDocs];
          for (int i = 0; i < values.length; i++) {
            values[i] = (float) supplier.getAsDouble();
          }
          _values = values;
          break;
        }
        case DOUBLE: {
          DoubleSupplier supplier = Distribution.createDoubleSupplier(_seed, _scenario);
          double[] values = new double[_numDocs];
          for (int i = 0; i < values.length; i++) {
            values[i] = supplier.getAsDouble();
          }
          _values = values;
          break;
        }
        default:
          throw new RuntimeException("impossible");
      }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      try {
        FileUtils.forceDelete(_indexDir);
      } catch (IOException e) {
      }
    }

    protected Comparable<?> max() {
      if (_values instanceof int[]) {
        return Arrays.stream((int[]) _values).max().orElse(0);
      }
      if (_values instanceof long[]) {
        return Arrays.stream((long[]) _values).max().orElse(0);
      }
      if (_values instanceof double[]) {
        return Arrays.stream((double[]) _values).max().orElse(0);
      }
      if (_values instanceof float[]) {
        return ((float[]) _values)[IntStream.range(0, _numDocs)
            .reduce(0, (i, j) -> ((float[]) _values)[i] >= ((float[]) _values)[j] ? i : j)];
      }
      return null;
    }

    protected Comparable<?> min() {
      if (_values instanceof int[]) {
        return Arrays.stream((int[]) _values).min().orElse(0);
      }
      if (_values instanceof long[]) {
        return Arrays.stream((long[]) _values).min().orElse(0);
      }
      if (_values instanceof double[]) {
        return Arrays.stream((double[]) _values).min().orElse(0);
      }
      if (_values instanceof float[]) {
        return ((float[]) _values)[IntStream.range(0, _numDocs)
            .reduce(0, (i, j) -> ((float[]) _values)[i] < ((float[]) _values)[j] ? i : j)];
      }
      return null;
    }
  }

  @State(Scope.Benchmark)
  public static class RangeIndexV1CreatorState extends BaseState {

    RangeIndexCreator _creator;

    @Setup(Level.Iteration)
    public void setup() throws IOException {
      super.setup();
      _creator = new RangeIndexCreator(_indexDir, _fieldSpec, _dataType, -1, -1, _numDocs, _numDocs);
    }
  }

  @State(Scope.Benchmark)
  public static class RangeIndexV2CreatorState extends BaseState {

    BitSlicedRangeIndexCreator _creator;

    @Setup(Level.Iteration)
    public void setup() throws IOException {
      super.setup();
      ColumnMetadata metadata = new ColumnMetadataImpl.Builder()
          .setFieldSpec(_fieldSpec)
          .setTotalDocs(_numDocs)
          .setHasDictionary(false)
          .setMaxValue(max())
          .setMinValue(min())
          .build();
      _creator = new BitSlicedRangeIndexCreator(_indexDir, metadata);
    }
  }

  @State(Scope.Benchmark)
  public static abstract class QueryState extends BaseState {
    @Param({"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"})
    int _decile;

    Object _deciles;

    PinotDataBuffer _buffer;

    @Setup(Level.Iteration)
    public void setup() throws IOException {
      super.setup();
      try (RawValueBasedInvertedIndexCreator creator = newCreator()) {
        addValues(creator, _dataType, _values);
      }
      _buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(
          new File(_indexDir, COLUMN_NAME + BITMAP_RANGE_INDEX_FILE_EXTENSION));
      computeDeciles();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      _buffer.close();
      super.tearDown();
    }

    protected abstract RawValueBasedInvertedIndexCreator newCreator() throws IOException;

    private void computeDeciles() {
      switch (_dataType) {
        case INT: {
          int[] deciles = new int[11];
          int[] sorted = Arrays.copyOf((int[]) _values, _numDocs);
          Arrays.sort(sorted);
          for (int i = 0, d = 0; i < sorted.length; i += (sorted.length / 10)) {
            deciles[d++] = sorted[i];
          }
          deciles[10] = sorted[_numDocs - 1];
          _deciles = deciles;
          break;
        }
        case LONG: {
          long[] deciles = new long[11];
          long[] sorted = Arrays.copyOf((long[]) _values, _numDocs);
          Arrays.sort(sorted);
          for (int i = 0, d = 0; i < sorted.length; i += (sorted.length / 10)) {
            deciles[d++] = sorted[i];
          }
          deciles[10] = sorted[_numDocs - 1];
          _deciles = deciles;
          break;
        }
        case FLOAT: {
          float[] deciles = new float[11];
          float[] sorted = Arrays.copyOf((float[]) _values, _numDocs);
          Arrays.sort(sorted);
          for (int i = 0, d = 0; i < sorted.length; i += (sorted.length / 10)) {
            deciles[d++] = sorted[i];
          }
          deciles[10] = sorted[_numDocs - 1];
          _deciles = deciles;
          break;
        }
        case DOUBLE: {
          double[] deciles = new double[11];
          double[] sorted = Arrays.copyOf((double[]) _values, _numDocs);
          Arrays.sort(sorted);
          for (int i = 0, d = 0; i < sorted.length; i += (sorted.length / 10)) {
            deciles[d++] = sorted[i];
          }
          deciles[10] = sorted[_numDocs - 1];
          _deciles = deciles;
          break;
        }
        default:
          throw new RuntimeException("impossible");
      }
    }
  }

  @State(Scope.Benchmark)
  public static class RangeIndexV1State extends QueryState {

    RangeIndexReader<ImmutableRoaringBitmap> _reader;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      super.setup();
      _reader = new RangeIndexReaderImpl(_buffer);
    }

    @Override
    protected RawValueBasedInvertedIndexCreator newCreator() throws IOException {
      return new RangeIndexCreator(_indexDir, _fieldSpec, _dataType, -1, -1, _numDocs, _numDocs);
    }
  }

  @State(Scope.Benchmark)
  public static class RangeIndexV2State extends QueryState {

    RangeIndexReader<ImmutableRoaringBitmap> _reader;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      super.setup();
      _reader = new BitSlicedRangeIndexReader(_buffer);
    }

    @Override
    protected RawValueBasedInvertedIndexCreator newCreator() {
      ColumnMetadata metadata = new ColumnMetadataImpl.Builder()
          .setFieldSpec(_fieldSpec)
          .setTotalDocs(_numDocs)
          .setHasDictionary(false)
          .setMaxValue(max())
          .setMinValue(min())
          .build();
      return new BitSlicedRangeIndexCreator(_indexDir, metadata);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void createV1(RangeIndexV1CreatorState state) throws IOException {
    try (RangeIndexCreator creator = state._creator) {
      addValues(creator, state._dataType, state._values);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void createV2(RangeIndexV2CreatorState state) throws IOException {
    try (BitSlicedRangeIndexCreator creator = state._creator) {
      addValues(creator, state._dataType, state._values);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public ImmutableRoaringBitmap queryV1(RangeIndexV1State state) {
    return query(state._reader, state._dataType, state._decile, state._deciles, state._values);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public ImmutableRoaringBitmap queryV2(RangeIndexV2State state) {
    return query(state._reader, state._dataType, state._decile, state._deciles, state._values);
  }

  private static ImmutableRoaringBitmap query(RangeIndexReader<ImmutableRoaringBitmap> reader,
                                              FieldSpec.DataType dataType, int decile, Object deciles, Object values) {
    switch (dataType) {
      case INT: {
        int[] ints = (int[]) deciles;
        ImmutableRoaringBitmap matching = reader.getMatchingDocIds(ints[decile], ints[decile + 1]);
        ImmutableRoaringBitmap partial = reader.getPartiallyMatchingDocIds(ints[decile], ints[decile + 1]);
        // emulate SVScanDocIdIterator without needing to set it up
        if (partial != null) {
          int[] intValues = (int[]) values;
          int min = ints[decile];
          int max = ints[decile + 1];
          MutableRoaringBitmap result = new MutableRoaringBitmap();
          IntIterator docIdIterator = partial.getIntIterator();
          while (docIdIterator.hasNext()) {
            int next = docIdIterator.next();
            if (intValues[next] >= min && intValues[next] <= max) {
              result.add(next);
            }
          }
          if (matching != null) {
            result.or(matching);
          }
          return result;
        }
        return matching;
      }
      case LONG: {
        long[] longs = (long[]) deciles;
        ImmutableRoaringBitmap matching = reader.getMatchingDocIds(longs[decile], longs[decile + 1]);
        ImmutableRoaringBitmap partial = reader.getPartiallyMatchingDocIds(longs[decile], longs[decile + 1]);
        // emulate SVScanDocIdIterator without needing to set it up
        if (partial != null) {
          long[] longValues = (long[]) values;
          long min = longs[decile];
          long max = longs[decile + 1];
          MutableRoaringBitmap result = new MutableRoaringBitmap();
          IntIterator docIdIterator = partial.getIntIterator();
          while (docIdIterator.hasNext()) {
            int next = docIdIterator.next();
            if (longValues[next] >= min && longValues[next] <= max) {
              result.add(next);
            }
          }
          if (matching != null) {
            result.or(matching);
          }
          return result;
        }
        return matching;
      }
      case FLOAT: {
        float[] floats = (float[]) deciles;
        ImmutableRoaringBitmap matching = reader.getMatchingDocIds(floats[decile], floats[decile + 1]);
        ImmutableRoaringBitmap partial = reader.getPartiallyMatchingDocIds(floats[decile], floats[decile + 1]);
        // emulate SVScanDocIdIterator without needing to set it up
        if (partial != null) {
          float[] floatValues = (float[]) values;
          float min = floats[decile];
          float max = floats[decile + 1];
          MutableRoaringBitmap result = new MutableRoaringBitmap();
          IntIterator docIdIterator = partial.getIntIterator();
          while (docIdIterator.hasNext()) {
            int next = docIdIterator.next();
            if (floatValues[next] >= min && floatValues[next] <= max) {
              result.add(next);
            }
          }
          if (matching != null) {
            result.or(matching);
          }
          return result;
        }
        return matching;
      }
      case DOUBLE: {
        double[] doubles = (double[]) deciles;
        ImmutableRoaringBitmap matching = reader.getMatchingDocIds(doubles[decile], doubles[decile + 1]);
        ImmutableRoaringBitmap partial = reader.getPartiallyMatchingDocIds(doubles[decile], doubles[decile + 1]);
        // emulate SVScanDocIdIterator without needing to set it up
        if (partial != null) {
          double[] doubleValues = (double[]) values;
          double min = doubles[decile];
          double max = doubles[decile + 1];
          MutableRoaringBitmap result = new MutableRoaringBitmap();
          IntIterator docIdIterator = partial.getIntIterator();
          while (docIdIterator.hasNext()) {
            int next = docIdIterator.next();
            if (doubleValues[next] >= min && doubleValues[next] <= max) {
              result.add(next);
            }
          }
          if (matching != null) {
            result.or(matching);
          }
          return result;
        }
        return matching;
      }
      default:
        throw new RuntimeException("impossible");
    }
  }

  private static void addValues(RawValueBasedInvertedIndexCreator creator,
                                FieldSpec.DataType dataType,
                                Object values) throws IOException {
    switch (dataType) {
      case INT:
        for (int value : (int[]) values) {
          creator.add(value);
        }
        break;
      case LONG:
        for (long value : (long[]) values) {
          creator.add(value);
        }
        break;
      case FLOAT:
        for (float value : (float[]) values) {
          creator.add(value);
        }
        break;
      case DOUBLE:
        for (double value : (double[]) values) {
          creator.add(value);
        }
        break;
      default:
        throw new RuntimeException("won't happen");
    }
    creator.seal();
  }
}
