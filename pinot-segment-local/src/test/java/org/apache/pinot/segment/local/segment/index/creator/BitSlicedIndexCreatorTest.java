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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitSlicedRangeIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.BitSlicedRangeIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;
import static org.apache.pinot.spi.data.FieldSpec.DataType.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class BitSlicedIndexCreatorTest {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BitSlicedIndexCreatorTest");
  private static final String COLUMN_NAME = "testColumn";
  private static final int SEED = 42;


  @BeforeClass
  public void setUp() throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @AfterClass
  public void tearDown() throws IOException {
    FileUtils.forceDelete(INDEX_DIR);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFailToCreateRawString() {
    new BitSlicedRangeIndexCreator(INDEX_DIR, new ColumnMetadataImpl.Builder()
        .setFieldSpec(new DimensionFieldSpec("foo", STRING, true)).build());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFailToCreateMV() {
    new BitSlicedRangeIndexCreator(INDEX_DIR, new ColumnMetadataImpl.Builder()
        .setFieldSpec(new DimensionFieldSpec("foo", INT, false)).build());
  }

  @Test
  public void testCreateAndQueryInt() throws IOException {
    testInt(Dataset.createInt(1000, 10, Distribution.NORMAL, 0, 100));
    testInt(Dataset.createInt(1000, 10, Distribution.NORMAL, -10_000_000, 10_000));
    testInt(Dataset.createInt(1000, 10, Distribution.UNIFORM, 1000, 10_000_000));
    testInt(Dataset.createInt(1000, 10, Distribution.EXP, 0.0001));
    testInt(Dataset.createInt(1000, 10, Distribution.EXP, 0.5));
    testInt(Dataset.createInt(1000, 10, Distribution.EXP, 0.9999));
    testInt(Dataset.createInt(1000, 10, Distribution.UNIFORM, Integer.MIN_VALUE, Integer.MAX_VALUE));
    testInt(Dataset.createInt(1000, 10, Distribution.UNIFORM, 0, Integer.MAX_VALUE));
    testInt(Dataset.createInt(1000, 10, Distribution.UNIFORM, Integer.MIN_VALUE, 0));
    testInt(Dataset.createInt(1000, 10, Distribution.UNIFORM, Integer.MAX_VALUE, Integer.MIN_VALUE));
  }

  @Test(description = "ensures supported raw data type isn't used when there is a dictionary")
  public void testCreateAndQueryDictionarizedFloat() throws IOException {
    testInt(Dataset.createDictionarized(FLOAT, 1000, 10, Distribution.NORMAL, 0, 100));
    testInt(Dataset.createDictionarized(FLOAT, 1000, 10, Distribution.UNIFORM, 1000, 10_000_000));
    testInt(Dataset.createDictionarized(FLOAT, 1000, 10, Distribution.EXP, 0.0001));
  }

  @Test(description = "ensures unsupported raw data type isn't used when there is a dictionary")
  public void testCreateAndQueryDictionarizedString() throws IOException {
    testInt(Dataset.createDictionarized(STRING, 1000, 10, Distribution.NORMAL, 0, 100));
    testInt(Dataset.createDictionarized(STRING, 1000, 10, Distribution.UNIFORM, 1000, 10_000_000));
    testInt(Dataset.createDictionarized(STRING, 1000, 10, Distribution.EXP, 0.0001));
  }

  @Test
  public void testCreateAndQueryLong() throws IOException {
    testLong(Dataset.createLong(1000, 10, Distribution.NORMAL, 0, 100));
    testLong(Dataset.createLong(1000, 10, Distribution.NORMAL, -10_000_000, 10_000));
    testLong(Dataset.createLong(1000, 10, Distribution.UNIFORM, 1000, 10_000_000));
    testLong(Dataset.createLong(1000, 10, Distribution.EXP, 0.0001));
    testLong(Dataset.createLong(1000, 10, Distribution.EXP, 0.5));
    testLong(Dataset.createLong(1000, 10, Distribution.EXP, 0.9999));
  }

  @Test
  public void testCreateAndQueryTimestamp() throws IOException {
    testLong(Dataset.createLong(TIMESTAMP, 1000, 10, Distribution.NORMAL, 0, 100));
    testLong(Dataset.createLong(TIMESTAMP, 1000, 10, Distribution.NORMAL, -10_000_000, 10_000));
    testLong(Dataset.createLong(TIMESTAMP, 1000, 10, Distribution.UNIFORM, 1000, 10_000_000));
    testLong(Dataset.createLong(TIMESTAMP, 1000, 10, Distribution.EXP, 0.0001));
    testLong(Dataset.createLong(TIMESTAMP, 1000, 10, Distribution.EXP, 0.5));
    testLong(Dataset.createLong(TIMESTAMP, 1000, 10, Distribution.EXP, 0.9999));
  }

  @Test
  public void testCreateAndQueryFloat() throws IOException {
    testFloat(Dataset.createFloat(1000, 10, Distribution.NORMAL, 0, 100));
    testFloat(Dataset.createFloat(1000, 10, Distribution.NORMAL, -10_000_000, 10_000));
    testFloat(Dataset.createFloat(1000, 10, Distribution.UNIFORM, 1000, 10_000_000));
    testFloat(Dataset.createFloat(1000, 10, Distribution.EXP, 0.0001));
    testFloat(Dataset.createFloat(1000, 10, Distribution.EXP, 0.5));
    testFloat(Dataset.createFloat(1000, 10, Distribution.EXP, 0.9999));
  }

  @Test
  public void testCreateAndQueryDouble() throws IOException {
    testDouble(Dataset.createDouble(1000, 10, Distribution.NORMAL, 0, 100));
    testDouble(Dataset.createDouble(1000, 10, Distribution.NORMAL, -10_000_000, 10_000));
    testDouble(Dataset.createDouble(1000, 10, Distribution.UNIFORM, 1000, 10_000_000));
    testDouble(Dataset.createDouble(1000, 10, Distribution.EXP, 0.0001));
    testDouble(Dataset.createDouble(1000, 10, Distribution.EXP, 0.5));
    testDouble(Dataset.createDouble(1000, 10, Distribution.EXP, 0.9999));
  }

  private void testInt(Dataset<int[]> dataset) throws IOException {
    ColumnMetadata metadata = dataset.toColumnMetadata();
    try (BitSlicedRangeIndexCreator creator = new BitSlicedRangeIndexCreator(INDEX_DIR, metadata)) {
      for (int value : dataset.values()) {
        creator.add(value);
      }
      creator.seal();
    }
    File rangeIndexFile = new File(INDEX_DIR, metadata.getColumnName() + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(rangeIndexFile)) {
      BitSlicedRangeIndexReader reader = new BitSlicedRangeIndexReader(dataBuffer);
      int prev = Integer.MIN_VALUE;
      for (int quantile : dataset.quantiles()) {
        ImmutableRoaringBitmap reference = dataset.scan(prev, quantile);
        ImmutableRoaringBitmap result = reader.getMatchingDocIds(prev, quantile);
        assertEquals(reference, result);
        prev = quantile;
      }
      ImmutableRoaringBitmap result = reader.getMatchingDocIds(prev + 1, Integer.MAX_VALUE);
      assertTrue(result != null && result.isEmpty());
    } finally {
      FileUtils.forceDelete(rangeIndexFile);
    }
  }

  private void testLong(Dataset<long[]> dataset) throws IOException {
    ColumnMetadata metadata = dataset.toColumnMetadata();
    try (BitSlicedRangeIndexCreator creator = new BitSlicedRangeIndexCreator(INDEX_DIR, metadata)) {
      for (long value : dataset.values()) {
        creator.add(value);
      }
      creator.seal();
    }
    File rangeIndexFile = new File(INDEX_DIR, metadata.getColumnName() + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(rangeIndexFile)) {
      BitSlicedRangeIndexReader reader = new BitSlicedRangeIndexReader(dataBuffer);
      long prev = Long.MIN_VALUE;
      for (long quantile : dataset.quantiles()) {
        ImmutableRoaringBitmap reference = dataset.scan(prev, quantile);
        ImmutableRoaringBitmap result = reader.getMatchingDocIds(prev, quantile);
        assertEquals(reference, result);
        prev = quantile;
      }
      ImmutableRoaringBitmap result = reader.getMatchingDocIds(prev + 1, Long.MAX_VALUE);
      assertTrue(result != null && result.isEmpty());
    } finally {
      FileUtils.forceDelete(rangeIndexFile);
    }
  }

  private void testFloat(Dataset<float[]> dataset) throws IOException {
    ColumnMetadata metadata = dataset.toColumnMetadata();
    try (BitSlicedRangeIndexCreator creator = new BitSlicedRangeIndexCreator(INDEX_DIR, metadata)) {
      for (float value : dataset.values()) {
        creator.add(value);
      }
      creator.seal();
    }
    File rangeIndexFile = new File(INDEX_DIR, metadata.getColumnName() + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(rangeIndexFile)) {
      BitSlicedRangeIndexReader reader = new BitSlicedRangeIndexReader(dataBuffer);
      float prev = Float.NEGATIVE_INFINITY;
      for (float quantile : dataset.quantiles()) {
        ImmutableRoaringBitmap reference = dataset.scan(prev, quantile);
        ImmutableRoaringBitmap result = reader.getMatchingDocIds(prev, quantile);
        assertEquals(reference, result);
        prev = quantile;
      }
      ImmutableRoaringBitmap result = reader.getMatchingDocIds(prev + 1, Float.POSITIVE_INFINITY);
      assertTrue(result != null && result.isEmpty());
    } finally {
      FileUtils.forceDelete(rangeIndexFile);
    }
  }

  private void testDouble(Dataset<double[]> dataset) throws IOException {
    ColumnMetadata metadata = dataset.toColumnMetadata();
    try (BitSlicedRangeIndexCreator creator = new BitSlicedRangeIndexCreator(INDEX_DIR, metadata)) {
      for (double value : dataset.values()) {
        creator.add(value);
      }
      creator.seal();
    }
    File rangeIndexFile = new File(INDEX_DIR, metadata.getColumnName() + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(rangeIndexFile)) {
      BitSlicedRangeIndexReader reader = new BitSlicedRangeIndexReader(dataBuffer);
      double prev = Double.NEGATIVE_INFINITY;
      for (double quantile : dataset.quantiles()) {
        ImmutableRoaringBitmap reference = dataset.scan(prev, quantile);
        ImmutableRoaringBitmap result = reader.getMatchingDocIds(prev, quantile);
        assertEquals(reference, result);
        prev = quantile;
      }
      ImmutableRoaringBitmap result = reader.getMatchingDocIds(prev + 1, Double.POSITIVE_INFINITY);
      assertTrue(result != null && result.isEmpty());
    } finally {
      FileUtils.forceDelete(rangeIndexFile);
    }
  }

  enum Distribution {
    NORMAL {
      @Override
      public DoubleSupplier createDouble(double... params) {
        Random random = new Random(SEED);
        return () -> random.nextGaussian() * params[1] + params[0];
      }
    },
    UNIFORM {
      @Override
      public DoubleSupplier createDouble(double... params) {
        Random random = new Random(SEED);
        return () -> (params[1] - params[0]) * random.nextDouble() + params[0];
      }
    },
    EXP {
      @Override
      public DoubleSupplier createDouble(double... params) {
        Random random = new Random(SEED);
        return () -> -(Math.log(random.nextDouble()) / params[0]);
      }
    };

    public LongSupplier createLong(double... params) {
      DoubleSupplier source = createDouble(params);
      return () -> (long) source.getAsDouble();
    }

    public abstract DoubleSupplier createDouble(double... params);
  }

  private static class Dataset<T> {

    private Dataset(FieldSpec.DataType dataType,
                    T data,
                    T quantiles,
                    int numDocs,
                    int numQuantiles,
                    int cardinality) {
      _dataType = dataType;
      _data = data;
      _quantiles = quantiles;
      _numDocs = numDocs;
      _numQuantiles = numQuantiles;
      _cardinality = cardinality;
    }

    public static Dataset<int[]> createDictionarized(FieldSpec.DataType rawDataType,
                                                     int count,
                                                     int quantileCount,
                                                     Distribution distribution,
                                                     double... params) {
      LongSupplier supplier = distribution.createLong(params);
      int[] data = IntStream.range(0, count)
          .map(i -> (int) supplier.getAsLong())
          .toArray();
      int[] distinct = Arrays.stream(data).distinct().sorted().toArray();
      for (int i = 0; i < data.length; i++) {
        data[i] = Arrays.binarySearch(distinct, data[i]);
      }
      int[] quantiles = computeQuantiles(data, quantileCount);
      return new Dataset<>(rawDataType, data, quantiles, count, quantileCount, distinct.length);
    }

    public static Dataset<int[]> createInt(int count,
                                           int quantileCount,
                                           Distribution distribution,
                                           double... params) {
      LongSupplier supplier = distribution.createLong(params);
      int[] data = IntStream.range(0, count)
          .map(i -> (int) supplier.getAsLong())
          .toArray();
      int[] quantiles = computeQuantiles(data, quantileCount);
      return new Dataset<>(INT, data, quantiles, count, quantileCount, -1);
    }

    public static Dataset<long[]> createLong(int count,
                                             int quantileCount,
                                             Distribution distribution,
                                             double... params) {
      return createLong(LONG, count, quantileCount, distribution, params);
    }

    public static Dataset<long[]> createLong(FieldSpec.DataType dataType,
                                             int count,
                                             int quantileCount,
                                             Distribution distribution,
                                             double... params) {
      LongSupplier supplier = distribution.createLong(params);
      long[] data = IntStream.range(0, count)
          .mapToLong(i -> supplier.getAsLong())
          .toArray();
      long[] quantiles = computeQuantiles(data, quantileCount);
      return new Dataset<>(dataType, data, quantiles, count, quantileCount, -1);
    }

    public static Dataset<float[]> createFloat(int count,
                                               int quantileCount,
                                               Distribution distribution,
                                               double... params) {
      DoubleSupplier supplier = distribution.createDouble(params);
      float[] data = new float[count];
      for (int i = 0; i < count; i++) {
        data[i] = (float) supplier.getAsDouble();
      }
      float[] quantiles = computeQuantiles(data, quantileCount);
      return new Dataset<>(FLOAT, data, quantiles, count, quantileCount, -1);
    }

    public static Dataset<double[]> createDouble(int count,
                                                 int quantileCount,
                                                 Distribution distribution,
                                                 double... params) {
      DoubleSupplier supplier = distribution.createDouble(params);
      double[] data = new double[count];
      for (int i = 0; i < count; i++) {
        data[i] = supplier.getAsDouble();
      }
      double[] quantiles = computeQuantiles(data, quantileCount);
      return new Dataset<>(DOUBLE, data, quantiles, count, quantileCount, -1);
    }

    private final FieldSpec.DataType _dataType;
    private final T _data;
    private final T _quantiles;
    private final int _numDocs;
    private final int _numQuantiles;
    private final int _cardinality;

    public ColumnMetadata toColumnMetadata() {
      return new ColumnMetadataImpl.Builder()
          .setMinValue(min())
          .setMaxValue(max())
          .setTotalDocs(_numDocs)
          .setCardinality(_cardinality)
          .setHasDictionary(_data instanceof int[] && _dataType != INT)
          .setFieldSpec(new DimensionFieldSpec(COLUMN_NAME, _dataType, true))
          .build();
    }

    public T values() {
      return _data;
    }

    public T quantiles() {
      return _quantiles;
    }

    private static int[] computeQuantiles(int[] data, int count) {
      int[] quantiles = new int[count + 1];
      int[] copy = Arrays.copyOf(data, data.length);
      Arrays.sort(copy);
      int stride = copy.length / count;
      for (int i = 0, q = 0; i < copy.length; i += stride, q++) {
        quantiles[q] = copy[i];
      }
      quantiles[count] = copy[copy.length - 1];
      return quantiles;
    }

    private static long[] computeQuantiles(long[] data, int count) {
      long[] quantiles = new long[count + 1];
      long[] copy = Arrays.copyOf(data, data.length);
      Arrays.sort(copy);
      int stride = copy.length / count;
      for (int i = 0, q = 0; i < copy.length; i += stride, q++) {
        quantiles[q] = copy[i];
      }
      quantiles[count] = copy[copy.length - 1];
      return quantiles;
    }

    private static float[] computeQuantiles(float[] data, int count) {
      float[] quantiles = new float[count + 1];
      float[] copy = Arrays.copyOf(data, data.length);
      Arrays.sort(copy);
      int stride = copy.length / count;
      for (int i = 0, q = 0; i < copy.length; i += stride, q++) {
        quantiles[q] = copy[i];
      }
      quantiles[count] = copy[copy.length - 1];
      return quantiles;
    }

    private static double[] computeQuantiles(double[] data, int count) {
      double[] quantiles = new double[count + 1];
      double[] copy = Arrays.copyOf(data, data.length);
      Arrays.sort(copy);
      int stride = copy.length / count;
      for (int i = 0, q = 0; i < copy.length; i += stride, q++) {
        quantiles[q] = copy[i];
      }
      quantiles[count] = copy[copy.length - 1];
      return quantiles;
    }

    public Comparable<?> min() {
      if (_quantiles instanceof int[]) {
        // INT or dictionarized
        return ((int[]) _quantiles)[0];
      }
      switch (_dataType.getStoredType()) {
        case LONG:
          return ((long[]) _quantiles)[0];
        case FLOAT:
          return ((float[]) _quantiles)[0];
        case DOUBLE:
          return ((double[]) _quantiles)[0];
        case INT:
        default:
          return null;
      }
    }

    public Comparable<?> max() {
      if (_quantiles instanceof int[]) {
        // INT or dictionarized
        return ((int[]) _quantiles)[_numQuantiles];
      }
      switch (_dataType.getStoredType()) {
        case LONG:
          return ((long[]) _quantiles)[_numQuantiles];
        case FLOAT:
          return ((float[]) _quantiles)[_numQuantiles];
        case DOUBLE:
          return ((double[]) _quantiles)[_numQuantiles];
        case INT:
        default:
          return null;
      }
    }

    public ImmutableRoaringBitmap scan(int lower, int upper) {
      MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
      int[] data = (int[]) _data;
      for (int i = 0; i < data.length; i++) {
        if (data[i] <= upper && data[i] >= lower) {
          bitmap.add(i);
        }
      }
      return bitmap;
    }

    public ImmutableRoaringBitmap scan(long lower, long upper) {
      MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
      long[] data = (long[]) _data;
      for (int i = 0; i < data.length; i++) {
        if (data[i] <= upper && data[i] >= lower) {
          bitmap.add(i);
        }
      }
      return bitmap;
    }

    public ImmutableRoaringBitmap scan(float lower, float upper) {
      MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
      float[] data = (float[]) _data;
      for (int i = 0; i < data.length; i++) {
        if (data[i] <= upper && data[i] >= lower) {
          bitmap.add(i);
        }
      }
      return bitmap;
    }

    public ImmutableRoaringBitmap scan(double lower, double upper) {
      MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
      double[] data = (double[]) _data;
      for (int i = 0; i < data.length; i++) {
        if (data[i] <= upper && data[i] >= lower) {
          bitmap.add(i);
        }
      }
      return bitmap;
    }
  }
}
