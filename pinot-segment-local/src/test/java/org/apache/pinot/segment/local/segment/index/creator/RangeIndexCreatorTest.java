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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.inv.RangeIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.RangeIndexReaderImpl;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;
import static org.testng.Assert.*;


public class RangeIndexCreatorTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "RangeIndexCreatorTest");
  private static final Random RANDOM = new Random(42);
  private static final String COLUMN_NAME = "testColumn";

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @Test
  public void testInt()
      throws Exception {
    testDataType(DataType.INT);
  }

  @Test
  public void testLong()
      throws Exception {
    testDataType(DataType.LONG);
  }

  @Test
  public void testFloat()
      throws Exception {
    testDataType(DataType.FLOAT);
  }

  @Test
  public void testDouble()
      throws Exception {
    testDataType(DataType.DOUBLE);
  }

  @Test
  public void testIntMV()
      throws Exception {
    testDataTypeMV(DataType.INT);
  }

  @Test
  public void testLongMV()
      throws Exception {
    testDataTypeMV(DataType.LONG);
  }

  @Test
  public void testFloatMV()
      throws Exception {
    testDataTypeMV(DataType.FLOAT);
  }

  @Test
  public void testDoubleMV()
      throws Exception {
    testDataTypeMV(DataType.DOUBLE);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  private void testDataType(DataType dataType)
      throws IOException {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, dataType, true);
    int numDocs = 1000;
    Object values = valuesArray(dataType, numDocs);

    int numValuesPerRange;
    try (RangeIndexCreator creator = new RangeIndexCreator(INDEX_DIR, fieldSpec, dataType, -1, -1, numDocs, numDocs)) {
      addDataToIndexer(dataType, numDocs, 1, creator, values);
      creator.seal();
      // account for off by one bug in v1 implementation
      numValuesPerRange = creator.getNumValuesPerRange() + 1;
    }

    File rangeIndexFile = new File(INDEX_DIR, COLUMN_NAME + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(rangeIndexFile)) {
      RangeIndexReaderImpl rangeIndexReader = new RangeIndexReaderImpl(dataBuffer);
      verifyRangesForDataType(dataType, values,
          splitIntoRanges(dataType, values, numValuesPerRange),
          1, rangeIndexReader);
    }

    FileUtils.forceDelete(rangeIndexFile);
  }

  private void testDataTypeMV(DataType dataType)
      throws IOException {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, dataType, false);
    int numDocs = 1000;
    int numValuesPerMVEntry = 10;
    int numValues = numDocs * numValuesPerMVEntry;
    Object values = valuesArray(dataType, numValues);

    int numValuesPerRange;
    try (
        RangeIndexCreator creator = new RangeIndexCreator(INDEX_DIR, fieldSpec, dataType, -1, -1, numDocs, numValues)) {
      addDataToIndexer(dataType, numDocs, numValuesPerMVEntry, creator, values);
      creator.seal();
      // account for off by one bug in existing implementation
      numValuesPerRange = creator.getNumValuesPerRange() + 1;
    }

    File rangeIndexFile = new File(INDEX_DIR, COLUMN_NAME + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(rangeIndexFile)) {
      RangeIndexReaderImpl rangeIndexReader = new RangeIndexReaderImpl(dataBuffer);
      verifyRangesForDataType(dataType, values,
          splitIntoRanges(dataType, values, numValuesPerRange),
          numValuesPerMVEntry, rangeIndexReader);
    }

    FileUtils.forceDelete(rangeIndexFile);
  }

  private void addDataToIndexer(DataType dataType, int numDocs, int numValuesPerEntry, RangeIndexCreator creator,
      Object values) {
    switch (dataType) {
      case INT:
        if (numValuesPerEntry == 1) {
          for (int i = 0; i < numDocs; i++) {
            int value = RANDOM.nextInt();
            ((int[]) values)[i] = value;
            creator.add(value);
          }
        } else {
          int[] intValues = new int[numValuesPerEntry];
          for (int i = 0; i < numDocs; i++) {
            for (int j = 0; j < numValuesPerEntry; j++) {
              int value = RANDOM.nextInt();
              intValues[j] = value;
              ((int[]) values)[i * numValuesPerEntry + j] = value;
            }
            creator.add(intValues, numValuesPerEntry);
          }
        }
        break;
      case LONG:
        if (numValuesPerEntry == 1) {
          for (int i = 0; i < numDocs; i++) {
            long value = RANDOM.nextLong();
            ((long[]) values)[i] = value;
            creator.add(value);
          }
        } else {
          long[] longValues = new long[numValuesPerEntry];
          for (int i = 0; i < numDocs; i++) {
            for (int j = 0; j < numValuesPerEntry; j++) {
              long value = RANDOM.nextLong();
              longValues[j] = value;
              ((long[]) values)[i * numValuesPerEntry + j] = value;
            }
            creator.add(longValues, numValuesPerEntry);
          }
        }
        break;
      case FLOAT:
        if (numValuesPerEntry == 1) {
          for (int i = 0; i < numDocs; i++) {
            float value = RANDOM.nextFloat();
            ((float[]) values)[i] = value;
            creator.add(value);
          }
        } else {
          float[] floatValues = new float[numValuesPerEntry];
          for (int i = 0; i < numDocs; i++) {
            for (int j = 0; j < numValuesPerEntry; j++) {
              float value = RANDOM.nextFloat();
              floatValues[j] = value;
              ((float[]) values)[i * numValuesPerEntry + j] = value;
            }
            creator.add(floatValues, numValuesPerEntry);
          }
        }
        break;
      case DOUBLE:
        if (numValuesPerEntry == 1) {
          for (int i = 0; i < numDocs; i++) {
            double value = RANDOM.nextDouble();
            ((double[]) values)[i] = value;
            creator.add(value);
          }
        } else {
          double[] doubleValues = new double[numValuesPerEntry];
          for (int i = 0; i < numDocs; i++) {
            for (int j = 0; j < numValuesPerEntry; j++) {
              double value = RANDOM.nextDouble();
              doubleValues[j] = value;
              ((double[]) values)[i * numValuesPerEntry + j] = value;
            }
            creator.add(doubleValues, numValuesPerEntry);
          }
        }
        break;
      default:
        throw new IllegalStateException();
    }
  }

  private void verifyRangesForDataType(DataType dataType, Object values, Object ranges, int numValuesPerMVEntry,
                                       RangeIndexReader<ImmutableRoaringBitmap> rangeIndexReader) {
    switch (dataType) {
      case INT: {
        // single bucket ranges
        int rangeId = 0;
        for (int[] range : (int[][]) ranges) {
          assertNull(rangeIndexReader.getMatchingDocIds(range[0], range[1]),
              "range index can't guarantee match within a single range");
          ImmutableRoaringBitmap partialMatches = rangeIndexReader.getPartiallyMatchingDocIds(range[0], range[1]);
          assertNotNull(partialMatches, "partial matches for single range must not be null");
          for (int docId : partialMatches.toArray()) {
            checkValueForDocId(dataType, values, ranges, rangeId, docId, numValuesPerMVEntry);
          }
          ++rangeId;
        }
        // multi bucket ranges
        int[] lowerPartialRange = ((int[][]) ranges)[0];
        int[] coveredRange = ((int[][]) ranges)[1];
        int[] upperPartialRange = ((int[][]) ranges)[2];
        ImmutableRoaringBitmap matches = rangeIndexReader.getMatchingDocIds(lowerPartialRange[0], upperPartialRange[1]);
        assertNotNull(matches,  "matches for covered range must not be null");
        for (int docId : matches.toArray()) {
          checkValueForDocId(dataType, values, ranges, 1, docId, numValuesPerMVEntry);
        }
        assertEquals(matches, rangeIndexReader.getPartiallyMatchingDocIds(coveredRange[0], coveredRange[1]));
        // partial matches must be the combination of the two edge buckets
        ImmutableRoaringBitmap partialMatches = rangeIndexReader.getPartiallyMatchingDocIds(
            lowerPartialRange[0], upperPartialRange[1]);
        assertNotNull(partialMatches, "partial matches for single range must not be null");
        assertEquals(ImmutableRoaringBitmap.or(
            rangeIndexReader.getPartiallyMatchingDocIds(lowerPartialRange[0], lowerPartialRange[1]),
            rangeIndexReader.getPartiallyMatchingDocIds(upperPartialRange[0], upperPartialRange[1])), partialMatches);
        break;
      }
      case LONG: {
        // single bucket ranges
        int rangeId = 0;
        for (long[] range : (long[][]) ranges) {
          assertNull(rangeIndexReader.getMatchingDocIds(range[0], range[1]),
              "range index can't guarantee match within a single range");
          ImmutableRoaringBitmap partialMatches = rangeIndexReader.getPartiallyMatchingDocIds(range[0], range[1]);
          assertNotNull(partialMatches, "partial matches for single range must not be null");
          for (int docId : partialMatches.toArray()) {
            checkValueForDocId(dataType, values, ranges, rangeId, docId, numValuesPerMVEntry);
          }
          ++rangeId;
        }
        // multi bucket ranges
        long[] lowerPartialRange = ((long[][]) ranges)[0];
        long[] coveredRange = ((long[][]) ranges)[1];
        long[] upperPartialRange = ((long[][]) ranges)[2];
        ImmutableRoaringBitmap matches = rangeIndexReader.getMatchingDocIds(lowerPartialRange[0], upperPartialRange[1]);
        assertNotNull(matches,  "matches for covered range must not be null");
        for (int docId : matches.toArray()) {
          checkValueForDocId(dataType, values, ranges, 1, docId, numValuesPerMVEntry);
        }
        assertEquals(matches, rangeIndexReader.getPartiallyMatchingDocIds(coveredRange[0], coveredRange[1]));
        // partial matches must be the combination of the two edge buckets
        ImmutableRoaringBitmap partialMatches = rangeIndexReader.getPartiallyMatchingDocIds(
            lowerPartialRange[0], upperPartialRange[1]);
        assertNotNull(partialMatches, "partial matches for single range must not be null");
        assertEquals(ImmutableRoaringBitmap.or(
            rangeIndexReader.getPartiallyMatchingDocIds(lowerPartialRange[0], lowerPartialRange[1]),
            rangeIndexReader.getPartiallyMatchingDocIds(upperPartialRange[0], upperPartialRange[1])), partialMatches);
        break;
      }
      case FLOAT: {
        // single bucket ranges
        int rangeId = 0;
        for (float[] range : (float[][]) ranges) {
          assertNull(rangeIndexReader.getMatchingDocIds(range[0], range[1]),
              "range index can't guarantee match within a single range");
          ImmutableRoaringBitmap partialMatches = rangeIndexReader.getPartiallyMatchingDocIds(range[0], range[1]);
          assertNotNull(partialMatches, "partial matches for single range must not be null");
          for (int docId : partialMatches.toArray()) {
            checkValueForDocId(dataType, values, ranges, rangeId, docId, numValuesPerMVEntry);
          }
          ++rangeId;
        }
        // multi bucket ranges
        float[] lowerPartialRange = ((float[][]) ranges)[0];
        float[] coveredRange = ((float[][]) ranges)[1];
        float[] upperPartialRange = ((float[][]) ranges)[2];
        ImmutableRoaringBitmap matches = rangeIndexReader.getMatchingDocIds(lowerPartialRange[0], upperPartialRange[1]);
        assertNotNull(matches,  "matches for covered range must not be null");
        for (int docId : matches.toArray()) {
          checkValueForDocId(dataType, values, ranges, 1, docId, numValuesPerMVEntry);
        }
        assertEquals(matches, rangeIndexReader.getPartiallyMatchingDocIds(coveredRange[0], coveredRange[1]));
        // partial matches must be the combination of the two edge buckets
        ImmutableRoaringBitmap partialMatches = rangeIndexReader.getPartiallyMatchingDocIds(
            lowerPartialRange[0], upperPartialRange[1]);
        assertNotNull(partialMatches, "partial matches for single range must not be null");
        assertEquals(ImmutableRoaringBitmap.or(
            rangeIndexReader.getPartiallyMatchingDocIds(lowerPartialRange[0], lowerPartialRange[1]),
            rangeIndexReader.getPartiallyMatchingDocIds(upperPartialRange[0], upperPartialRange[1])), partialMatches);
        break;
      }
      case DOUBLE: {
        // single bucket ranges
        int rangeId = 0;
        for (double[] range : (double[][]) ranges) {
          assertNull(rangeIndexReader.getMatchingDocIds(range[0], range[1]),
              "range index can't guarantee match within a single range");
          ImmutableRoaringBitmap partialMatches = rangeIndexReader.getPartiallyMatchingDocIds(range[0], range[1]);
          assertNotNull(partialMatches, "partial matches for single range must not be null");
          for (int docId : partialMatches.toArray()) {
            checkValueForDocId(dataType, values, ranges, rangeId, docId, numValuesPerMVEntry);
          }
          ++rangeId;
        }
        // multi bucket ranges
        double[] lowerPartialRange = ((double[][]) ranges)[0];
        double[] coveredRange = ((double[][]) ranges)[1];
        double[] upperPartialRange = ((double[][]) ranges)[2];
        ImmutableRoaringBitmap matches = rangeIndexReader.getMatchingDocIds(lowerPartialRange[0], upperPartialRange[1]);
        assertNotNull(matches,  "matches for covered range must not be null");
        for (int docId : matches.toArray()) {
          checkValueForDocId(dataType, values, ranges, 1, docId, numValuesPerMVEntry);
        }
        assertEquals(matches, rangeIndexReader.getPartiallyMatchingDocIds(coveredRange[0], coveredRange[1]));
        // partial matches must be the combination of the two edge buckets
        ImmutableRoaringBitmap partialMatches = rangeIndexReader.getPartiallyMatchingDocIds(
            lowerPartialRange[0], upperPartialRange[1]);
        assertNotNull(partialMatches, "partial matches for single range must not be null");
        assertEquals(ImmutableRoaringBitmap.or(
            rangeIndexReader.getPartiallyMatchingDocIds(lowerPartialRange[0], lowerPartialRange[1]),
            rangeIndexReader.getPartiallyMatchingDocIds(upperPartialRange[0], upperPartialRange[1])), partialMatches);
        break;
      }
      default:
        throw new IllegalStateException();
    }
  }

  private void checkValueForDocId(DataType dataType, Object values, Object ranges, int rangeId, int docId,
      int numValuesPerEntry) {
    switch (dataType) {
      case INT:
        if (numValuesPerEntry == 1) {
          checkInt((int[][]) ranges, rangeId, ((int[]) values)[docId]);
        } else {
          checkIntMV((int[][]) ranges, rangeId, (int[]) values, docId, numValuesPerEntry);
        }
        break;
      case LONG:
        if (numValuesPerEntry == 1) {
          checkLong((long[][]) ranges, rangeId, ((long[]) values)[docId]);
        } else {
          checkLongMV((long[][]) ranges, rangeId, (long[]) values, docId, numValuesPerEntry);
        }
        break;
      case FLOAT:
        if (numValuesPerEntry == 1) {
          checkFloat((float[][]) ranges, rangeId, ((float[]) values)[docId]);
        } else {
          checkFloatMV((float[][]) ranges, rangeId, (float[]) values, docId, numValuesPerEntry);
        }
        break;
      case DOUBLE:
        if (numValuesPerEntry == 1) {
          checkDouble((double[][]) ranges, rangeId, ((double[]) values)[docId]);
        } else {
          checkDoubleMV((double[][]) ranges, rangeId, (double[]) values, docId, numValuesPerEntry);
        }
        break;
      default:
        throw new IllegalStateException("unexpected type " + dataType);
    }
  }

  private void checkInt(int[][] ranges, int rangeId, int value) {
    Assert.assertTrue(ranges[rangeId][0] <= value);
    Assert.assertTrue(value <= ranges[rangeId][1]);
  }

  private void checkIntMV(int[][] ranges, int rangeId, int[] values, int docId, int numValuesPerMVEntry) {
    for (int i = 0; i < numValuesPerMVEntry; i++) {
      if (ranges[rangeId][0] <= values[docId * numValuesPerMVEntry + i]
          && values[docId * numValuesPerMVEntry + i] <= ranges[rangeId][1]) {
        return;
      }
    }
    Assert.fail();
  }

  private void checkLong(long[][] ranges, int rangeId, long value) {
    Assert.assertTrue(ranges[rangeId][0] <= value);
    Assert.assertTrue(value <= ranges[rangeId][1]);
  }

  private void checkLongMV(long[][] ranges, int rangeId, long[] values, int docId, int numValuesPerMVEntry) {
    for (int i = 0; i < numValuesPerMVEntry; i++) {
      if (ranges[rangeId][0] <= values[docId * numValuesPerMVEntry + i]
          && values[docId * numValuesPerMVEntry + i] <= ranges[rangeId][1]) {
        return;
      }
    }
    Assert.fail();
  }

  private void checkFloat(float[][] ranges, int rangeId, float value) {
    Assert.assertTrue(ranges[rangeId][0] <= value);
    Assert.assertTrue(value <= ranges[rangeId][1]);
  }

  private void checkFloatMV(float[][] ranges, int rangeId, float[] values, int docId,
      int numValuesPerMVEntry) {
    for (int i = 0; i < numValuesPerMVEntry; i++) {
      if (ranges[rangeId][0] <= values[docId * numValuesPerMVEntry + i]
          && values[docId * numValuesPerMVEntry + i] <= ranges[rangeId][1]) {
        return;
      }
    }
    Assert.fail();
  }

  private void checkDouble(double[][] ranges, int rangeId, double value) {
    Assert.assertTrue(ranges[rangeId][0] <= value);
    Assert.assertTrue(value <= ranges[rangeId][1]);
  }

  private void checkDoubleMV(double[][] ranges, int rangeId, double[] values, int docId,
      int numValuesPerMVEntry) {
    for (int i = 0; i < numValuesPerMVEntry; i++) {
      if (ranges[rangeId][0] <= values[docId * numValuesPerMVEntry + i]
          && values[docId * numValuesPerMVEntry + i] <= ranges[rangeId][1]) {
        return;
      }
    }
    Assert.fail();
  }


  private static Object valuesArray(DataType dataType, int numValues) {
    switch (dataType) {
      case INT:
        return new int[numValues];
      case LONG:
        return new long[numValues];
      case FLOAT:
        return new float[numValues];
      case DOUBLE:
        return new double[numValues];
      default:
        throw new IllegalArgumentException("unexpected type " + dataType);
    }
  }

  private static Object splitIntoRanges(DataType dataType, Object values, int numValuesPerRange) {
    switch (dataType) {
      case INT: {
        int[] ints = (int[]) values;
        int[] sorted = Arrays.copyOf(ints, ints.length);
        Arrays.sort(sorted);
        int[][] split = new int[ints.length / numValuesPerRange][2];
        for (int i = 0; i < split.length; ++i) {
          split[i][0] = sorted[i * numValuesPerRange];
          split[i][1] = sorted[(i + 1) * numValuesPerRange - 1];
        }
        return split;
      }
      case LONG: {
        long[] longs = (long[]) values;
        long[] sorted = Arrays.copyOf(longs, longs.length);
        Arrays.sort(sorted);
        long[][] split = new long[longs.length / numValuesPerRange][2];
        for (int i = 0; i < split.length; ++i) {
          split[i][0] = sorted[i * numValuesPerRange];
          split[i][1] = sorted[(i + 1) * numValuesPerRange - 1];
        }
        return split;
      }
      case FLOAT: {
        float[] floats = (float[]) values;
        float[] sorted = Arrays.copyOf(floats, floats.length);
        Arrays.sort(sorted);
        float[][] split = new float[floats.length / numValuesPerRange][2];
        for (int i = 0; i < split.length; ++i) {
          split[i][0] = sorted[i * numValuesPerRange];
          split[i][1] = sorted[(i + 1) * numValuesPerRange - 1];
        }
        return split;
      }
      case DOUBLE: {
        double[] doubles = (double[]) values;
        double[] sorted = Arrays.copyOf(doubles, doubles.length);
        Arrays.sort(sorted);
        double[][] split = new double[doubles.length / numValuesPerRange][2];
        for (int i = 0; i < split.length; ++i) {
          split[i][0] = sorted[i * numValuesPerRange];
          split[i][1] = sorted[(i + 1) * numValuesPerRange - 1];
        }
        return split;
      }
      default:
        throw new IllegalArgumentException("unexpected type " + dataType);
    }
  }
}
