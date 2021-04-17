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
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.inv.RangeIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.RangeIndexReader;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.segment.creator.impl.V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;


public class RangeIndexCreatorTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "RangeIndexCreatorTest");
  private static final Random RANDOM = new Random();
  private static final String COLUMN_NAME = "testColumn";

  @BeforeClass
  public void setUp() throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @Test
  public void testInt() throws Exception {
    testDataType(DataType.INT);
  }

  @Test
  public void testLong() throws Exception {
    testDataType(DataType.LONG);
  }

  @Test
  public void testFloat() throws Exception {
    testDataType(DataType.FLOAT);
  }

  @Test
  public void testDouble() throws Exception {
    testDataType(DataType.DOUBLE);
  }

  @Test
  public void testIntMV() throws Exception {
    testDataTypeMV(DataType.INT);
  }

  @Test
  public void testLongMV() throws Exception {
    testDataTypeMV(DataType.LONG);
  }

  @Test
  public void testFloatMV() throws Exception {
    testDataTypeMV(DataType.FLOAT);
  }

  @Test
  public void testDoubleMV() throws Exception {
    testDataTypeMV(DataType.DOUBLE);
  }

  @AfterClass
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  private void testDataType(DataType dataType) throws IOException {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, dataType, true);
    int numDocs = 1000;
    Number[] values = new Number[numDocs];

    try (RangeIndexCreator creator = new RangeIndexCreator(INDEX_DIR, fieldSpec, dataType, -1, -1, numDocs, numDocs)) {
      addDataToIndexer(dataType, numDocs, 1, creator, values);
      creator.seal();
    }

    File rangeIndexFile = new File(INDEX_DIR, COLUMN_NAME + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(rangeIndexFile)) {
      RangeIndexReader rangeIndexReader = new RangeIndexReader(dataBuffer);
      Number[] rangeStartArray = rangeIndexReader.getRangeStartArray();
      for (int rangeId = 0; rangeId < rangeStartArray.length; rangeId++) {
        ImmutableRoaringBitmap bitmap = rangeIndexReader.getDocIds(rangeId);
        for (int docId : bitmap.toArray()) {
          checkValueForDocId(dataType, values, rangeStartArray, rangeId, docId, 1);
        }
      }
    }

    FileUtils.forceDelete(rangeIndexFile);
  }

  private void testDataTypeMV(DataType dataType) throws IOException {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, dataType, false);
    int numDocs = 1000;
    int numValuesPerMVEntry = 10;
    int numValues = numDocs * numValuesPerMVEntry;
    Number[] values = new Number[numValues];

    try (
        RangeIndexCreator creator = new RangeIndexCreator(INDEX_DIR, fieldSpec, dataType, -1, -1, numDocs, numValues)) {
      addDataToIndexer(dataType, numDocs, numValuesPerMVEntry, creator, values);
      creator.seal();
    }

    File rangeIndexFile = new File(INDEX_DIR, COLUMN_NAME + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(rangeIndexFile)) {
      RangeIndexReader rangeIndexReader = new RangeIndexReader(dataBuffer);
      Number[] rangeStartArray = rangeIndexReader.getRangeStartArray();
      int numRanges = rangeStartArray.length;
      for (int rangeId = 0; rangeId < numRanges; rangeId++) {
        ImmutableRoaringBitmap bitmap = rangeIndexReader.getDocIds(rangeId);
        for (int docId : bitmap.toArray()) {
          checkValueForDocId(dataType, values, rangeStartArray, rangeId, docId, numValuesPerMVEntry);
        }
      }
    }

    FileUtils.forceDelete(rangeIndexFile);
  }

  private void addDataToIndexer(DataType dataType, int numDocs, int numValuesPerEntry, RangeIndexCreator creator,
      Number[] values) {
    switch (dataType) {
      case INT:
        if (numValuesPerEntry == 1) {
          for (int i = 0; i < numDocs; i++) {
            int value = RANDOM.nextInt();
            values[i] = value;
            creator.add(value);
          }
        } else {
          int[] intValues = new int[numValuesPerEntry];
          for (int i = 0; i < numDocs; i++) {
            for (int j = 0; j < numValuesPerEntry; j++) {
              int value = RANDOM.nextInt();
              intValues[j] = value;
              values[i * numValuesPerEntry + j] = value;
            }
            creator.add(intValues, numValuesPerEntry);
          }
        }
        break;
      case LONG:
        if (numValuesPerEntry == 1) {
          for (int i = 0; i < numDocs; i++) {
            long value = RANDOM.nextLong();
            values[i] = value;
            creator.add(value);
          }
        } else {
          long[] longValues = new long[numValuesPerEntry];
          for (int i = 0; i < numDocs; i++) {
            for (int j = 0; j < numValuesPerEntry; j++) {
              long value = RANDOM.nextLong();
              longValues[j] = value;
              values[i * numValuesPerEntry + j] = value;
            }
            creator.add(longValues, numValuesPerEntry);
          }
        }
        break;
      case FLOAT:
        if (numValuesPerEntry == 1) {
          for (int i = 0; i < numDocs; i++) {
            float value = RANDOM.nextFloat();
            values[i] = value;
            creator.add(value);
          }
        } else {
          float[] floatValues = new float[numValuesPerEntry];
          for (int i = 0; i < numDocs; i++) {
            for (int j = 0; j < numValuesPerEntry; j++) {
              float value = RANDOM.nextFloat();
              floatValues[j] = value;
              values[i * numValuesPerEntry + j] = value;
            }
            creator.add(floatValues, numValuesPerEntry);
          }
        }
        break;
      case DOUBLE:
        if (numValuesPerEntry == 1) {
          for (int i = 0; i < numDocs; i++) {
            double value = RANDOM.nextDouble();
            values[i] = value;
            creator.add(value);
          }
        } else {
          double[] doubleValues = new double[numValuesPerEntry];
          for (int i = 0; i < numDocs; i++) {
            for (int j = 0; j < numValuesPerEntry; j++) {
              double value = RANDOM.nextDouble();
              doubleValues[j] = value;
              values[i * numValuesPerEntry + j] = value;
            }
            creator.add(doubleValues, numValuesPerEntry);
          }
        }
        break;
      default:
        throw new IllegalStateException();
    }
  }

  private void checkValueForDocId(DataType dataType, Number[] values, Number[] rangeStartArray, int rangeId, int docId,
      int numValuesPerEntry) {
    switch (dataType) {
      case INT:
        if (numValuesPerEntry == 1) {
          checkInt(rangeStartArray, rangeId, values[docId].intValue());
        } else {
          checkIntMV(rangeStartArray, rangeId, values, docId, numValuesPerEntry);
        }
        break;
      case LONG:
        if (numValuesPerEntry == 1) {
          checkLong(rangeStartArray, rangeId, values[docId].longValue());
        } else {
          checkLongMV(rangeStartArray, rangeId, values, docId, numValuesPerEntry);
        }
        break;
      case FLOAT:
        if (numValuesPerEntry == 1) {
          checkFloat(rangeStartArray, rangeId, values[docId].floatValue());
        } else {
          checkFloatMV(rangeStartArray, rangeId, values, docId, numValuesPerEntry);
        }
        break;
      case DOUBLE:
        if (numValuesPerEntry == 1) {
          checkDouble(rangeStartArray, rangeId, values[docId].doubleValue());
        } else {
          checkDoubleMV(rangeStartArray, rangeId, values, docId, numValuesPerEntry);
        }
        break;
      default:
        throw new IllegalStateException();
    }
  }

  private void checkInt(Number[] rangeStartArray, int rangeId, int value) {
    Assert.assertTrue(rangeStartArray[rangeId].intValue() <= value);
    if (rangeId != rangeStartArray.length - 1) {
      Assert.assertTrue(value < rangeStartArray[rangeId + 1].intValue());
    }
  }

  private void checkIntMV(Number[] rangeStartArray, int rangeId, Number[] values, int docId, int numValuesPerMVEntry) {
    if (rangeId != rangeStartArray.length - 1) {
      for (int i = 0; i < numValuesPerMVEntry; i++) {
        if (rangeStartArray[rangeId].intValue() <= values[docId * numValuesPerMVEntry + i].intValue()
            && values[docId * numValuesPerMVEntry + i].intValue() < rangeStartArray[rangeId + 1].intValue()) {
          return;
        }
      }
    } else {
      for (int i = 0; i < numValuesPerMVEntry; i++) {
        if (rangeStartArray[rangeId].intValue() <= values[docId * numValuesPerMVEntry + i].intValue()) {
          return;
        }
      }
    }
    Assert.fail();
  }

  private void checkLong(Number[] rangeStartArray, int rangeId, long value) {
    Assert.assertTrue(rangeStartArray[rangeId].longValue() <= value);
    if (rangeId != rangeStartArray.length - 1) {
      Assert.assertTrue(value < rangeStartArray[rangeId + 1].longValue());
    }
  }

  private void checkLongMV(Number[] rangeStartArray, int rangeId, Number[] values, int docId, int numValuesPerMVEntry) {
    if (rangeId != rangeStartArray.length - 1) {
      for (int i = 0; i < numValuesPerMVEntry; i++) {
        if (rangeStartArray[rangeId].longValue() <= values[docId * numValuesPerMVEntry + i].longValue()
            && values[docId * numValuesPerMVEntry + i].longValue() < rangeStartArray[rangeId + 1].longValue()) {
          return;
        }
      }
    } else {
      for (int i = 0; i < numValuesPerMVEntry; i++) {
        if (rangeStartArray[rangeId].longValue() <= values[docId * numValuesPerMVEntry + i].longValue()) {
          return;
        }
      }
    }
    Assert.fail();
  }

  private void checkFloat(Number[] rangeStartArray, int rangeId, float value) {
    Assert.assertTrue(rangeStartArray[rangeId].floatValue() <= value);
    if (rangeId != rangeStartArray.length - 1) {
      Assert.assertTrue(value < rangeStartArray[rangeId + 1].floatValue());
    }
  }

  private void checkFloatMV(Number[] rangeStartArray, int rangeId, Number[] values, int docId,
      int numValuesPerMVEntry) {
    if (rangeId != rangeStartArray.length - 1) {
      for (int i = 0; i < numValuesPerMVEntry; i++) {
        if (rangeStartArray[rangeId].floatValue() <= values[docId * numValuesPerMVEntry + i].floatValue()
            && values[docId * numValuesPerMVEntry + i].floatValue() < rangeStartArray[rangeId + 1].floatValue()) {
          return;
        }
      }
    } else {
      for (int i = 0; i < numValuesPerMVEntry; i++) {
        if (rangeStartArray[rangeId].floatValue() <= values[docId * numValuesPerMVEntry + i].floatValue()) {
          return;
        }
      }
    }
    Assert.fail();
  }

  private void checkDouble(Number[] rangeStartArray, int rangeId, double value) {
    Assert.assertTrue(rangeStartArray[rangeId].doubleValue() <= value);
    if (rangeId != rangeStartArray.length - 1) {
      Assert.assertTrue(value < rangeStartArray[rangeId + 1].doubleValue());
    }
  }

  private void checkDoubleMV(Number[] rangeStartArray, int rangeId, Number[] values, int docId,
      int numValuesPerMVEntry) {
    if (rangeId != rangeStartArray.length - 1) {
      for (int i = 0; i < numValuesPerMVEntry; i++) {
        if (rangeStartArray[rangeId].doubleValue() <= values[docId * numValuesPerMVEntry + i].doubleValue()
            && values[docId * numValuesPerMVEntry + i].doubleValue() < rangeStartArray[rangeId + 1].doubleValue()) {
          return;
        }
      }
    } else {
      for (int i = 0; i < numValuesPerMVEntry; i++) {
        if (rangeStartArray[rangeId].doubleValue() <= values[docId * numValuesPerMVEntry + i].doubleValue()) {
          return;
        }
      }
    }
    Assert.fail();
  }
}
