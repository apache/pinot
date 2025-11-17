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
package org.apache.pinot.segment.local.segment.index.creator.inv;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pinot.segment.local.segment.creator.impl.inv.RawValueBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.RawValueBitmapInvertedIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class RawValueBitmapInvertedIndexTest {
  private static final int NUM_DOCS = 10000;
  private static final int NUM_VALUES = 100;
  private static final int MAX_MULTI_VALUES = 5;  // Maximum number of values per document for multi-value case
  private static final Random RANDOM = new Random(System.currentTimeMillis());

  @DataProvider(name = "dataTypes")
  public Object[][] dataTypes() {
    return new Object[][]{
        {DataType.INT},
        {DataType.LONG},
        {DataType.FLOAT},
        {DataType.DOUBLE},
        {DataType.STRING}
    };
  }

  private List<Object> generateValues(DataType dataType) {
    switch (dataType) {
      case INT:
        return IntStream.range(0, NUM_VALUES)
            .mapToObj(i -> RANDOM.nextInt(100000))
            .collect(Collectors.toList());
      case LONG:
        return IntStream.range(0, NUM_VALUES)
            .mapToObj(i -> RANDOM.nextLong())
            .collect(Collectors.toList());
      case FLOAT:
        return IntStream.range(0, NUM_VALUES)
            .mapToObj(i -> RANDOM.nextFloat() * 100000)
            .collect(Collectors.toList());
      case DOUBLE:
        return IntStream.range(0, NUM_VALUES)
            .mapToObj(i -> RANDOM.nextDouble() * 100000)
            .collect(Collectors.toList());
      case STRING:
        return IntStream.range(0, NUM_VALUES)
            .mapToObj(i -> "value_" + RANDOM.nextInt(100000))
            .collect(Collectors.toList());
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }

  private Object[] generateMultiValue(DataType dataType, List<Object> values) {
    int numValues = 1 + RANDOM.nextInt(MAX_MULTI_VALUES); // At least one value
    Object[] multiValue = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      multiValue[i] = values.get(RANDOM.nextInt(values.size()));
    }
    return multiValue;
  }

  private Object[] convertToTypedArray(Object[] values, DataType dataType) {
    switch (dataType) {
      case INT:
        int[] intValues = new int[values.length];
        for (int i = 0; i < values.length; i++) {
          intValues[i] = (Integer) values[i];
        }
        return new Object[]{intValues, values.length};
      case LONG:
        long[] longValues = new long[values.length];
        for (int i = 0; i < values.length; i++) {
          longValues[i] = (Long) values[i];
        }
        return new Object[]{longValues, values.length};
      case FLOAT:
        float[] floatValues = new float[values.length];
        for (int i = 0; i < values.length; i++) {
          floatValues[i] = (Float) values[i];
        }
        return new Object[]{floatValues, values.length};
      case DOUBLE:
        double[] doubleValues = new double[values.length];
        for (int i = 0; i < values.length; i++) {
          doubleValues[i] = (Double) values[i];
        }
        return new Object[]{doubleValues, values.length};
      case STRING:
        String[] stringValues = new String[values.length];
        for (int i = 0; i < values.length; i++) {
          stringValues[i] = (String) values[i];
        }
        return new Object[]{stringValues, values.length};
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }

  @Test(dataProvider = "dataTypes")
  public void testSingleValueInvertedIndex(DataType dataType) throws IOException {
    List<Object> values = generateValues(dataType);
    Map<Object, Set<Integer>> valueToDocIds = new HashMap<>();
    File indexDir = Files.createTempDirectory("inverted-index").toFile();
    indexDir.deleteOnExit();
    File indexFile = new File(indexDir, "col" + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    indexFile.deleteOnExit();

    // Create index
    try (RawValueBitmapInvertedIndexCreator creator =
        new RawValueBitmapInvertedIndexCreator(dataType, "col", indexDir)) {
      // Generate random docIds for each value
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        Object value = values.get(RANDOM.nextInt(values.size()));
        // Add the value based on data type
        switch (dataType) {
          case INT:
            creator.add((Integer) value);
            break;
          case LONG:
            creator.add((Long) value);
            break;
          case FLOAT:
            creator.add((Float) value);
            break;
          case DOUBLE:
            creator.add((Double) value);
            break;
          case STRING:
            creator.add((String) value);
            break;
          default:
            throw new IllegalStateException("Unsupported data type: " + dataType);
        }
        valueToDocIds.computeIfAbsent(value, k -> new HashSet<>()).add(docId);
      }
    }

    // Verify index
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(indexFile, true, 0,
        indexFile.length(), ByteOrder.BIG_ENDIAN, "")) {
      try (RawValueBitmapInvertedIndexReader reader =
          new RawValueBitmapInvertedIndexReader(dataBuffer, dataType)) {
        // Test all values
        for (Object value : values) {
          ImmutableRoaringBitmap actualBitmap = getBitmap(reader, value);
          Set<Integer> expectedDocIds = valueToDocIds.getOrDefault(value, Collections.emptySet());

          // Convert bitmap to docIds for comparison
          Set<Integer> actualDocIds = new HashSet<>();
          actualBitmap.forEach((int docId) -> actualDocIds.add(docId));

          Assert.assertEquals(actualDocIds, expectedDocIds, "DocIds mismatch for value: " + value);
        }

        // Test non-existent values
        Object nonExistentValue = getNonExistentValue(dataType);
        ImmutableRoaringBitmap bitmap = getBitmap(reader, nonExistentValue);
        Assert.assertTrue(bitmap.isEmpty(), "Bitmap should be empty for non-existent value");
      }
    }
  }

  @Test(dataProvider = "dataTypes")
  public void testMultiValueInvertedIndex(DataType dataType) throws IOException {
    List<Object> values = generateValues(dataType);
    Map<Object, Set<Integer>> valueToDocIds = new HashMap<>();
    File indexDir = Files.createTempDirectory("inverted-index").toFile();
    indexDir.deleteOnExit();
    File indexFile = new File(indexDir, "col" + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    indexFile.deleteOnExit();

    // Create index
    try (RawValueBitmapInvertedIndexCreator creator =
        new RawValueBitmapInvertedIndexCreator(dataType, "col", indexDir)) {
      // Generate random multi-values for each docId
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        Object[] multiValue = generateMultiValue(dataType, values);
        Object[] typedArray = convertToTypedArray(multiValue, dataType);
        // Add the multi-value array based on data type
        switch (dataType) {
          case INT:
            creator.add((int[]) typedArray[0], (int) typedArray[1]);
            break;
          case LONG:
            creator.add((long[]) typedArray[0], (int) typedArray[1]);
            break;
          case FLOAT:
            creator.add((float[]) typedArray[0], (int) typedArray[1]);
            break;
          case DOUBLE:
            creator.add((double[]) typedArray[0], (int) typedArray[1]);
            break;
          case STRING:
            creator.add((String[]) typedArray[0], (int) typedArray[1]);
            break;
          default:
            throw new IllegalStateException("Unsupported data type: " + dataType);
        }

        // Record which docId contains which values
        for (Object value : multiValue) {
          valueToDocIds.computeIfAbsent(value, k -> new HashSet<>()).add(docId);
        }
      }
    }

    // Verify index
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(indexFile, true, 0,
        indexFile.length(), ByteOrder.BIG_ENDIAN, "")) {
      try (RawValueBitmapInvertedIndexReader reader =
          new RawValueBitmapInvertedIndexReader(dataBuffer, dataType)) {
        // Test all values
        for (Object value : values) {
          ImmutableRoaringBitmap actualBitmap = getBitmap(reader, value);
          Set<Integer> expectedDocIds = valueToDocIds.getOrDefault(value, Collections.emptySet());

          // Convert bitmap to docIds for comparison
          Set<Integer> actualDocIds = new HashSet<>();
          actualBitmap.forEach((int docId) -> actualDocIds.add(docId));

          Assert.assertEquals(actualDocIds, expectedDocIds,
              String.format("DocIds mismatch for value: %s, expected: %s, actual: %s",
                  value, expectedDocIds, actualDocIds));
        }

        // Test non-existent values
        Object nonExistentValue = getNonExistentValue(dataType);
        ImmutableRoaringBitmap bitmap = getBitmap(reader, nonExistentValue);
        Assert.assertTrue(bitmap.isEmpty(), "Bitmap should be empty for non-existent value");
      }
    }
  }

  private ImmutableRoaringBitmap getBitmap(RawValueBitmapInvertedIndexReader reader, Object value) {
    switch (value.getClass().getSimpleName()) {
      case "Integer":
        return reader.getDocIdsForInt((Integer) value);
      case "Long":
        return reader.getDocIdsForLong((Long) value);
      case "Float":
        return reader.getDocIdsForFloat((Float) value);
      case "Double":
        return reader.getDocIdsForDouble((Double) value);
      case "String":
        return reader.getDocIdsForString((String) value);
      default:
        throw new IllegalStateException("Unsupported value type: " + value.getClass());
    }
  }

  private Object getNonExistentValue(DataType dataType) {
    switch (dataType) {
      case INT:
        return Integer.MAX_VALUE;
      case LONG:
        return Long.MAX_VALUE;
      case FLOAT:
        return Float.MAX_VALUE;
      case DOUBLE:
        return Double.MAX_VALUE;
      case STRING:
        return "non_existent_value";
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }
}
