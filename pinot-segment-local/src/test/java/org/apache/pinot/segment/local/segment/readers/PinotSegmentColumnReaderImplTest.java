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
package org.apache.pinot.segment.local.segment.readers;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.ColumnarSegmentBuildingTestBase;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Comprehensive tests for PinotSegmentColumnReaderImpl random access methods.
 *
 * <p>This test validates:
 * <ul>
 *   <li>Single-value accessor methods (getInt, getLong, getFloat, getDouble, getString, getBytes)</li>
 *   <li>Multi-value accessor methods (getIntMV, getLongMV, getFloatMV, getDoubleMV, getStringMV, getBytesMV)</li>
 *   <li>Metadata methods (getTotalDocs, isNull)</li>
 *   <li>Boundary conditions and exception handling</li>
 *   <li>Consistency between iterator and random access patterns</li>
 *   <li>Multiple readers for the same column</li>
 *   <li>Both dictionary-encoded and raw forward index columns</li>
 * </ul>
 */
public class PinotSegmentColumnReaderImplTest extends ColumnarSegmentBuildingTestBase {

  /**
   * Enum representing different segment index configurations for testing.
   */
  enum IndexType {
    DICT_ENCODED,
    RAW_INDEX,
    NULLABLE
  }

  /**
   * Functional interface for single-value getters that can throw IOException.
   */
  @FunctionalInterface
  interface SingleValueGetter {
    Object get(PinotSegmentColumnReaderImpl reader, int docId)
        throws IOException;
  }

  /**
   * Functional interface for single-value sequential getters that can throw IOException.
   */
  @FunctionalInterface
  interface SingleValueSequentialGetter {
    Object get(PinotSegmentColumnReaderImpl reader)
        throws IOException;
  }

  /**
   * Functional interface for multi-value getters that can throw IOException.
   */
  @FunctionalInterface
  interface MultiValueGetter {
    Object get(PinotSegmentColumnReaderImpl reader, int docId)
        throws IOException;
  }

  /**
   * Functional interface for multi-value sequential getters that can throw IOException.
   */
  @FunctionalInterface
  interface MultiValueSequentialGetter {
    Object get(PinotSegmentColumnReaderImpl reader)
        throws IOException;
  }

  private ImmutableSegment _dictEncodedSegment;
  private ImmutableSegment _rawIndexSegment;
  private ImmutableSegment _nullableSegment;

  @BeforeClass
  @Override
  public void setUp()
      throws IOException {
    super.setUp();
    // Create test segments with different configurations
    try {
      // Dictionary-encoded segment (default)
      File dictEncodedSegmentDir = createRowMajorSegment();
      _dictEncodedSegment = ImmutableSegmentLoader.load(dictEncodedSegmentDir, ReadMode.mmap);

      // Raw forward index segment (no dictionary)
      TableConfig noDictConfig = createTableConfigWithNoDictionary();
      File rawIndexSegmentDir = createRowMajorSegmentWithConfig(noDictConfig, _originalSchema, "rawIndexSegment");
      _rawIndexSegment = ImmutableSegmentLoader.load(rawIndexSegmentDir, ReadMode.mmap);

      // Nullable segment (allows null values)
      Schema nullableSchema = createNullableSchema();
      File nullableSegmentDir = createRowMajorSegmentWithConfig(_tableConfig, nullableSchema, "nullableSegment");
      _nullableSegment = ImmutableSegmentLoader.load(nullableSegmentDir, ReadMode.mmap);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public void tearDownTest()
      throws Exception {
    if (_dictEncodedSegment != null) {
      _dictEncodedSegment.destroy();
    }
    if (_rawIndexSegment != null) {
      _rawIndexSegment.destroy();
    }
    if (_nullableSegment != null) {
      _nullableSegment.destroy();
    }
    super.tearDown();
  }

  @Test
  public void testGetTotalDocs()
      throws Exception {
    PinotSegmentColumnReaderImpl reader = new PinotSegmentColumnReaderImpl(_dictEncodedSegment, INT_COL_1);

    Assert.assertEquals(reader.getTotalDocs(), _testData.size());
    Assert.assertEquals(reader.getTotalDocs(), _dictEncodedSegment.getSegmentMetadata().getTotalDocs());

    reader.close();
  }

  /**
   * Helper method to get the appropriate segment and schema based on IndexType.
   */
  private ImmutableSegment getSegment(IndexType indexType) {
    switch (indexType) {
      case DICT_ENCODED:
        return _dictEncodedSegment;
      case RAW_INDEX:
        return _rawIndexSegment;
      case NULLABLE:
        return _nullableSegment;
      default:
        throw new IllegalArgumentException("Unknown index type: " + indexType);
    }
  }

  /**
   * Helper method to get the appropriate schema based on IndexType.
   */
  private Schema getSchema(IndexType indexType) {
    switch (indexType) {
      case DICT_ENCODED:
      case RAW_INDEX:
        return _originalSchema;
      case NULLABLE:
        return createNullableSchema();
      default:
        throw new IllegalArgumentException("Unknown index type: " + indexType);
    }
  }

  /**
   * Data provider for single-value accessor methods.
   * @return test parameters: column name, random access getter, sequential getter,
   * value converter function (required for float), index type
   */
  @DataProvider(name = "singleValueAccessorProvider")
  public Object[][] singleValueAccessorProvider() {
    // Base column configurations
    Object[][] baseConfigs = new Object[][] {
        {INT_COL_1, (SingleValueGetter) PinotSegmentColumnReaderImpl::getInt,
            (SingleValueSequentialGetter) PinotSegmentColumnReaderImpl::nextInt, null},
        {LONG_COL, (SingleValueGetter) PinotSegmentColumnReaderImpl::getLong,
            (SingleValueSequentialGetter) PinotSegmentColumnReaderImpl::nextLong, null},
        {FLOAT_COL, (SingleValueGetter) PinotSegmentColumnReaderImpl::getFloat,
            (SingleValueSequentialGetter) PinotSegmentColumnReaderImpl::nextFloat,
            (Function<Object, Object>) val -> val instanceof Double ? ((Double) val).floatValue() : val},
        {DOUBLE_COL, (SingleValueGetter) PinotSegmentColumnReaderImpl::getDouble,
            (SingleValueSequentialGetter) PinotSegmentColumnReaderImpl::nextDouble, null},
        {STRING_COL_1, (SingleValueGetter) PinotSegmentColumnReaderImpl::getString, null, null},
        {BYTES_COL, (SingleValueGetter) PinotSegmentColumnReaderImpl::getBytes, null, null}
    };

    // Create test cases for all index types
    IndexType[] indexTypes = IndexType.values();
    Object[][] result = new Object[baseConfigs.length * indexTypes.length][];
    int idx = 0;
    for (Object[] baseConfig : baseConfigs) {
      for (IndexType indexType : indexTypes) {
        result[idx++] = new Object[]{baseConfig[0], baseConfig[1], baseConfig[2], baseConfig[3], indexType};
      }
    }
    return result;
  }

  @Test(dataProvider = "singleValueAccessorProvider")
  public void testSingleValueAccessors(String columnName, SingleValueGetter randomAccessGetter,
      SingleValueSequentialGetter sequentialGetter, Function<Object, Object> valueConverter, IndexType indexType)
      throws Exception {
    ImmutableSegment segment = getSegment(indexType);
    Schema schema = getSchema(indexType);
    PinotSegmentColumnReaderImpl reader = new PinotSegmentColumnReaderImpl(segment, columnName);

    try {
      int totalDocs = reader.getTotalDocs();
      Assert.assertEquals(totalDocs, _testData.size());

      // Get default value for the column type
      Object defaultValue = schema.getFieldSpecFor(columnName).getDefaultNullValue();

      // Test reading all documents using random access (Pattern 3)
      for (int docId = 0; docId < totalDocs; docId++) {
        GenericRow expectedRow = _testData.get(docId);
        Object expectedValue = expectedRow.getValue(columnName);

        // For nullable segments, check if the value is actually null
        if (indexType == IndexType.NULLABLE && expectedValue == null) {
          Assert.assertTrue(reader.isNull(docId),
              "isNull() should return true for null value at docId " + docId);
          continue;
        }

        Object actualValue = randomAccessGetter.get(reader, docId);

        if (expectedValue == null) {
          // Null values are replaced with default during segment creation (non-nullable)
          if (actualValue instanceof byte[]) {
            Assert.assertEquals(actualValue, defaultValue, "Null should be replaced with default at docId " + docId);
          } else if (actualValue instanceof Double) {
            Assert.assertEquals((Double) actualValue, (Double) defaultValue, 0.0001,
                "Null should be replaced with default at docId " + docId);
          } else if (actualValue instanceof Float) {
            Assert.assertEquals((Float) actualValue, (Float) defaultValue, 0.0001f,
                "Null should be replaced with default at docId " + docId);
          } else {
            Assert.assertEquals(actualValue, defaultValue, "Null should be replaced with default at docId " + docId);
          }
        } else {
          // Convert expected value if converter is provided
          Object convertedExpectedValue = valueConverter != null ? valueConverter.apply(expectedValue) : expectedValue;
          if (actualValue instanceof Double) {
            Assert.assertEquals((Double) actualValue, (Double) convertedExpectedValue, 0.0001,
                "Value mismatch at docId " + docId);
          } else if (actualValue instanceof Float) {
            Assert.assertEquals((Float) actualValue, (Float) convertedExpectedValue, 0.0001f,
                "Value mismatch at docId " + docId);
          } else {
            Assert.assertEquals(actualValue, convertedExpectedValue, "Value mismatch at docId " + docId);
          }
        }
      }

      // Test sequential iteration with type-specific methods (Pattern 2) - only for columns with sequential getters
      if (sequentialGetter != null) {
        reader.rewind();
        int docId = 0;
        while (reader.hasNext()) {
          GenericRow expectedRow = _testData.get(docId);
          Object expectedValue = expectedRow.getValue(columnName);

          if (indexType == IndexType.NULLABLE && expectedValue == null) {
            Assert.assertTrue(reader.isNextNull(),
                "isNextNull() should return true for null value at docId " + docId);
            reader.skipNext();
          } else if (reader.isNextNull()) {
            reader.skipNext();
            // For non-nullable segments, null is replaced with default
            Assert.assertNull(expectedValue, "Expected null value at docId " + docId);
          } else {
            Object actualValue = sequentialGetter.get(reader);

            if (expectedValue == null) {
              // Null values are replaced with default during segment creation (non-nullable)
              if (actualValue instanceof byte[]) {
                Assert.assertEquals(actualValue, defaultValue,
                    "Null should be replaced with default at docId " + docId + " (sequential)");
              } else if (actualValue instanceof Double) {
                Assert.assertEquals((Double) actualValue, (Double) defaultValue, 0.0001,
                    "Null should be replaced with default at docId " + docId + " (sequential)");
              } else if (actualValue instanceof Float) {
                Assert.assertEquals((Float) actualValue, (Float) defaultValue, 0.0001f,
                    "Null should be replaced with default at docId " + docId + " (sequential)");
              } else {
                Assert.assertEquals(actualValue, defaultValue,
                    "Null should be replaced with default at docId " + docId + " (sequential)");
              }
            } else {
              // Convert expected value if converter is provided
              Object convertedValue = valueConverter != null ? valueConverter.apply(expectedValue) : expectedValue;
              if (actualValue instanceof Double) {
                Assert.assertEquals((Double) actualValue, (Double) convertedValue, 0.0001,
                    "Value mismatch at docId " + docId + " (sequential)");
              } else if (actualValue instanceof Float) {
                Assert.assertEquals((Float) actualValue, (Float) convertedValue, 0.0001f,
                    "Value mismatch at docId " + docId + " (sequential)");
              } else {
                Assert.assertEquals(actualValue, convertedValue,
                    "Value mismatch at docId " + docId + " (sequential)");
              }
            }
          }
          docId++;
        }
        Assert.assertEquals(docId, totalDocs, "Should have iterated through all documents");
      }
    } finally {
      reader.close();
    }
  }


  /**
   * Data provider for multi-value accessor methods.
   * @return test parameters: column name, random access getter function,
   * sequential getter function, array converter function, index type
   */
  @DataProvider(name = "multiValueAccessorProvider")
  public Object[][] multiValueAccessorProvider() {
    // Base column configurations
    Object[][] baseConfigs = new Object[][] {
        {MV_INT_COL, (MultiValueGetter) PinotSegmentColumnReaderImpl::getIntMV,
            (MultiValueSequentialGetter) PinotSegmentColumnReaderImpl::nextIntMV, null},
        {MV_LONG_COL, (MultiValueGetter) PinotSegmentColumnReaderImpl::getLongMV,
            (MultiValueSequentialGetter) PinotSegmentColumnReaderImpl::nextLongMV, null},
        {MV_FLOAT_COL, (MultiValueGetter) PinotSegmentColumnReaderImpl::getFloatMV,
            (MultiValueSequentialGetter) PinotSegmentColumnReaderImpl::nextFloatMV,
            (Function<Object[], Object>) expectedArray -> {
              float[] expectedFloatArray = new float[expectedArray.length];
              for (int i = 0; i < expectedArray.length; i++) {
                expectedFloatArray[i] = (Float) expectedArray[i];
              }
              return expectedFloatArray;
            } },
        {MV_DOUBLE_COL, (MultiValueGetter) PinotSegmentColumnReaderImpl::getDoubleMV,
            (MultiValueSequentialGetter) PinotSegmentColumnReaderImpl::nextDoubleMV, null},
        {MV_STRING_COL, (MultiValueGetter) PinotSegmentColumnReaderImpl::getStringMV,
            (MultiValueSequentialGetter) PinotSegmentColumnReaderImpl::nextStringMV, null},
        {MV_BYTES_COL, (MultiValueGetter) PinotSegmentColumnReaderImpl::getBytesMV,
            (MultiValueSequentialGetter) PinotSegmentColumnReaderImpl::nextBytesMV, null},
    };

    // Create test cases for all index types
    IndexType[] indexTypes = IndexType.values();
    Object[][] result = new Object[baseConfigs.length * indexTypes.length][];
    int idx = 0;
    for (Object[] baseConfig : baseConfigs) {
      for (IndexType indexType : indexTypes) {
        result[idx++] = new Object[]{baseConfig[0], baseConfig[1], baseConfig[2], baseConfig[3], indexType};
      }
    }
    return result;
  }

  @Test(dataProvider = "multiValueAccessorProvider")
  public void testMultiValueAccessors(String columnName, MultiValueGetter randomAccessGetter,
      MultiValueSequentialGetter sequentialGetter, Function<Object[], Object> arrayConverter, IndexType indexType)
      throws Exception {
    ImmutableSegment segment = getSegment(indexType);
    Schema schema = getSchema(indexType);
    PinotSegmentColumnReaderImpl reader = new PinotSegmentColumnReaderImpl(segment, columnName);

    try {
      int totalDocs = reader.getTotalDocs();

      // Get default value for MV type (wrapped in array)
      FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
      Object defaultNullValue = fieldSpec.getDefaultNullValue();
      Object defaultValue;

      // Create default array based on type
      switch (fieldSpec.getDataType()) {
        case INT:
          defaultValue = new int[]{((Number) defaultNullValue).intValue()};
          break;
        case LONG:
          defaultValue = new long[]{((Number) defaultNullValue).longValue()};
          break;
        case FLOAT:
          defaultValue = new float[]{((Number) defaultNullValue).floatValue()};
          break;
        case DOUBLE:
          defaultValue = new double[]{((Number) defaultNullValue).doubleValue()};
          break;
        case STRING:
          defaultValue = new String[]{(String) defaultNullValue};
          break;
        case BYTES:
          defaultValue = new byte[][]{(byte[]) defaultNullValue};
          break;
        default:
          throw new IllegalArgumentException("Unsupported data type: " + fieldSpec.getDataType());
      }

      // Test reading all documents using random access
      for (int docId = 0; docId < totalDocs; docId++) {
        GenericRow expectedRow = _testData.get(docId);
        Object expectedValue = expectedRow.getValue(columnName);

        // For nullable segments, check if the value is actually null
        if (indexType == IndexType.NULLABLE && expectedValue == null) {
          Assert.assertTrue(reader.isNull(docId),
              "isNull() should return true for null value at docId " + docId);
          continue;
        }

        Object actualValue = randomAccessGetter.get(reader, docId);

        if (expectedValue == null) {
          // Null values are replaced with default array during segment creation (non-nullable)
          Assert.assertEquals(actualValue, defaultValue,
              "Null should be replaced with default array at docId " + docId);
        } else {
          // Convert expected Object[] to appropriate type array for comparison
          Object[] expectedArray = (Object[]) expectedValue;
          Object expectedConvertedArray = arrayConverter != null ? arrayConverter.apply(expectedArray) : expectedArray;

          Assert.assertEquals(actualValue, expectedConvertedArray,
              "MV array mismatch at docId " + docId);
        }
      }

      // Test sequential iteration with type-specific MV methods
      reader.rewind();
      int docId = 0;
      while (reader.hasNext()) {
        GenericRow expectedRow = _testData.get(docId);
        Object expectedValue = expectedRow.getValue(columnName);

        if (indexType == IndexType.NULLABLE && expectedValue == null) {
          Assert.assertTrue(reader.isNextNull(),
              "isNextNull() should return true for null value at docId " + docId);
          reader.skipNext();
        } else if (reader.isNextNull()) {
          reader.skipNext();
          // For non-nullable segments, null is replaced with default
          Assert.assertNull(expectedValue, "Expected null value at docId " + docId);
        } else {
          Object actualValue = sequentialGetter.get(reader);

          if (expectedValue == null) {
            // Null values are replaced with default array during segment creation (non-nullable)
            Assert.assertEquals(actualValue, defaultValue,
                "Null should be replaced with default array at docId " + docId + " (sequential)");
          } else {
            // Convert expected Object[] to appropriate type array for comparison
            Object[] expectedArray = (Object[]) expectedValue;
            Object convertedArray = arrayConverter != null ? arrayConverter.apply(expectedArray) : expectedArray;

            Assert.assertEquals(actualValue, convertedArray,
                "MV array mismatch at docId " + docId + " (sequential)");
          }
        }
        docId++;
      }

      Assert.assertEquals(docId, totalDocs, "Should have iterated through all documents");
    } finally {
      reader.close();
    }
  }

  @Test
  public void testIsNull()
      throws Exception {
    PinotSegmentColumnReaderImpl reader = new PinotSegmentColumnReaderImpl(_dictEncodedSegment, STRING_COL_1);

    try {
      int totalDocs = reader.getTotalDocs();

      // In Pinot segments, nulls are replaced with defaults, so isNull should always return false
      for (int docId = 0; docId < totalDocs; docId++) {
        boolean actualNull = reader.isNull(docId);
        Assert.assertFalse(actualNull, "isNull() should return false (nulls replaced with defaults) at docId " + docId);
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testBoundaryConditions()
      throws Exception {
    PinotSegmentColumnReaderImpl reader = new PinotSegmentColumnReaderImpl(_dictEncodedSegment, INT_COL_1);

    try {
      int totalDocs = reader.getTotalDocs();
      Assert.assertTrue(totalDocs > 0, "Expected non-empty segment");

      // Test first document (docId = 0)
      int firstValue = reader.getInt(0);
      Object expectedFirst = _testData.get(0).getValue(INT_COL_1);
      if (expectedFirst != null) {
        Assert.assertEquals(firstValue, expectedFirst, "First document value mismatch");
      }

      // Test last document (docId = totalDocs - 1)
      int lastDocId = totalDocs - 1;
      int lastValue = reader.getInt(lastDocId);
      Object expectedLast = _testData.get(lastDocId).getValue(INT_COL_1);
      if (expectedLast != null) {
        Assert.assertEquals(lastValue, expectedLast, "Last document value mismatch");
      }

      // Test docId = -1 (should throw IndexOutOfBoundsException)
      try {
        reader.getInt(-1);
        Assert.fail("Expected IndexOutOfBoundsException for docId = -1");
      } catch (IndexOutOfBoundsException e) {
        // Expected
      }

      // Test docId = totalDocs (should throw IndexOutOfBoundsException)
      try {
        reader.getInt(totalDocs);
        Assert.fail("Expected IndexOutOfBoundsException for docId = totalDocs");
      } catch (IndexOutOfBoundsException e) {
        // Expected
      }

      // Test docId = totalDocs + 100 (should throw IndexOutOfBoundsException)
      try {
        reader.getInt(totalDocs + 100);
        Assert.fail("Expected IndexOutOfBoundsException for docId > totalDocs");
      } catch (IndexOutOfBoundsException e) {
        // Expected
      }

      // Test isNull with out of bounds docId
      try {
        reader.isNull(-1);
        Assert.fail("Expected IndexOutOfBoundsException for isNull(-1)");
      } catch (IndexOutOfBoundsException e) {
        // Expected
      }

      try {
        reader.isNull(totalDocs);
        Assert.fail("Expected IndexOutOfBoundsException for isNull(totalDocs)");
      } catch (IndexOutOfBoundsException e) {
        // Expected
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testIteratorAndRandomAccessConsistency()
      throws Exception {
    PinotSegmentColumnReaderImpl reader = new PinotSegmentColumnReaderImpl(_dictEncodedSegment, INT_COL_1);

    try {
      int totalDocs = reader.getTotalDocs();

      // First, read all values using random access
      Integer[] randomAccessValues = new Integer[totalDocs];
      for (int docId = 0; docId < totalDocs; docId++) {
        randomAccessValues[docId] = reader.getInt(docId);
      }

      // Now read using iterator pattern
      reader.rewind();
      Integer[] iteratorValues = new Integer[totalDocs];
      int index = 0;
      while (reader.hasNext()) {
        Object value = reader.next();
        iteratorValues[index++] = (Integer) value;
      }

      Assert.assertEquals(index, totalDocs, "Iterator should read all documents");

      // Compare values
      for (int i = 0; i < totalDocs; i++) {
        Assert.assertEquals(iteratorValues[i], randomAccessValues[i],
            "Iterator and random access values differ at index " + i);
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testIteratorAndRandomAccessConsistencyMV()
      throws Exception {
    PinotSegmentColumnReaderImpl reader = new PinotSegmentColumnReaderImpl(_dictEncodedSegment, MV_INT_COL);

    try {
      int totalDocs = reader.getTotalDocs();

      // First, read all values using random access
      int[][] randomAccessValues = new int[totalDocs][];
      for (int docId = 0; docId < totalDocs; docId++) {
        randomAccessValues[docId] = reader.getIntMV(docId);
      }

      // Now read using iterator pattern
      reader.rewind();
      Object[] iteratorValues = new Object[totalDocs];
      int index = 0;
      while (reader.hasNext()) {
        Object value = reader.next();
        iteratorValues[index++] = value;
      }

      Assert.assertEquals(index, totalDocs, "Iterator should read all documents");

      // Compare values
      for (int i = 0; i < totalDocs; i++) {
        // Convert iterator value (Integer[]) to int[]
        Integer[] iteratorArray = (Integer[]) iteratorValues[i];
        int[] iteratorIntArray = new int[iteratorArray.length];
        for (int j = 0; j < iteratorArray.length; j++) {
          iteratorIntArray[j] = iteratorArray[j];
        }

        Assert.assertEquals(iteratorIntArray, randomAccessValues[i],
            "Iterator and random access MV values differ at index " + i);
      }
    } finally {
      reader.close();
    }
  }

  @Test
  public void testMultipleReadersForSameColumn()
      throws Exception {
    // Test that multiple readers can be created for the same column and read independently
    PinotSegmentColumnReaderImpl reader1 = new PinotSegmentColumnReaderImpl(_dictEncodedSegment, INT_COL_1);
    PinotSegmentColumnReaderImpl reader2 = new PinotSegmentColumnReaderImpl(_dictEncodedSegment, INT_COL_1);

    try {
      int totalDocs = reader1.getTotalDocs();
      Assert.assertEquals(reader2.getTotalDocs(), totalDocs);

      // Read from both readers at different positions
      for (int docId = 0; docId < Math.min(10, totalDocs); docId++) {
        int value1 = reader1.getInt(docId);
        int value2 = reader2.getInt(docId);
        Assert.assertEquals(value1, value2, "Values should match at docId " + docId);
      }
    } finally {
      reader1.close();
      reader2.close();
    }
  }

  @Test
  public void testColumnName()
      throws Exception {
    PinotSegmentColumnReaderImpl reader = new PinotSegmentColumnReaderImpl(_dictEncodedSegment, STRING_COL_1);

    try {
      Assert.assertEquals(reader.getColumnName(), STRING_COL_1);
    } finally {
      reader.close();
    }
  }

  /**
   * Test isNextNull() method behavior.
   */
  @Test
  public void testIsNextNull()
      throws Exception {
    PinotSegmentColumnReaderImpl reader = new PinotSegmentColumnReaderImpl(_dictEncodedSegment, INT_COL_1);

    try {
      int totalDocs = reader.getTotalDocs();
      int docId = 0;

      // For non-nullable segments, isNextNull should always return false
      while (reader.hasNext()) {
        boolean isNull = reader.isNextNull();
        boolean expectedNull = reader.isNull(docId);
        Assert.assertEquals(isNull, expectedNull, "isNextNull() mismatch at docId " + docId);
        // Verify isNextNull doesn't advance the iterator
        if (!isNull) {
          int value = reader.nextInt();
          Assert.assertEquals(value, reader.getInt(docId));
        } else {
          reader.skipNext();
        }
        docId++;
      }

      Assert.assertEquals(docId, totalDocs);
    } finally {
      reader.close();
    }
  }

  /**
   * Test skipNext() method behavior.
   */
  @Test
  public void testSkipNext()
      throws Exception {
    PinotSegmentColumnReaderImpl reader = new PinotSegmentColumnReaderImpl(_dictEncodedSegment, INT_COL_1);

    try {
      int totalDocs = reader.getTotalDocs();
      // Skip every other value
      int docId = 0;
      int valuesRead = 0;
      while (reader.hasNext()) {
        if (docId % 2 == 0) {
          // Read even-indexed documents
          int value = reader.nextInt();
          Assert.assertEquals(value, reader.getInt(docId));
          valuesRead++;
        } else {
          // Skip odd-indexed documents
          reader.skipNext();
        }
        docId++;
      }

      Assert.assertEquals(docId, totalDocs);
      Assert.assertEquals(valuesRead, (totalDocs + 1) / 2, "Should have read half the documents");
    } finally {
      reader.close();
    }
  }

  /**
   * Test that calling next methods after hasNext returns false throws exception.
   */
  @Test
  public void testNextMethodsAfterEnd()
      throws Exception {
    PinotSegmentColumnReaderImpl reader = new PinotSegmentColumnReaderImpl(_dictEncodedSegment, INT_COL_1);

    try {
      // Read all values
      while (reader.hasNext()) {
        reader.nextInt();
      }

      // Now try to read more - should throw IllegalStateException
      Assert.assertFalse(reader.hasNext());

      try {
        reader.nextInt();
        Assert.fail("nextInt() should throw IllegalStateException when hasNext is false");
      } catch (IllegalStateException e) {
        // Expected
      }

      try {
        reader.isNextNull();
        Assert.fail("isNextNull() should throw IllegalStateException when hasNext is false");
      } catch (IllegalStateException e) {
        // Expected
      }

      try {
        reader.skipNext();
        Assert.fail("skipNext() should throw IllegalStateException when hasNext is false");
      } catch (IllegalStateException e) {
        // Expected
      }
    } finally {
      reader.close();
    }
  }

  /**
   * Test rewind with type-specific iteration methods.
   */
  @Test
  public void testRewindWithTypeSpecificMethods()
      throws Exception {
    PinotSegmentColumnReaderImpl reader = new PinotSegmentColumnReaderImpl(_dictEncodedSegment, LONG_COL);

    try {
      int totalDocs = reader.getTotalDocs();

      // First pass: read all values
      long[] firstPassValues = new long[totalDocs];
      int idx = 0;
      while (reader.hasNext()) {
        if (!reader.isNextNull()) {
          firstPassValues[idx++] = reader.nextLong();
        } else {
          reader.skipNext();
          idx++;
        }
      }

      Assert.assertFalse(reader.hasNext(), "Should be at end after first pass");

      // Rewind
      reader.rewind();
      Assert.assertTrue(reader.hasNext(), "Should have values after rewind");

      // Second pass: verify values match
      idx = 0;
      while (reader.hasNext()) {
        if (!reader.isNextNull()) {
          long value = reader.nextLong();
          Assert.assertEquals(value, firstPassValues[idx++], "Value mismatch on second pass at index " + idx);
        } else {
          reader.skipNext();
          idx++;
        }
      }

      Assert.assertEquals(idx, totalDocs, "Should read same number of documents on second pass");
    } finally {
      reader.close();
    }
  }

  /**
   * Test consistency between all three iteration patterns.
   */
  @Test
  public void testAllIterationPatternsConsistency()
      throws Exception {
    PinotSegmentColumnReaderImpl reader = new PinotSegmentColumnReaderImpl(_dictEncodedSegment, INT_COL_1);

    try {
      int totalDocs = reader.getTotalDocs();

      // Pattern 1: Read using next() and null checks
      reader.rewind();
      Integer[] pattern1Values = new Integer[totalDocs];
      int idx = 0;
      while (reader.hasNext()) {
        Object value = reader.next();
        pattern1Values[idx++] = (Integer) value;
      }

      // Pattern 2: Read using isNextNull() + nextInt()
      reader.rewind();
      Integer[] pattern2Values = new Integer[totalDocs];
      idx = 0;
      while (reader.hasNext()) {
        if (!reader.isNextNull()) {
          pattern2Values[idx++] = reader.nextInt();
        } else {
          reader.skipNext();
          pattern2Values[idx++] = null;
        }
      }

      // Pattern 3: Read using getTotalDocs() + getInt(docId)
      Integer[] pattern3Values = new Integer[totalDocs];
      for (int docId = 0; docId < totalDocs; docId++) {
        if (!reader.isNull(docId)) {
          pattern3Values[docId] = reader.getInt(docId);
        } else {
          pattern3Values[docId] = null;
        }
      }

      // Compare all patterns
      for (int i = 0; i < totalDocs; i++) {
        Assert.assertEquals(pattern1Values[i], pattern2Values[i],
            "Pattern 1 and Pattern 2 differ at index " + i);
        Assert.assertEquals(pattern1Values[i], pattern3Values[i],
            "Pattern 1 and Pattern 3 differ at index " + i);
        Assert.assertEquals(pattern2Values[i], pattern3Values[i],
            "Pattern 2 and Pattern 3 differ at index " + i);
      }
    } finally {
      reader.close();
    }
  }
}
