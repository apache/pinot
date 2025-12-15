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
package org.apache.pinot.segment.local.columntransformer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.readers.InMemoryColumnReader;
import org.apache.pinot.spi.columntransformer.ColumnTransformer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Comprehensive tests for ColumnReaderTransformer.
 * Tests the decorator pattern, transformation chain, performance optimizations,
 * and all three reading patterns (sequential iteration, type-specific iteration, random access).
 */
public class ColumnReaderTransformerTest {

  private static final String COLUMN_NAME = "testColumn";
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

  /**
   * Test custom transformer that appends a suffix to string values.
   */
  private static class SuffixTransformer implements ColumnTransformer {
    private final String _suffix;

    public SuffixTransformer(String suffix) {
      _suffix = suffix;
    }

    @Override
    public boolean isNoOp() {
      return false;
    }

    @Override
    public Object transform(@Nullable Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof String) {
        return value + _suffix;
      }
      return value;
    }
  }

  /**
   * Test custom transformer that always returns null (for testing null detection).
   */
  private static class NullifyTransformer implements ColumnTransformer {
    @Override
    public boolean isNoOp() {
      return false;
    }

    @Override
    public Object transform(@Nullable Object value) {
      return null;
    }
  }

  /**
   * Test custom no-op transformer.
   */
  private static class NoOpTestTransformer implements ColumnTransformer {
    @Override
    public boolean isNoOp() {
      return true;
    }

    @Override
    public Object transform(@Nullable Object value) {
      return value;
    }
  }

  @Test
  public void testBasicConstruction() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, 2, 3};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertNotNull(transformer);
    assertEquals(transformer.getColumnName(), COLUMN_NAME);
    assertEquals(transformer.getTotalDocs(), 3);
  }

  @Test
  public void testConstructionWithCustomTransformers() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {"a", "b", "c"};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, String.class);

    List<ColumnTransformer> customTransformers = new ArrayList<>();
    customTransformers.add(new SuffixTransformer("_suffix"));

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader, customTransformers);

    assertNotNull(transformer);
  }

  @Test
  public void testNoOpTransformersAreFiltered() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, 2, 3};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    List<ColumnTransformer> customTransformers = new ArrayList<>();
    customTransformers.add(new NoOpTestTransformer());

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader, customTransformers);

    // No-op transformers should be filtered out
    // For INT type with matching source/dest, DataTypeTransformer is no-op
    // Only NullValueTransformer remains, so requiresTransformation should be false
    assertFalse(transformer.requiresTransformation());
  }

  @Test
  public void testRequiresTransformationForMatchingTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, 2, 3};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    // INT -> INT requires no transformation (except null handling which is separate)
    assertFalse(transformer.requiresTransformation());
  }

  @Test
  public void testRequiresTransformationForDifferentTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.LONG)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, 2, 3};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    // INT -> LONG requires transformation
    assertTrue(transformer.requiresTransformation());
  }

  @Test
  public void testSequentialIterationWithNext() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {"a", "b", "c"};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, String.class);

    List<ColumnTransformer> customTransformers = new ArrayList<>();
    customTransformers.add(new SuffixTransformer("_suffix"));

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader, customTransformers);

    List<Object> results = new ArrayList<>();
    while (transformer.hasNext()) {
      results.add(transformer.next());
    }

    assertEquals(results.size(), 3);
    assertEquals(results.get(0), "a_suffix");
    assertEquals(results.get(1), "b_suffix");
    assertEquals(results.get(2), "c_suffix");
  }

  @Test
  public void testRewind() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, 2, 3};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    // Read all values
    while (transformer.hasNext()) {
      transformer.next();
    }
    assertFalse(transformer.hasNext());

    // Rewind and read again
    transformer.rewind();
    assertTrue(transformer.hasNext());
    assertEquals(transformer.next(), 1);
  }

  @Test
  public void testIsNextNullAndSkipNext() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, null, 3};
    boolean[] isNull = {false, true, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertTrue(transformer.hasNext());
    assertFalse(transformer.isNextNull());
    assertEquals(transformer.next(), 1);

    assertTrue(transformer.hasNext());
    assertTrue(transformer.isNextNull());
    transformer.skipNext();

    assertTrue(transformer.hasNext());
    assertFalse(transformer.isNextNull());
    assertEquals(transformer.next(), 3);
  }

  @Test
  public void testPrimitiveFastPathWithNextInt() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, 2, 3};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    // Should use fast path since no transformation required
    assertFalse(transformer.requiresTransformation());

    assertEquals(transformer.nextInt(), 1);
    assertEquals(transformer.nextInt(), 2);
    assertEquals(transformer.nextInt(), 3);
  }

  @Test
  public void testPrimitiveFastPathWithNextLong() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.LONG)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1L, 2L, 3L};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Long.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertFalse(transformer.requiresTransformation());

    assertEquals(transformer.nextLong(), 1L);
    assertEquals(transformer.nextLong(), 2L);
    assertEquals(transformer.nextLong(), 3L);
  }

  @Test
  public void testPrimitiveFastPathWithNextFloat() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1.0f, 2.0f, 3.0f};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Float.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertFalse(transformer.requiresTransformation());

    assertEquals(transformer.nextFloat(), 1.0f);
    assertEquals(transformer.nextFloat(), 2.0f);
    assertEquals(transformer.nextFloat(), 3.0f);
  }

  @Test
  public void testPrimitiveFastPathWithNextDouble() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1.0, 2.0, 3.0};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Double.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertFalse(transformer.requiresTransformation());

    assertEquals(transformer.nextDouble(), 1.0);
    assertEquals(transformer.nextDouble(), 2.0);
    assertEquals(transformer.nextDouble(), 3.0);
  }

  @Test
  public void testNextIntWithNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, null, 3};
    boolean[] isNull = {false, true, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertEquals(transformer.nextInt(), 1);
    // Null should be replaced with default value
    assertEquals(transformer.nextInt(), Integer.MIN_VALUE);
    transformer.skipNext();
    assertEquals(transformer.nextInt(), 3);
  }

  @Test
  public void testNextLongWithNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.LONG)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1L, null, 3L};
    boolean[] isNull = {false, true, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Long.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertEquals(transformer.nextLong(), 1L);
    assertEquals(transformer.nextLong(), Long.MIN_VALUE);
    transformer.skipNext();
    assertEquals(transformer.nextLong(), 3L);
  }

  @Test
  public void testNextFloatWithNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1.0f, null, 3.0f};
    boolean[] isNull = {false, true, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Float.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertEquals(transformer.nextFloat(), 1.0f);
    assertEquals(transformer.nextFloat(), Float.NEGATIVE_INFINITY);
    transformer.skipNext();
    assertEquals(transformer.nextFloat(), 3.0f);
  }

  @Test
  public void testNextDoubleWithNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1.0, null, 3.0};
    boolean[] isNull = {false, true, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Double.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertFalse(transformer.isNextNull());
    assertEquals(transformer.nextDouble(), 1.0);
    assertTrue(transformer.isNextNull());
    assertEquals(transformer.nextDouble(), Double.NEGATIVE_INFINITY);
    transformer.skipNext();
    assertFalse(transformer.isNextNull());
    assertEquals(transformer.nextDouble(), 3.0);
  }

  @Test
  public void testNextStringAppliesTransformations() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {"a", "b", "c"};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, String.class);

    List<ColumnTransformer> customTransformers = new ArrayList<>();
    customTransformers.add(new SuffixTransformer("_test"));

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader, customTransformers);

    assertEquals(transformer.nextString(), "a_test");
    assertEquals(transformer.nextString(), "b_test");
    assertEquals(transformer.nextString(), "c_test");
  }

  @Test
  public void testNextBytesAppliesTransformations() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.BYTES)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {"a".getBytes(), "b".getBytes()};
    boolean[] isNull = {false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, byte[].class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    byte[] result1 = transformer.nextBytes();
    assertNotNull(result1);
    byte[] result2 = transformer.nextBytes();
    assertNotNull(result2);
  }

  @Test
  public void testNextIntMVWithNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {new int[]{1, 2}, null, new int[]{3, 4}};
    boolean[] isNull = {false, true, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, false, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    int[] result1 = transformer.nextIntMV();
    assertEquals(result1.length, 2);
    assertEquals(result1[0], 1);
    assertEquals(result1[1], 2);

    // Null should be replaced with default MV value
    int[] result2 = transformer.nextIntMV();
    assertEquals(result2.length, 1);
    assertEquals(result2[0], Integer.MIN_VALUE);
    transformer.skipNext();

    int[] result3 = transformer.nextIntMV();
    assertEquals(result3.length, 2);
    assertEquals(result3[0], 3);
    assertEquals(result3[1], 4);
  }

  @Test
  public void testNextIntMVWithEmptyArray() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {new int[]{1, 2}, new int[0], new int[]{3, 4}};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, false, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    int[] result1 = transformer.nextIntMV();
    assertEquals(result1.length, 2);

    // Empty array should be replaced with default MV value
    int[] result2 = transformer.nextIntMV();
    assertEquals(result2.length, 1);
    assertEquals(result2[0], Integer.MIN_VALUE);

    int[] result3 = transformer.nextIntMV();
    assertEquals(result3.length, 2);
  }

  @Test
  public void testNextLongMVWithNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.LONG)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {new long[]{1L, 2L}, null};
    boolean[] isNull = {false, true};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, false, Long.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    long[] result1 = transformer.nextLongMV();
    assertEquals(result1.length, 2);

    long[] result2 = transformer.nextLongMV();
    assertEquals(result2.length, 1);
    assertEquals(result2[0], Long.MIN_VALUE);
  }

  @Test
  public void testNextFloatMVWithNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {new float[]{1.0f, 2.0f}, null};
    boolean[] isNull = {false, true};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, false, Float.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    float[] result1 = transformer.nextFloatMV();
    assertEquals(result1.length, 2);

    float[] result2 = transformer.nextFloatMV();
    assertEquals(result2.length, 1);
    assertEquals(result2[0], Float.NEGATIVE_INFINITY);
  }

  @Test
  public void testNextDoubleMVWithNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {new double[]{1.0, 2.0}, null};
    boolean[] isNull = {false, true};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, false, Double.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertFalse(transformer.isNextNull());
    double[] result1 = transformer.nextDoubleMV();
    assertEquals(result1.length, 2);

    double[] result2 = transformer.nextDoubleMV();
    assertEquals(result2.length, 1);
    assertEquals(result2[0], Double.NEGATIVE_INFINITY);
  }

  @Test
  public void testNextStringMVAppliesTransformations() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {new String[]{"a", "b"}};
    boolean[] isNull = {false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, false, String.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    String[] result = transformer.nextStringMV();
    assertNotNull(result);
    assertEquals(result.length, 2);
  }

  @Test
  public void testIsNullForSourceNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, null, 3};
    boolean[] isNull = {false, true, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertFalse(transformer.isNull(0));
    assertTrue(transformer.isNull(1));
    assertFalse(transformer.isNull(2));
  }

  @Test
  public void testIsNullForTransformerProducedNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {"a", "b", "c"};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, String.class);

    List<ColumnTransformer> customTransformers = new ArrayList<>();
    customTransformers.add(new NullifyTransformer());

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader, customTransformers);

    // All values should be null because NullifyTransformer returns null
    // isNull should detect this
    assertTrue(transformer.isNull(0));
    assertTrue(transformer.isNull(1));
    assertTrue(transformer.isNull(2));
  }

  @Test
  public void testIsNullOptimizationWithNoTransformers() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, 2, 3};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    // With no transformers (except null), should use fast path
    assertFalse(transformer.requiresTransformation());
    assertFalse(transformer.isNull(0));
  }

  @Test
  public void testGetIntWithRandomAccess() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, 2, 3, 4, 5};
    boolean[] isNull = {false, false, false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    // Random access
    assertEquals(transformer.getInt(2), 3);
    assertEquals(transformer.getInt(0), 1);
    assertEquals(transformer.getInt(4), 5);
  }

  @Test
  public void testGetIntWithNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, null, 3};
    boolean[] isNull = {false, true, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertEquals(transformer.getInt(0), 1);
    assertEquals(transformer.getInt(1), Integer.MIN_VALUE);
    assertEquals(transformer.getInt(2), 3);
  }

  @Test
  public void testGetLongWithRandomAccess() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.LONG)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1L, 2L, 3L};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Long.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertEquals(transformer.getLong(1), 2L);
  }

  @Test
  public void testGetFloatWithRandomAccess() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1.0f, 2.0f, 3.0f};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Float.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertEquals(transformer.getFloat(1), 2.0f);
  }

  @Test
  public void testGetDoubleWithRandomAccess() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1.0, 2.0, 3.0};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Double.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertEquals(transformer.getDouble(1), 2.0);
  }

  @Test
  public void testGetStringAppliesTransformations() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {"a", "b", "c"};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, String.class);

    List<ColumnTransformer> customTransformers = new ArrayList<>();
    customTransformers.add(new SuffixTransformer("_test"));

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader, customTransformers);

    assertEquals(transformer.getString(0), "a_test");
    assertEquals(transformer.getString(1), "b_test");
    assertEquals(transformer.getString(2), "c_test");
  }

  @Test
  public void testGetBytesAppliesTransformations() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.BYTES)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {"a".getBytes(), "b".getBytes()};
    boolean[] isNull = {false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, byte[].class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    byte[] result = transformer.getBytes(0);
    assertNotNull(result);
  }

  @Test
  public void testGetValueAppliesTransformations() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {"a", "b", "c"};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, String.class);

    List<ColumnTransformer> customTransformers = new ArrayList<>();
    customTransformers.add(new SuffixTransformer("_test"));

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader, customTransformers);

    assertEquals(transformer.getValue(1), "b_test");
  }

  @Test
  public void testGetIntMVWithNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {new int[]{1, 2}, null, new int[]{3, 4}};
    boolean[] isNull = {false, true, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, false, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    int[] result1 = transformer.getIntMV(0);
    assertEquals(result1.length, 2);

    int[] result2 = transformer.getIntMV(1);
    assertEquals(result2.length, 1);
    assertEquals(result2[0], Integer.MIN_VALUE);

    int[] result3 = transformer.getIntMV(2);
    assertEquals(result3.length, 2);
  }

  @Test
  public void testGetIntMVWithEmptyArray() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {new int[]{1, 2}, new int[0]};
    boolean[] isNull = {false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, false, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    int[] result1 = transformer.getIntMV(0);
    assertEquals(result1.length, 2);

    int[] result2 = transformer.getIntMV(1);
    assertEquals(result2.length, 1);
    assertEquals(result2[0], Integer.MIN_VALUE);
  }

  @Test
  public void testGetLongMVWithNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.LONG)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {new long[]{1L, 2L}, null};
    boolean[] isNull = {false, true};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, false, Long.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    long[] result2 = transformer.getLongMV(1);
    assertEquals(result2.length, 1);
    assertEquals(result2[0], Long.MIN_VALUE);
  }

  @Test
  public void testGetFloatMVWithNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {new float[]{1.0f, 2.0f}, null};
    boolean[] isNull = {false, true};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, false, Float.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    float[] result2 = transformer.getFloatMV(1);
    assertEquals(result2.length, 1);
    assertEquals(result2[0], Float.NEGATIVE_INFINITY);
  }

  @Test
  public void testGetDoubleMVWithNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {new double[]{1.0, 2.0}, null};
    boolean[] isNull = {false, true};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, false, Double.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    double[] result2 = transformer.getDoubleMV(1);
    assertEquals(result2.length, 1);
    assertEquals(result2[0], Double.NEGATIVE_INFINITY);
  }

  @Test
  public void testGetStringMVAppliesTransformations() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {new String[]{"a", "b"}};
    boolean[] isNull = {false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, false, String.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    String[] result = transformer.getStringMV(0);
    assertNotNull(result);
    assertEquals(result.length, 2);
  }

  @Test
  public void testGetBytesMVAppliesTransformations() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.BYTES)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {new byte[][]{{'a'}, {'b'}}};
    boolean[] isNull = {false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, false, byte[].class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    byte[][] result = transformer.getBytesMV(0);
    assertNotNull(result);
    assertEquals(result.length, 2);
  }

  @Test
  public void testTypeIntrospectionDelegation() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, 2, 3};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertTrue(transformer.isInt());
    assertFalse(transformer.isLong());
    assertFalse(transformer.isFloat());
    assertFalse(transformer.isDouble());
    assertFalse(transformer.isString());
    assertFalse(transformer.isBytes());
    assertTrue(transformer.isSingleValue());
  }

  @Test
  public void testChainedTransformations() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {"a", "b", "c"};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, String.class);

    List<ColumnTransformer> customTransformers = new ArrayList<>();
    customTransformers.add(new SuffixTransformer("_1"));
    customTransformers.add(new SuffixTransformer("_2"));

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader, customTransformers);

    // Transformers should be applied in order: DataType -> Null -> Suffix1 -> Suffix2
    assertEquals(transformer.getString(0), "a_1_2");
    assertEquals(transformer.getString(1), "b_1_2");
  }

  @Test
  public void testZeroDocs() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {};
    boolean[] isNull = {};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertEquals(transformer.getTotalDocs(), 0);
    assertFalse(transformer.hasNext());
  }

  @Test
  public void testAllNulls() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {null, null, null};
    boolean[] isNull = {true, true, true};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    // All should be replaced with default value
    assertEquals(transformer.nextInt(), Integer.MIN_VALUE);
    assertEquals(transformer.nextInt(), Integer.MIN_VALUE);
    assertEquals(transformer.nextInt(), Integer.MIN_VALUE);
  }

  @Test
  public void testMixedNulls() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, null, 3, null, 5};
    boolean[] isNull = {false, true, false, true, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertEquals(transformer.nextInt(), 1);
    assertEquals(transformer.nextInt(), Integer.MIN_VALUE);
    transformer.skipNext();
    assertEquals(transformer.nextInt(), 3);
    assertEquals(transformer.nextInt(), Integer.MIN_VALUE);
    transformer.skipNext();
    assertEquals(transformer.nextInt(), 5);
  }

  @Test
  public void testCloseIsDelegated() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, 2, 3};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    // Should not throw exception
    transformer.close();
  }

  @Test
  public void testTypeConversionProducesNull() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {"valid", "invalid", "123"};
    boolean[] isNull = {false, false, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, String.class);

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setContinueOnError(true);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setIngestionConfig(ingestionConfig)
        .build();

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(tableConfig, schema, fieldSpec, reader);

    // isNull should detect that transformation produces null
    assertTrue(transformer.isNull(0)); // "valid" -> null (invalid int) but we check source first
  }

  @Test
  public void testTransformerOrdering() throws IOException {
    // Test that transformers are applied in correct order:
    // 1. DataTypeColumnTransformer
    // 2. NullValueColumnTransformer
    // 3. Custom transformers (in order provided)
    
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {"test"};
    boolean[] isNull = {false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, String.class);

    List<ColumnTransformer> customTransformers = new ArrayList<>();
    customTransformers.add(new SuffixTransformer("_custom"));

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader, customTransformers);

    // Should apply: DataType (no-op for STRING->STRING) -> Null (no-op for non-null) -> Custom
    String result = (String) transformer.next();
    assertEquals(result, "test_custom");
  }

  @Test
  public void testCustomDefaultNullValue() throws IOException {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT, 999)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    Object[] values = {1, null, 3};
    boolean[] isNull = {false, true, false};
    InMemoryColumnReader reader = new InMemoryColumnReader(COLUMN_NAME, values, isNull, true, Integer.class);

    ColumnReaderTransformer transformer =
        new ColumnReaderTransformer(TABLE_CONFIG, schema, fieldSpec, reader);

    assertEquals(transformer.nextInt(), 1);
    assertEquals(transformer.nextInt(), 999); // Custom default value
    transformer.skipNext();
    assertEquals(transformer.nextInt(), 3);
  }
}

