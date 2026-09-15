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
package org.apache.pinot.segment.local.segment.virtualcolumn;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.BuiltInVirtualColumnDefinitions;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Tests for the built-in virtual columns exposing segment metadata (`$creationTime`, `$startTime`, `$endTime`,
/// `$totalDocs` and `$crc`).
public class SegmentMetadataVirtualColumnProviderTest {
  private static final int NUM_DOCS = 17;
  private static final long CREATION_TIME_MS = 1_700_000_000_000L;
  private static final long START_TIME_MS = 1_690_000_000_000L;
  private static final long END_TIME_MS = 1_695_000_000_000L;
  private static final long CRC = 1234567890L;

  @DataProvider(name = "unsetCreationTimes")
  public Object[][] unsetCreationTimes() {
    return new Object[][]{{Long.MIN_VALUE}, {-1L}, {0L}};
  }

  /// Returns the schema a segment gets after the built-in virtual columns are added to it.
  private static Schema buildSegmentSchema() {
    Schema schema = new Schema();
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(schema, "testSegment");
    return schema;
  }

  private static SegmentMetadata mockSegmentMetadata() {
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getIndexCreationTime()).thenReturn(CREATION_TIME_MS);
    when(segmentMetadata.getTimeInterval()).thenReturn(new Interval(START_TIME_MS, END_TIME_MS, DateTimeZone.UTC));
    when(segmentMetadata.getCrc()).thenReturn(String.valueOf(CRC));
    when(segmentMetadata.getTotalDocs()).thenReturn(NUM_DOCS);
    return segmentMetadata;
  }

  /// Returns the metadata a real CONSUMING segment is created with: no time range and no CRC yet. Uses the real
  /// `SegmentMetadataImpl` rather than a mock so that the unset-value contract stays pinned to what
  /// `pinot-segment-spi` actually produces.
  private static SegmentMetadata consumingSegmentMetadata(long creationTime) {
    return new SegmentMetadataImpl("testTable", "testTable__0__0__20240101T0000Z", new Schema(), creationTime);
  }

  private static Dictionary buildDictionary(Schema schema, String column, SegmentMetadata segmentMetadata) {
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    assertNotNull(fieldSpec, column + " should be added to the segment schema");
    assertTrue(fieldSpec.isVirtualColumn(), column + " should be a virtual column");
    VirtualColumnContext context = new VirtualColumnContext(fieldSpec, NUM_DOCS, segmentMetadata);
    return VirtualColumnProviderFactory.buildProvider(context).buildDictionary(context);
  }

  @Nullable
  private static NullValueVectorReader buildNullValueVector(Schema schema, String column,
      SegmentMetadata segmentMetadata) {
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    VirtualColumnContext context = new VirtualColumnContext(fieldSpec, NUM_DOCS, segmentMetadata);
    return VirtualColumnProviderFactory.buildProvider(context).buildNullValueVector(context);
  }

  /// Asserts the column reads as SQL NULL for every document, and that the data source exposes the null vector (which
  /// is what the query engine consults once null handling is enabled).
  private static void assertColumnIsNull(Schema schema, String column, SegmentMetadata segmentMetadata) {
    NullValueVectorReader nullValueVector = buildNullValueVector(schema, column, segmentMetadata);
    assertNotNull(nullValueVector, column + " should report a null value vector when its metadata is unavailable");
    assertEquals(nullValueVector.getNullBitmap().getCardinality(), NUM_DOCS);
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      assertTrue(nullValueVector.isNull(docId));
    }
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    VirtualColumnContext context = new VirtualColumnContext(fieldSpec, NUM_DOCS, segmentMetadata);
    assertNotNull(VirtualColumnProviderFactory.buildProvider(context).buildDataSource(context).getNullValueVector(),
        column + " data source should expose the null value vector");
  }

  private static ColumnMetadata buildColumnMetadata(Schema schema, String column, SegmentMetadata segmentMetadata) {
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    VirtualColumnContext context = new VirtualColumnContext(fieldSpec, NUM_DOCS, segmentMetadata);
    return VirtualColumnProviderFactory.buildProvider(context).buildMetadata(context);
  }

  /// Replaces the class-load assertion that used to live in VirtualColumnProviderFactory: a definition added without
  /// a provider must be caught here at build time rather than at segment load.
  @Test
  public void testEveryDefinitionHasAProvider() {
    Schema schema = buildSegmentSchema();
    for (BuiltInVirtualColumnDefinitions.Definition definition : BuiltInVirtualColumnDefinitions.DEFINITIONS) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(definition.getName());
      assertNotNull(fieldSpec, "No field spec added for: " + definition.getName());
      assertTrue(fieldSpec.isVirtualColumn(), "No provider configured for: " + definition.getName());
      assertEquals(fieldSpec.getDataType(), definition.getDataType());
      assertEquals(fieldSpec.isSingleValueField(), definition.isSingleValueField());
    }
  }

  @Test
  public void testAllBuiltInVirtualColumnsAreAddedToSegmentSchema() {
    Schema schema = buildSegmentSchema();
    for (String column : BuiltInVirtualColumn.BUILT_IN_VIRTUAL_COLUMNS) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      assertNotNull(fieldSpec, column + " should be added to the segment schema");
      assertTrue(fieldSpec.isVirtualColumn(), column + " should have a virtual column provider configured");
      // Every configured provider must be resolvable
      assertNotNull(VirtualColumnProviderFactory.buildProvider(
          new VirtualColumnContext(fieldSpec, NUM_DOCS, mockSegmentMetadata())));
    }
  }

  @Test
  public void testSegmentMetadataColumnTypes() {
    Schema schema = buildSegmentSchema();
    assertEquals(schema.getFieldSpecFor(BuiltInVirtualColumn.CREATIONTIME).getDataType(),
        FieldSpec.DataType.TIMESTAMP);
    assertEquals(schema.getFieldSpecFor(BuiltInVirtualColumn.STARTTIME).getDataType(),
        FieldSpec.DataType.TIMESTAMP);
    assertEquals(schema.getFieldSpecFor(BuiltInVirtualColumn.ENDTIME).getDataType(),
        FieldSpec.DataType.TIMESTAMP);
    assertEquals(schema.getFieldSpecFor(BuiltInVirtualColumn.TOTALDOCS).getDataType(), FieldSpec.DataType.INT);
    assertEquals(schema.getFieldSpecFor(BuiltInVirtualColumn.CRC).getDataType(), FieldSpec.DataType.LONG);
    for (String column : List.of(BuiltInVirtualColumn.CREATIONTIME, BuiltInVirtualColumn.STARTTIME,
        BuiltInVirtualColumn.ENDTIME, BuiltInVirtualColumn.TOTALDOCS, BuiltInVirtualColumn.CRC)) {
      assertTrue(schema.getFieldSpecFor(column).isSingleValueField(), column + " should be single-value");
    }
  }

  @Test
  public void testValuesFromSegmentMetadata() {
    Schema schema = buildSegmentSchema();
    SegmentMetadata segmentMetadata = mockSegmentMetadata();

    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.CREATIONTIME, segmentMetadata).getLongValue(0),
        CREATION_TIME_MS);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.STARTTIME, segmentMetadata).getLongValue(0),
        START_TIME_MS);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.ENDTIME, segmentMetadata).getLongValue(0), END_TIME_MS);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.TOTALDOCS, segmentMetadata).getIntValue(0), NUM_DOCS);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.CRC, segmentMetadata).getLongValue(0), CRC);
  }

  @Test
  public void testColumnMetadataMatchesValue() {
    Schema schema = buildSegmentSchema();
    SegmentMetadata segmentMetadata = mockSegmentMetadata();

    ColumnMetadata creationTimeMetadata =
        buildColumnMetadata(schema, BuiltInVirtualColumn.CREATIONTIME, segmentMetadata);
    assertEquals(creationTimeMetadata.getTotalDocs(), NUM_DOCS);
    assertEquals(creationTimeMetadata.getCardinality(), 1);
    assertTrue(creationTimeMetadata.isSorted());
    assertTrue(creationTimeMetadata.hasDictionary());
    assertEquals(creationTimeMetadata.getMinValue(), CREATION_TIME_MS);
    assertEquals(creationTimeMetadata.getMaxValue(), CREATION_TIME_MS);

    ColumnMetadata crcMetadata = buildColumnMetadata(schema, BuiltInVirtualColumn.CRC, segmentMetadata);
    assertEquals(crcMetadata.getMinValue(), CRC);
    assertEquals(crcMetadata.getMaxValue(), CRC);

    // When the metadata is available the column carries a real value, so there is no null vector
    for (String column : List.of(BuiltInVirtualColumn.CREATIONTIME, BuiltInVirtualColumn.STARTTIME,
        BuiltInVirtualColumn.ENDTIME, BuiltInVirtualColumn.TOTALDOCS, BuiltInVirtualColumn.CRC)) {
      assertNull(buildNullValueVector(schema, column, segmentMetadata), column + " should not be null");
    }
  }

  /// A segment without a time range and without a CRC must fall back to the default null value for those columns,
  /// while `$creationTime` and `$totalDocs` remain meaningful. An unset creation time is `Long.MIN_VALUE` in the
  /// segment metadata of an immutable segment and `-1` in the ZK metadata a CONSUMING segment is created from.
  @Test(dataProvider = "unsetCreationTimes")
  public void testUnavailableMetadataFallsBackToDefaultNullValue(long unsetCreationTime) {
    Schema schema = buildSegmentSchema();
    SegmentMetadata segmentMetadata = consumingSegmentMetadata(unsetCreationTime);

    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.CREATIONTIME, segmentMetadata).getLongValue(0),
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.STARTTIME, segmentMetadata).getLongValue(0),
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.ENDTIME, segmentMetadata).getLongValue(0),
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.CRC, segmentMetadata).getLongValue(0),
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    // The stored value is only a placeholder: the column must additionally report every document as null
    assertColumnIsNull(schema, BuiltInVirtualColumn.CREATIONTIME, segmentMetadata);
    assertColumnIsNull(schema, BuiltInVirtualColumn.STARTTIME, segmentMetadata);
    assertColumnIsNull(schema, BuiltInVirtualColumn.ENDTIME, segmentMetadata);
    assertColumnIsNull(schema, BuiltInVirtualColumn.CRC, segmentMetadata);

    // $totalDocs comes from the context, so it is always available and never null
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.TOTALDOCS, segmentMetadata).getIntValue(0), NUM_DOCS);
    assertNull(buildNullValueVector(schema, BuiltInVirtualColumn.TOTALDOCS, segmentMetadata));
  }

  /// Segment pruners read `ColumnMetadata` min/max without consulting the null value vector, so a column whose value
  /// is unavailable must not publish the placeholder as its min/max. Otherwise a CONSUMING segment reporting epoch 0
  /// for `$creationTime` sorts first in `ORDER BY $creationTime ASC LIMIT n` and prunes away the committed segments
  /// that actually hold the answer.
  @Test
  public void testUnavailableMetadataPublishesNoMinMax() {
    Schema schema = buildSegmentSchema();
    SegmentMetadata segmentMetadata = consumingSegmentMetadata(-1L);
    for (String column : List.of(BuiltInVirtualColumn.CREATIONTIME, BuiltInVirtualColumn.STARTTIME,
        BuiltInVirtualColumn.ENDTIME, BuiltInVirtualColumn.CRC)) {
      ColumnMetadata columnMetadata = buildColumnMetadata(schema, column, segmentMetadata);
      assertNull(columnMetadata.getMinValue(), "Unavailable " + column + " should not publish a min value");
      assertNull(columnMetadata.getMaxValue(), "Unavailable " + column + " should not publish a max value");

      // The same has to hold through buildDataSource, which is the path a mutable segment takes
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      VirtualColumnContext context = new VirtualColumnContext(fieldSpec, NUM_DOCS, segmentMetadata);
      DataSourceMetadata dataSourceMetadata =
          VirtualColumnProviderFactory.buildProvider(context).buildDataSource(context).getDataSourceMetadata();
      assertNull(dataSourceMetadata.getMinValue(), column);
      assertNull(dataSourceMetadata.getMaxValue(), column);
    }

    // A column whose value IS available still publishes min/max, so pruning keeps working
    ColumnMetadata available = buildColumnMetadata(schema, BuiltInVirtualColumn.CREATIONTIME, mockSegmentMetadata());
    assertEquals(available.getMinValue(), CREATION_TIME_MS);
    assertEquals(available.getMaxValue(), CREATION_TIME_MS);
  }

  /// A segment that has not indexed anything yet still has to produce a well-formed, empty null bitmap rather than
  /// failing or reporting a stale document count.
  @Test
  public void testNullValueVectorOnAnEmptySegment() {
    Schema schema = buildSegmentSchema();
    FieldSpec fieldSpec = schema.getFieldSpecFor(BuiltInVirtualColumn.STARTTIME);
    VirtualColumnContext context = new VirtualColumnContext(fieldSpec, 0, consumingSegmentMetadata(-1L));
    NullValueVectorReader nullValueVector =
        VirtualColumnProviderFactory.buildProvider(context).buildNullValueVector(context);
    assertNotNull(nullValueVector);
    assertTrue(nullValueVector.getNullBitmap().isEmpty());
  }

  /// The null value vector is built on every data source access for a mutable segment, so the bitmap is materialized
  /// lazily. Repeated reads must return an equal - and stable - bitmap.
  @Test
  public void testNullValueVectorBitmapIsStableAcrossReads() {
    Schema schema = buildSegmentSchema();
    FieldSpec fieldSpec = schema.getFieldSpecFor(BuiltInVirtualColumn.ENDTIME);
    VirtualColumnContext context = new VirtualColumnContext(fieldSpec, NUM_DOCS, consumingSegmentMetadata(-1L));
    NullValueVectorReader nullValueVector =
        VirtualColumnProviderFactory.buildProvider(context).buildNullValueVector(context);
    assertNotNull(nullValueVector);
    assertEquals(nullValueVector.getNullBitmap(), nullValueVector.getNullBitmap());
    assertEquals(nullValueVector.getNullBitmap().getCardinality(), NUM_DOCS);
  }

  /// The index container must hand the null value vector to the engine, and must not hand one back for an index type
  /// a virtual column does not have.
  @Test
  public void testIndexContainerExposesTheNullValueVector()
      throws IOException {
    Schema schema = buildSegmentSchema();
    FieldSpec fieldSpec = schema.getFieldSpecFor(BuiltInVirtualColumn.CRC);
    VirtualColumnContext context = new VirtualColumnContext(fieldSpec, NUM_DOCS, consumingSegmentMetadata(-1L));
    try (ColumnIndexContainer container =
        VirtualColumnProviderFactory.buildProvider(context).buildColumnIndexContainer(context)) {
      assertNotNull(container.getIndex(StandardIndexes.nullValueVector()));
      assertNotNull(container.getIndex(StandardIndexes.forward()));
      assertNotNull(container.getIndex(StandardIndexes.dictionary()));
      assertNull(container.getIndex(StandardIndexes.range()));
    }

    // With the metadata available there is no null value vector at all
    VirtualColumnContext availableContext = new VirtualColumnContext(fieldSpec, NUM_DOCS, mockSegmentMetadata());
    try (ColumnIndexContainer container =
        VirtualColumnProviderFactory.buildProvider(availableContext).buildColumnIndexContainer(availableContext)) {
      assertNull(container.getIndex(StandardIndexes.nullValueVector()));
    }
  }

  /// Every stored type the constant-value base supports must round-trip through the type check, so that a new virtual
  /// column of any type is rejected loudly rather than silently mis-cast.
  @Test(dataProvider = "storedTypeValues")
  public void testValueTypeCheckAcceptsEveryStoredType(FieldSpec.DataType dataType, Object value) {
    FieldSpec fieldSpec = new DimensionFieldSpec("col", dataType, true);
    VirtualColumnContext context = new VirtualColumnContext(fieldSpec, NUM_DOCS, null);
    BaseConstantValueVirtualColumnProvider provider = new BaseConstantValueVirtualColumnProvider() {
      @Override
      protected Object getValue(VirtualColumnContext ctx) {
        return value;
      }
    };
    assertNotNull(provider.buildDictionary(context));
    assertNotNull(provider.buildMetadata(context));

    // ... and the same type check rejects a value of the wrong type
    BaseConstantValueVirtualColumnProvider wrong = new BaseConstantValueVirtualColumnProvider() {
      @Override
      protected Object getValue(VirtualColumnContext ctx) {
        return new Object();
      }
    };
    try {
      wrong.buildDictionary(context);
      fail("Expecting an IllegalStateException for data type: " + dataType);
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("col"), e.getMessage());
    }
  }

  @DataProvider(name = "storedTypeValues")
  public Object[][] storedTypeValues() {
    return new Object[][]{
        {FieldSpec.DataType.INT, 1},
        {FieldSpec.DataType.LONG, 1L},
        {FieldSpec.DataType.FLOAT, 1.0f},
        {FieldSpec.DataType.DOUBLE, 1.0d},
        {FieldSpec.DataType.BIG_DECIMAL, BigDecimal.ONE},
        {FieldSpec.DataType.STRING, "value"},
        {FieldSpec.DataType.BYTES, new byte[]{1, 2}},
        {FieldSpec.DataType.TIMESTAMP, 1L}
    };
  }

  /// The type check exists so that a provider returning the wrong box type names the column and the provider instead
  /// of throwing a bare ClassCastException from deep inside segment loading.
  @Test
  public void testWrongValueTypeIsRejectedWithADiagnosticMessage() {
    FieldSpec fieldSpec = buildSegmentSchema().getFieldSpecFor(BuiltInVirtualColumn.CREATIONTIME);
    VirtualColumnContext context = new VirtualColumnContext(fieldSpec, NUM_DOCS, mockSegmentMetadata());
    // A LONG-stored column handed an Integer
    BaseConstantValueVirtualColumnProvider provider = new BaseConstantValueVirtualColumnProvider() {
      @Override
      protected Object getValue(VirtualColumnContext ctx) {
        return 1;
      }
    };
    try {
      provider.buildDictionary(context);
      fail("Expecting an IllegalStateException for a wrongly typed virtual column value");
    } catch (IllegalStateException e) {
      String message = e.getMessage();
      assertTrue(message.contains(BuiltInVirtualColumn.CREATIONTIME), message);
      assertTrue(message.contains("java.lang.Integer"), message);
      assertTrue(message.contains("java.lang.Long"), message);
    }
  }

  /// The segment metadata is not always available (e.g. when the virtual column is built for a column that is missing
  /// from the segment). The providers must not fail in that case.
  @Test
  public void testMissingSegmentMetadataFallsBackToDefaultNullValue() {
    Schema schema = buildSegmentSchema();

    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.CREATIONTIME, null).getLongValue(0),
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.STARTTIME, null).getLongValue(0),
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.ENDTIME, null).getLongValue(0),
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.CRC, null).getLongValue(0),
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.TOTALDOCS, null).getIntValue(0), NUM_DOCS);

    assertColumnIsNull(schema, BuiltInVirtualColumn.CREATIONTIME, null);
    assertColumnIsNull(schema, BuiltInVirtualColumn.STARTTIME, null);
    assertColumnIsNull(schema, BuiltInVirtualColumn.ENDTIME, null);
    assertColumnIsNull(schema, BuiltInVirtualColumn.CRC, null);
  }
}
