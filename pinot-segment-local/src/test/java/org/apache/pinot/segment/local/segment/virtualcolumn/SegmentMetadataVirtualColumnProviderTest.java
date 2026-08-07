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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
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


/// Tests for the built-in virtual columns exposing segment metadata (`$creationTime`, `$startTimeMs`, `$endTimeMs`,
/// `$totalDocs` and `$segmentCrc`).
public class SegmentMetadataVirtualColumnProviderTest {
  private static final int NUM_DOCS = 17;
  private static final long CREATION_TIME_MS = 1_700_000_000_000L;
  private static final long START_TIME_MS = 1_690_000_000_000L;
  private static final long END_TIME_MS = 1_695_000_000_000L;
  private static final String CRC = "1234567890";

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
    when(segmentMetadata.getCrc()).thenReturn(CRC);
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
    assertEquals(schema.getFieldSpecFor(BuiltInVirtualColumn.CREATIONTIME).getDataType(), FieldSpec.DataType.LONG);
    assertEquals(schema.getFieldSpecFor(BuiltInVirtualColumn.STARTTIMEMS).getDataType(), FieldSpec.DataType.LONG);
    assertEquals(schema.getFieldSpecFor(BuiltInVirtualColumn.ENDTIMEMS).getDataType(), FieldSpec.DataType.LONG);
    assertEquals(schema.getFieldSpecFor(BuiltInVirtualColumn.TOTALDOCS).getDataType(), FieldSpec.DataType.INT);
    assertEquals(schema.getFieldSpecFor(BuiltInVirtualColumn.SEGMENTCRC).getDataType(), FieldSpec.DataType.STRING);
    for (String column : List.of(BuiltInVirtualColumn.CREATIONTIME, BuiltInVirtualColumn.STARTTIMEMS,
        BuiltInVirtualColumn.ENDTIMEMS, BuiltInVirtualColumn.TOTALDOCS, BuiltInVirtualColumn.SEGMENTCRC)) {
      assertTrue(schema.getFieldSpecFor(column).isSingleValueField(), column + " should be single-value");
    }
  }

  @Test
  public void testValuesFromSegmentMetadata() {
    Schema schema = buildSegmentSchema();
    SegmentMetadata segmentMetadata = mockSegmentMetadata();

    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.CREATIONTIME, segmentMetadata).getLongValue(0),
        CREATION_TIME_MS);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.STARTTIMEMS, segmentMetadata).getLongValue(0),
        START_TIME_MS);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.ENDTIMEMS, segmentMetadata).getLongValue(0), END_TIME_MS);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.TOTALDOCS, segmentMetadata).getIntValue(0), NUM_DOCS);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.SEGMENTCRC, segmentMetadata).getStringValue(0), CRC);
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

    ColumnMetadata crcMetadata = buildColumnMetadata(schema, BuiltInVirtualColumn.SEGMENTCRC, segmentMetadata);
    assertEquals(crcMetadata.getMinValue(), CRC);
    assertEquals(crcMetadata.getMaxValue(), CRC);

    // When the metadata is available the column carries a real value, so there is no null vector
    for (String column : List.of(BuiltInVirtualColumn.CREATIONTIME, BuiltInVirtualColumn.STARTTIMEMS,
        BuiltInVirtualColumn.ENDTIMEMS, BuiltInVirtualColumn.TOTALDOCS, BuiltInVirtualColumn.SEGMENTCRC)) {
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
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.STARTTIMEMS, segmentMetadata).getLongValue(0),
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.ENDTIMEMS, segmentMetadata).getLongValue(0),
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.SEGMENTCRC, segmentMetadata).getStringValue(0),
        FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
    // The stored value is only a placeholder: the column must additionally report every document as null
    assertColumnIsNull(schema, BuiltInVirtualColumn.CREATIONTIME, segmentMetadata);
    assertColumnIsNull(schema, BuiltInVirtualColumn.STARTTIMEMS, segmentMetadata);
    assertColumnIsNull(schema, BuiltInVirtualColumn.ENDTIMEMS, segmentMetadata);
    assertColumnIsNull(schema, BuiltInVirtualColumn.SEGMENTCRC, segmentMetadata);

    // $totalDocs comes from the context, so it is always available and never null
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.TOTALDOCS, segmentMetadata).getIntValue(0), NUM_DOCS);
    assertNull(buildNullValueVector(schema, BuiltInVirtualColumn.TOTALDOCS, segmentMetadata));
  }

  /// The segment metadata is not always available (e.g. when the virtual column is built for a column that is missing
  /// from the segment). The providers must not fail in that case.
  @Test
  public void testMissingSegmentMetadataFallsBackToDefaultNullValue() {
    Schema schema = buildSegmentSchema();

    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.CREATIONTIME, null).getLongValue(0),
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.STARTTIMEMS, null).getLongValue(0),
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.ENDTIMEMS, null).getLongValue(0),
        (long) FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.SEGMENTCRC, null).getStringValue(0),
        FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
    assertEquals(buildDictionary(schema, BuiltInVirtualColumn.TOTALDOCS, null).getIntValue(0), NUM_DOCS);

    assertColumnIsNull(schema, BuiltInVirtualColumn.CREATIONTIME, null);
    assertColumnIsNull(schema, BuiltInVirtualColumn.STARTTIMEMS, null);
    assertColumnIsNull(schema, BuiltInVirtualColumn.ENDTIMEMS, null);
    assertColumnIsNull(schema, BuiltInVirtualColumn.SEGMENTCRC, null);
  }
}
