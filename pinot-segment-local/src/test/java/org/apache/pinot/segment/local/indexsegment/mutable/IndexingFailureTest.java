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
package org.apache.pinot.segment.local.indexsegment.mutable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class IndexingFailureTest implements PinotBuffersAfterMethodCheckRule {
  private static final String TABLE_NAME = "testTable";
  private static final String INT_COL = "int_col";
  private static final String STRING_COL = "string_col";
  private static final String JSON_COL = "json_col";

  private MutableSegmentImpl _mutableSegment;
  private ServerMetrics _serverMetrics;

  @BeforeMethod
  public void setup() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(STRING_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(JSON_COL, FieldSpec.DataType.JSON).setSchemaName(TABLE_NAME).build();
    _serverMetrics = mock(ServerMetrics.class);
    _mutableSegment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Collections.emptySet(), Collections.emptySet(),
            new HashSet<>(Arrays.asList(INT_COL, STRING_COL)),
            Collections.singletonMap(JSON_COL, new JsonIndexConfig()), _serverMetrics);
  }

  @AfterMethod
  public void tearDown() {
    _mutableSegment.destroy();
  }

  @Test
  public void testIndexingFailures()
      throws IOException {
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(System.currentTimeMillis(), new GenericRow());
    GenericRow goodRow = new GenericRow();
    goodRow.putValue(INT_COL, 0);
    goodRow.putValue(STRING_COL, "a");
    goodRow.putValue(JSON_COL, "{\"valid\": \"json\"}");
    _mutableSegment.index(goodRow, defaultMetadata);
    assertEquals(_mutableSegment.getNumDocsIndexed(), 1);
    assertEquals(_mutableSegment.getDataSource(INT_COL).getInvertedIndex().getDocIds(0),
        ImmutableRoaringBitmap.bitmapOf(0));
    assertEquals(_mutableSegment.getDataSource(STRING_COL).getInvertedIndex().getDocIds(0),
        ImmutableRoaringBitmap.bitmapOf(0));
    assertEquals(_mutableSegment.getDataSource(JSON_COL).getJsonIndex().getMatchingDocIds("valid = 'json'"),
        ImmutableRoaringBitmap.bitmapOf(0));
    verify(_serverMetrics, never()).addMeteredTableValue(matches("indexingError$"), eq(ServerMeter.INDEXING_FAILURES),
        anyLong());
    reset(_serverMetrics);

    GenericRow badRow = new GenericRow();
    badRow.putValue(INT_COL, 0);
    badRow.putValue(STRING_COL, "b");
    badRow.putValue(JSON_COL, "{\"truncatedJson...");
    _mutableSegment.index(badRow, defaultMetadata);
    assertEquals(_mutableSegment.getNumDocsIndexed(), 2);
    assertEquals(_mutableSegment.getDataSource(INT_COL).getInvertedIndex().getDocIds(0),
        ImmutableRoaringBitmap.bitmapOf(0, 1));
    assertEquals(_mutableSegment.getDataSource(STRING_COL).getInvertedIndex().getDocIds(1),
        ImmutableRoaringBitmap.bitmapOf(1));
    verify(_serverMetrics, times(1)).addMeteredTableValue(matches("-JSON-indexingError"),
        eq(ServerMeter.INDEXING_FAILURES), eq(1L));
    reset(_serverMetrics);

    GenericRow anotherGoodRow = new GenericRow();
    anotherGoodRow.putValue(INT_COL, 2);
    anotherGoodRow.putValue(STRING_COL, "c");
    anotherGoodRow.putValue(JSON_COL, "{\"valid\": \"json\"}");
    _mutableSegment.index(anotherGoodRow, defaultMetadata);
    assertEquals(_mutableSegment.getNumDocsIndexed(), 3);
    assertEquals(_mutableSegment.getDataSource(INT_COL).getInvertedIndex().getDocIds(1),
        ImmutableRoaringBitmap.bitmapOf(2));
    assertEquals(_mutableSegment.getDataSource(STRING_COL).getInvertedIndex().getDocIds(2),
        ImmutableRoaringBitmap.bitmapOf(2));
    assertEquals(_mutableSegment.getDataSource(JSON_COL).getJsonIndex().getMatchingDocIds("valid = 'json'"),
        ImmutableRoaringBitmap.bitmapOf(0, 2));
    verify(_serverMetrics, never()).addMeteredTableValue(matches("indexingError$"), eq(ServerMeter.INDEXING_FAILURES),
        anyLong());
    reset(_serverMetrics);

    GenericRow nullStringRow = new GenericRow();
    nullStringRow.putValue(INT_COL, 0);
    nullStringRow.putValue(STRING_COL, null);
    nullStringRow.addNullValueField(STRING_COL);
    nullStringRow.putValue(JSON_COL, "{\"valid\": \"json\"}");
    _mutableSegment.index(nullStringRow, defaultMetadata);
    assertEquals(_mutableSegment.getNumDocsIndexed(), 4);
    assertEquals(_mutableSegment.getDataSource(INT_COL).getInvertedIndex().getDocIds(0),
        ImmutableRoaringBitmap.bitmapOf(0, 1, 3));
    assertEquals(_mutableSegment.getDataSource(JSON_COL).getJsonIndex().getMatchingDocIds("valid = 'json'"),
        ImmutableRoaringBitmap.bitmapOf(0, 2, 3));
    assertTrue(_mutableSegment.getDataSource(STRING_COL).getNullValueVector().isNull(3));
    // null string value skipped
    verify(_serverMetrics, times(1)).addMeteredTableValue(matches("DICTIONARY-indexingError$"),
        eq(ServerMeter.INDEXING_FAILURES), eq(1L));
  }
}
