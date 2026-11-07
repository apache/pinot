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
package org.apache.pinot.core.common.datatable;

import java.io.IOException;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.Test;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


/// Tests that [BaseDataTableBuilder] stops building a table that exceeds the maximum response size of the query, and
/// that it samples the resource usage by bytes written.
public class DataTableBuilderSizeLimitTest {
  private static final DataSchema LONG_SCHEMA =
      new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.LONG});
  private static final DataSchema STRING_SCHEMA =
      new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.STRING});
  private static final DataSchema BYTES_SCHEMA =
      new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.BYTES});

  @Test
  public void testNoLimitWithoutQueryContext()
      throws IOException {
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(LONG_SCHEMA);
    addLongRows(builder, 1000);
    assertEquals(builder.build().getNumberOfRows(), 1000);
  }

  @Test
  public void testTableWithinLimit()
      throws IOException {
    try (QueryThreadContext ignore = openContext(100, ThreadAccountantUtils.getNoOpAccountant())) {
      DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(LONG_SCHEMA);
      // 12 rows of 8 bytes are 96 bytes.
      addLongRows(builder, 12);
      DataTable dataTable = builder.build();
      assertEquals(dataTable.getNumberOfRows(), 12);
      assertEquals(dataTable.getLong(11, 0), 11L);
    }
  }

  @Test
  public void testFixedSizeSectionExceedsLimit() {
    try (QueryThreadContext ignore = openContext(100, ThreadAccountantUtils.getNoOpAccountant())) {
      DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(LONG_SCHEMA);
      ResponseSizeLimitExceededException e =
          expectThrows(ResponseSizeLimitExceededException.class, () -> addLongRows(builder, 1000));
      // The 13th row brings the size to 104 bytes.
      assertEquals(e.getMessage(), "Serialized query response size of at least 104 exceeds threshold 100");
      assertEquals(e.getSizeBytes(), 104);
    }
  }

  @Test
  public void testVariableSizeSectionExceedsLimit()
      throws IOException {
    try (QueryThreadContext ignore = openContext(1000, ThreadAccountantUtils.getNoOpAccountant())) {
      DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(BYTES_SCHEMA);
      // Each row writes 8 bytes in the fixed size section and 400 bytes in the variable size section, so 2 rows are
      // 816 bytes and 3 rows are 1224 bytes.
      for (int i = 0; i < 2; i++) {
        builder.startRow();
        builder.setColumn(0, new ByteArray(new byte[400]));
        builder.finishRow();
      }
      builder.startRow();
      builder.setColumn(0, new ByteArray(new byte[400]));
      ResponseSizeLimitExceededException e =
          expectThrows(ResponseSizeLimitExceededException.class, builder::finishRow);
      assertEquals(e.getSizeBytes(), 1224);
    }
  }

  /// Strings are written to a dictionary, not to the data sections, so the builder counts the new dictionary entries.
  @Test
  public void testStringDictionaryCountsTowardsLimit()
      throws IOException {
    String longValue = "x".repeat(100);
    try (QueryThreadContext ignore = openContext(250, ThreadAccountantUtils.getNoOpAccountant())) {
      // The same value adds one dictionary entry of 104 bytes, and 4 bytes per row.
      DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(STRING_SCHEMA);
      for (int i = 0; i < 30; i++) {
        builder.startRow();
        builder.setColumn(0, longValue);
        builder.finishRow();
      }
      assertEquals(builder.build().getNumberOfRows(), 30);

      // Distinct values add one dictionary entry of 105 bytes each, so 2 rows are 218 bytes and 3 rows are 327 bytes.
      DataTableBuilder distinctBuilder = DataTableBuilderFactory.getDataTableBuilder(STRING_SCHEMA);
      for (int i = 0; i < 2; i++) {
        distinctBuilder.startRow();
        distinctBuilder.setColumn(0, longValue + i);
        distinctBuilder.finishRow();
      }
      distinctBuilder.startRow();
      distinctBuilder.setColumn(0, longValue + 2);
      ResponseSizeLimitExceededException e =
          expectThrows(ResponseSizeLimitExceededException.class, distinctBuilder::finishRow);
      assertEquals(e.getSizeBytes(), 327);
    }
  }

  /// A few large rows still sample the resource usage, although the callers sample only every 8192 rows.
  @Test
  public void testSamplesUsageByBytesWritten()
      throws IOException {
    ThreadAccountant accountant = mock(ThreadAccountant.class);
    try (QueryThreadContext ignore = openContext(Integer.MAX_VALUE, accountant)) {
      DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(BYTES_SCHEMA);
      byte[] value = new byte[64 * 1024];
      // 64 rows of 64 KB are 4 MB, so the builder samples about once per MB.
      for (int i = 0; i < 64; i++) {
        builder.startRow();
        builder.setColumn(0, new ByteArray(value));
        builder.finishRow();
      }
      verify(accountant, atLeast(3)).sampleUsage();
      verify(accountant, atMost(4)).sampleUsage();
    }
  }

  @Test
  public void testDoesNotSampleSmallTables()
      throws IOException {
    ThreadAccountant accountant = mock(ThreadAccountant.class);
    try (QueryThreadContext ignore = openContext(Integer.MAX_VALUE, accountant)) {
      DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(LONG_SCHEMA);
      addLongRows(builder, 1000);
      verify(accountant, never()).sampleUsage();
    }
  }

  /// A limit above the 2 GB of the wire format is capped to it.
  @Test
  public void testLimitIsCappedToWireFormatLimit() {
    try (QueryThreadContext ignore = openContext(Long.MAX_VALUE, ThreadAccountantUtils.getNoOpAccountant())) {
      BaseDataTableBuilder builder = (BaseDataTableBuilder) DataTableBuilderFactory.getDataTableBuilder(LONG_SCHEMA);
      assertEquals(builder.getMaxSizeBytes(), Integer.MAX_VALUE);
    }
  }

  private static QueryThreadContext openContext(long maxResponseSizeBytes, ThreadAccountant accountant) {
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();
    executionContext.setMaxResponseSizeBytes(maxResponseSizeBytes);
    return QueryThreadContext.open(executionContext, accountant);
  }

  private static void addLongRows(DataTableBuilder builder, int numRows)
      throws IOException {
    for (int i = 0; i < numRows; i++) {
      builder.startRow();
      builder.setColumn(0, (long) i);
      builder.finishRow();
    }
  }
}
