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
package org.apache.pinot.segment.local.segment.creator.impl;

import java.io.File;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReaderFactory;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for core ColumnReader interface and implementations.
 *
 * <p>This test class validates:
 * <ul>
 *   <li>PinotSegmentColumnReaderFactory functionality</li>
 *   <li>DefaultValueColumnReader for new columns</li>
 *   <li>ColumnReader interface contract</li>
 * </ul>
 */
public class ColumnReaderInterfaceTest extends ColumnarSegmentBuildingTestBase {

  @Test
  public void testColumnReaderFactory()
      throws Exception {
    // Create a segment to test column reader factory with
    File segmentDir = createRowMajorSegment();
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);

    try {
      // Test PinotSegmentColumnReaderFactory
      try (PinotSegmentColumnReaderFactory factory = new PinotSegmentColumnReaderFactory(segment)) {
        factory.init(_originalSchema);

        // Test that all expected columns are available
        Set<String> availableColumns = factory.getAvailableColumns();
        Assert.assertTrue(availableColumns.contains(STRING_COL_1));
        Assert.assertTrue(availableColumns.contains(INT_COL_1));
        Assert.assertTrue(availableColumns.contains(MV_INT_COL));

        // Test getting individual column readers
        ColumnReader stringReader = factory.getColumnReader(STRING_COL_1);
        Assert.assertEquals(stringReader.getColumnName(), STRING_COL_1);

        ColumnReader mvReader = factory.getColumnReader(MV_INT_COL);
        Assert.assertEquals(mvReader.getColumnName(), MV_INT_COL);

        // Test reading values using iterator pattern
        Assert.assertTrue(stringReader.hasNext());
        Object firstStringValue = stringReader.next();
        Assert.assertEquals(firstStringValue, "string1_0");

        // Test that we can continue reading
        Assert.assertTrue(stringReader.hasNext());
        Object secondStringValue = stringReader.next();
        Assert.assertEquals(secondStringValue, "string1_1");

        // Reset and test again
        stringReader.rewind();
        Assert.assertTrue(stringReader.hasNext());
        firstStringValue = stringReader.next();
        Assert.assertEquals(firstStringValue, "string1_0");

        // Test getting all column readers
        Map<String, ColumnReader> allReaders = factory.getAllColumnReaders();
        Assert.assertEquals(allReaders.size(), _originalSchema.getPhysicalColumnNames().size());
      }
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testColumnReaderWithNewColumns()
      throws Exception {
    // Create a segment to test with
    File segmentDir = createRowMajorSegment();
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);

    try {
      // Test getting readers for new columns (should return default value readers)
      try (PinotSegmentColumnReaderFactory factory = new PinotSegmentColumnReaderFactory(segment)) {
        factory.init(_extendedSchema);

        // Test getting reader for new column
        ColumnReader newStringReader = factory.getColumnReader(NEW_STRING_COL);
        Assert.assertEquals(newStringReader.getColumnName(), NEW_STRING_COL);

        // Verify it returns default values using iterator pattern
        Assert.assertTrue(newStringReader.hasNext());
        Object defaultValue = newStringReader.next();
        Assert.assertEquals(defaultValue, _extendedSchema.getFieldSpecFor(NEW_STRING_COL).getDefaultNullValue());

        // Test that all values are the same (default)
        int valueCount = 1; // We already read one value
        while (newStringReader.hasNext()) {
          Object value = newStringReader.next();
          Assert.assertEquals(value, defaultValue);
          valueCount++;
        }
        Assert.assertEquals(valueCount, _testData.size());
      }
    } finally {
      segment.destroy();
    }
  }
}
