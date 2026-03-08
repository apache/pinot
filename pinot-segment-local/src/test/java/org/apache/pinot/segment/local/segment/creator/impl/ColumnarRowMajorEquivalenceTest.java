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
import java.util.Set;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.utils.ReadMode;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for columnar vs row-major equivalence.
 *
 * <p>This test class validates that:
 * <ul>
 *   <li>Columnar segment building produces identical results to row-major building</li>
 *   <li>All data types are correctly handled in columnar mode</li>
 *   <li>Segment statistics and metadata are preserved</li>
 * </ul>
 */
public class ColumnarRowMajorEquivalenceTest extends ColumnarSegmentBuildingTestBase {

  @Test
  public void testBasicColumnarBuilding()
      throws Exception {
    // First create a segment using traditional row-major approach
    File rowMajorSegmentDir = createRowMajorSegment();

    // Then create a segment using columnar approach from the row-major segment
    File columnarSegmentDir = createColumnarSegment(rowMajorSegmentDir);

    // Validate that both segments have identical data
    validateSegmentsIdentical(rowMajorSegmentDir, columnarSegmentDir);
  }

  @Test
  public void testAllDataTypes()
      throws Exception {
    // This test validates that all supported data types work correctly with columnar building
    File rowMajorSegmentDir = createRowMajorSegment();
    File columnarSegmentDir = createColumnarSegment(rowMajorSegmentDir);

    // Validate that both segments have identical data for all data types
    validateSegmentsIdentical(rowMajorSegmentDir, columnarSegmentDir);

    // Additionally validate that all expected columns and data types are present
    ImmutableSegment segment = ImmutableSegmentLoader.load(columnarSegmentDir, ReadMode.mmap);
    try {
      Set<String> columnNames = segment.getPhysicalColumnNames();

      // Validate all data types are present
      Assert.assertTrue(columnNames.contains(STRING_COL_1), "STRING column missing");
      Assert.assertTrue(columnNames.contains(INT_COL_1), "INT column missing");
      Assert.assertTrue(columnNames.contains(LONG_COL), "LONG column missing");
      Assert.assertTrue(columnNames.contains(FLOAT_COL), "FLOAT column missing");
      Assert.assertTrue(columnNames.contains(DOUBLE_COL), "DOUBLE column missing");
      Assert.assertTrue(columnNames.contains(BIG_DECIMAL_COL), "BIG_DECIMAL column missing");
      Assert.assertTrue(columnNames.contains(BYTES_COL), "BYTES column missing");
      Assert.assertTrue(columnNames.contains(MV_INT_COL), "Multi-value INT column missing");
      Assert.assertTrue(columnNames.contains(MV_STRING_COL), "Multi-value STRING column missing");
      Assert.assertTrue(columnNames.contains(TIME_COL), "TIME column missing");
    } finally {
      segment.destroy();
    }
  }
}
