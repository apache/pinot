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
import org.testng.annotations.Test;


/**
 * Tests for schema evolution and new column handling.
 *
 * <p>This test class validates:
 * <ul>
 *   <li>Adding new columns with default values during columnar building</li>
 *   <li>Schema evolution compatibility between row-major and columnar approaches</li>
 *   <li>Default value handling for single-value and multi-value columns</li>
 * </ul>
 */
public class ColumnarSchemaEvolutionTest extends ColumnarSegmentBuildingTestBase {

  @Test
  public void testColumnarBuildingWithNewColumns()
      throws Exception {
    // Create original segment with original schema
    File originalSegmentDir = createRowMajorSegment();

    // Create columnar segment with extended schema (has additional columns)
    File columnarSegmentDir = createColumnarSegmentWithNewColumns(originalSegmentDir);

    // Create row-major segment with extended schema for comparison using the same original segment
    File rowMajorExtendedSegmentDir = createRowMajorSegmentWithExtendedSchema(originalSegmentDir);

    // Validate that both segments are identical (comprehensive comparison)
    validateSegmentsIdentical(rowMajorExtendedSegmentDir, columnarSegmentDir);

    // Additional validation that the new segment has the additional columns with default values
    validateSegmentWithNewColumns(columnarSegmentDir);
  }
}
