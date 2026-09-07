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
package org.apache.pinot.segment.local.startree.v2.builder;

import org.apache.pinot.segment.local.startree.v2.builder.OffHeapSingleTreeBuilder.FixedSizeRecordOffsets;
import org.apache.pinot.segment.local.startree.v2.builder.OffHeapSingleTreeBuilder.RecordOffsets;
import org.apache.pinot.segment.local.startree.v2.builder.OffHeapSingleTreeBuilder.VariableSizeRecordOffsets;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class OffHeapSingleTreeBuilderTest {

  @Test
  public void testFixedSizeRecordOffsets() {
    RecordOffsets offsets = new FixedSizeRecordOffsets(1 << 30);
    for (int i = 0; i < 4; i++) {
      offsets.addRecord(1 << 30);
    }
    assertEquals(offsets.getStartOffset(0), 0L);
    assertEquals(offsets.getStartOffset(1), 1L << 30);
    assertEquals(offsets.getStartOffset(3), 3L << 30);
    assertEquals(offsets.getEndOffset(), 1L << 32);
  }

  @Test
  public void testVariableSizeRecordOffsets() {
    RecordOffsets offsets = new VariableSizeRecordOffsets();
    offsets.addRecord(123);
    offsets.addRecord(Integer.MAX_VALUE - 123);
    offsets.addRecord(456);
    offsets.addRecord(789);
    assertEquals(offsets.getStartOffset(0), 0L);
    assertEquals(offsets.getStartOffset(1), 123L);
    assertEquals(offsets.getStartOffset(2), Integer.MAX_VALUE);
    assertEquals(offsets.getStartOffset(3), Integer.MAX_VALUE + 456L);
    assertEquals(offsets.getEndOffset(), Integer.MAX_VALUE + 456L + 789L);
  }
}
