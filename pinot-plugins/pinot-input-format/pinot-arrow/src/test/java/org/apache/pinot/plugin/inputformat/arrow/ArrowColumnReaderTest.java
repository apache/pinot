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
package org.apache.pinot.plugin.inputformat.arrow;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Unit tests for [ArrowColumnReader]'s `Bool` / `Timestamp` handling. Neither has a dedicated accessor, so
/// [ArrowColumnReader#getValueType()] returns `null` and both are read through [ArrowColumnReader#getValue(int)] —
/// a `Boolean` for `Bool`; a `Timestamp`, or a raw epoch `Long` under `extractRawTimeValues`, for `Timestamp`.
public class ArrowColumnReaderTest {

  @Test
  public void testBooleanReadsAsBooleanThroughGetValue()
      throws Exception {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        BitVector vector = new BitVector("boolCol", allocator)) {
      vector.allocateNew(2);
      vector.set(0, 1);
      vector.set(1, 0);
      vector.setValueCount(2);

      ArrowColumnReader reader = new ArrowColumnReader("boolCol", vector);
      assertNull(reader.getValueType());
      assertEquals(reader.getValue(0), Boolean.TRUE);
      assertEquals(reader.getValue(1), Boolean.FALSE);
    }
  }

  @Test
  public void testTimestampReadsAsTimestampThroughGetValue()
      throws Exception {
    long epochMillis = LocalDateTime.of(2026, 7, 3, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        TimeStampMilliVector vector = new TimeStampMilliVector("tsCol", allocator)) {
      vector.allocateNew(1);
      vector.set(0, epochMillis);
      vector.setValueCount(1);

      ArrowColumnReader reader = new ArrowColumnReader("tsCol", vector);
      assertNull(reader.getValueType());
      assertEquals(reader.getValue(0), new Timestamp(epochMillis));
    }
  }

  @Test
  public void testTimestampWithRawTimeValuesReadsRawLongThroughGetValue()
      throws Exception {
    long epochMillis = LocalDateTime.of(2026, 7, 3, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        TimeStampMilliVector vector = new TimeStampMilliVector("tsCol", allocator)) {
      vector.allocateNew(1);
      vector.set(0, epochMillis);
      vector.setValueCount(1);

      // Timestamp has no dedicated accessor even with raw extraction; getValue() yields the raw epoch Long.
      ArrowColumnReader reader = new ArrowColumnReader("tsCol", vector, true);
      assertNull(reader.getValueType());
      assertEquals(reader.getValue(0), epochMillis);
    }
  }
}
