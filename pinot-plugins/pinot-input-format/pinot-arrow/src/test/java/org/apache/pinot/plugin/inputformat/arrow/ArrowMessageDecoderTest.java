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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


/// Tests [ArrowMessageDecoder] — decoder-specific concerns: lifecycle (init / re-init / close /
/// allocator config), error handling for malformed payloads, the empty-batch edge case (returns
/// `null`), single-row batches (fields populated directly into the destination), and multi-row
/// batches ([GenericRow#MULTIPLE_RECORDS_KEY] wrapper). Per-type extraction is covered by
/// [ArrowRecordExtractorTest].
public class ArrowMessageDecoderTest {

  @Test
  public void testAllocatorLimitConfig() throws Exception {
    // Custom allocator limit via the props map.
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put(ArrowMessageDecoder.ARROW_ALLOCATOR_LIMIT, "67108864"); // 64MB
    decoder.init(props, Set.of("field1"), "test-topic-custom");
    decoder.close();

    // Default allocator limit when the prop is absent.
    ArrowMessageDecoder defaultDecoder = new ArrowMessageDecoder();
    defaultDecoder.init(new HashMap<>(), Set.of("field1"), "test-topic-default");
    defaultDecoder.close();
  }

  @Test
  public void testReInit() throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();
    Map<String, String> props = new HashMap<>();
    decoder.init(props, Set.of("id"), "topic-1");
    decoder.init(props, Set.of("id"), "topic-1"); // re-init must not throw
    decoder.close();
  }

  @Test
  public void testCloseIsIdempotent() throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();
    decoder.init(new HashMap<>(), Set.of("id"), "topic-close");
    decoder.close();
    decoder.close();
    decoder.close();
  }

  @Test
  public void testInvalidPayloadReturnsNull() throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();
    decoder.init(new HashMap<>(), Set.of("id"), "topic-invalid");
    GenericRow destination = new GenericRow();
    assertNull(decoder.decode("not arrow ipc".getBytes(), destination));
    assertNull(decoder.decode(new byte[]{1, 2, 3, 4, 5}, destination));
    assertNull(decoder.decode(new byte[0], destination));
    assertNull(decoder.decode("not arrow ipc".getBytes(), null));
    decoder.close();
  }

  @Test
  public void testEmptyBatchReturnsNull()
      throws Exception {
    // A stream with the schema header but no record batches → `decode` returns null.
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();
    decoder.init(new HashMap<>(), Set.of("id", "name"), "topic-empty");
    assertNull(decoder.decode(ArrowTestDataUtils.createEmptyArrowIpcData(), null));
    decoder.close();
  }

  @Test
  public void testSingleRowFillsDestinationDirectly()
      throws Exception {
    // 1-row batch → fields populated directly into the destination, no `MULTIPLE_RECORDS_KEY`
    // wrapper. Pre-existing values on the destination are preserved.
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();
    decoder.init(new HashMap<>(), Set.of("id", "name"), "topic-single-row");

    GenericRow destination = new GenericRow();
    destination.putValue("existing_field", "existing_value");
    GenericRow result = decoder.decode(ArrowTestDataUtils.createValidArrowIpcData(1), destination);
    assertSame(result, destination);
    assertEquals(result.getValue("existing_field"), "existing_value");
    assertEquals(result.getValue("id"), 1);
    assertEquals(result.getValue("name"), "name_1");
    assertNull(result.getValue(GenericRow.MULTIPLE_RECORDS_KEY));
    decoder.close();
  }

  @Test
  public void testMultiRowBatch()
      throws Exception {
    // A single Arrow batch with N rows produces N `GenericRow`s under `MULTIPLE_RECORDS_KEY`.
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();
    decoder.init(new HashMap<>(), Set.of("id", "batch_num", "value"), "topic-multi-row");
    GenericRow result =
        decoder.decode(ArrowTestDataUtils.createMultiBatchArrowIpcData(1, 3), null);

    assertNotNull(result);
    //noinspection unchecked
    List<GenericRow> rows = (List<GenericRow>) result.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(rows);
    assertEquals(rows.size(), 3);
    for (int i = 0; i < 3; i++) {
      GenericRow row = rows.get(i);
      assertEquals(row.getValue("id"), i + 1);
      assertEquals(row.getValue("batch_num"), 0);
      assertEquals(row.getValue("value"), "batch_0_row_" + i);
    }
    decoder.close();
  }
}
