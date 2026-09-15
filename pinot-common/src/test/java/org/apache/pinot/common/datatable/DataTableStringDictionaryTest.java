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
package org.apache.pinot.common.datatable;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;


/// Tests dictionary decoding and buffer consumption. Each invocation owns its buffers and decoder.
public class DataTableStringDictionaryTest {
  private static final int BUFFER_PREFIX_SIZE = 11;
  private static final int TRAILING_SENTINEL = 0x12345678;

  @DataProvider
  public Object[][] bufferKinds() {
    return new Object[][]{
        {false, false, false}, {false, false, true}, {false, true, false}, {false, true, true},
        {true, false, false}, {true, false, true}, {true, true, false}, {true, true, true}
    };
  }

  @Test(dataProvider = "bufferKinds")
  public void testMixedStringsAndBufferPosition(boolean direct, boolean readOnly, boolean sliced)
      throws IOException {
    String[] expected = {
        "", "first", "é", "東京", "𝄞😀", "", "long-" + "x".repeat(4097), "tiny", "\u0000tail", "final"
    };
    byte[][] entries = new byte[expected.length][];
    for (int i = 0; i < expected.length; i++) {
      entries[i] = expected[i].getBytes(UTF_8);
    }
    byte[] payload = dictionaryPayload(entries);
    ByteBuffer buffer = inputBuffer(payload, direct, readOnly, sliced);
    int initialLimit = buffer.limit();
    buffer.mark();

    String[] actual = new DataTableImplV4().deserializeStringDictionary(buffer);

    // Earlier entries must remain unchanged after longer, shorter and empty entries reuse the scratch bytes.
    assertEquals(actual, expected);
    assertSame(actual[0], "");
    assertSame(actual[5], "");
    assertEquals(buffer.position(), BUFFER_PREFIX_SIZE + payload.length - Integer.BYTES);
    assertEquals(buffer.limit(), initialLimit);
    assertEquals(buffer.getInt(), TRAILING_SENTINEL);
    assertEquals(buffer.remaining(), 0);
    buffer.reset();
    assertEquals(buffer.position(), BUFFER_PREFIX_SIZE);
    assertEquals(new DataTableImplV4().deserializeStringDictionary(buffer), expected);
  }

  @Test(dataProvider = "bufferKinds")
  public void testEmptyDictionary(boolean direct, boolean readOnly, boolean sliced)
      throws IOException {
    ByteBuffer buffer = inputBuffer(dictionaryPayload(), direct, readOnly, sliced);
    assertEquals(new DataTableImplV4().deserializeStringDictionary(buffer), new String[0]);
    assertEquals(buffer.position(), BUFFER_PREFIX_SIZE + Integer.BYTES);
    assertEquals(buffer.getInt(), TRAILING_SENTINEL);
    assertEquals(buffer.remaining(), 0);
  }

  @Test(dataProvider = "bufferKinds")
  public void testMalformedUtf8MatchesStandaloneDecoder(boolean direct, boolean readOnly, boolean sliced)
      throws IOException {
    byte[][] entries = {
        {(byte) 0xc3, 0x28}, {(byte) 0xf0, (byte) 0x9f}, {(byte) 0xed, (byte) 0xa0, (byte) 0x80},
        {(byte) 0x80, 0x41}, {(byte) 0xef, (byte) 0xbf, (byte) 0xbd}, {}, "last".getBytes(UTF_8)
    };
    byte[] payload = dictionaryPayload(entries);
    ByteBuffer buffer = inputBuffer(payload, direct, readOnly, sliced);
    ByteBuffer reference = inputBuffer(payload, direct, readOnly, sliced);
    String[] expected = new String[reference.getInt()];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = DataTableUtils.decodeString(reference);
    }

    String[] actual = new DataTableImplV4().deserializeStringDictionary(buffer);
    assertEquals(actual, expected);
    assertEquals(actual, new String[]{"\ufffd(", "\ufffd", "\ufffd", "\ufffdA", "\ufffd", "", "last"});
    assertEquals(buffer.position(), reference.position());
    assertEquals(buffer.getInt(), TRAILING_SENTINEL);
  }

  @Test(dataProvider = "bufferKinds")
  public void testNegativeDictionaryAndEntryLengths(boolean direct, boolean readOnly, boolean sliced) {
    assertDecodeFailure(ByteBuffer.allocate(4).putInt(-1).array(), direct, readOnly, sliced,
        NegativeArraySizeException.class, 4);
    assertDecodeFailure(ByteBuffer.allocate(8).putInt(1).putInt(-1).array(), direct, readOnly, sliced,
        NegativeArraySizeException.class, 8);
    // A negative length must retain its original failure even after a previous entry allocated the scratch bytes.
    assertDecodeFailure(ByteBuffer.allocate(13).putInt(2).putInt(1).put((byte) 'a').putInt(-1).array(), direct,
        readOnly, sliced, NegativeArraySizeException.class, 13);
  }

  @Test(dataProvider = "bufferKinds")
  public void testTruncatedDictionaryAndEntryLengths(boolean direct, boolean readOnly, boolean sliced) {
    for (int availableBytes = 0; availableBytes < Integer.BYTES; availableBytes++) {
      assertDecodeFailure(new byte[availableBytes], direct, readOnly, sliced, BufferUnderflowException.class, 0);
      assertDecodeFailure(ByteBuffer.allocate(4 + availableBytes).putInt(1).array(), direct, readOnly, sliced,
          BufferUnderflowException.class, 4);
    }
  }

  @Test(dataProvider = "bufferKinds")
  public void testTruncatedStringBytes(boolean direct, boolean readOnly, boolean sliced) {
    assertDecodeFailure(ByteBuffer.allocate(10).putInt(1).putInt(5).put((byte) 'a').put((byte) 'b').array(), direct,
        readOnly, sliced, BufferUnderflowException.class, 8);
    // The second entry fits the existing scratch array but exceeds the remaining bytes in the source buffer.
    assertDecodeFailure(ByteBuffer.allocate(17).putInt(2).putInt(3).put(new byte[]{'a', 'b', 'c'}).putInt(3)
        .put(new byte[]{'d', 'e'}).array(), direct, readOnly, sliced, BufferUnderflowException.class, 15);
  }

  private static void assertDecodeFailure(byte[] payload, boolean direct, boolean readOnly, boolean sliced,
      Class<? extends Throwable> exceptionType, int consumedBytes) {
    ByteBuffer buffer = inputBuffer(payload, direct, readOnly, sliced);
    int initialLimit = buffer.limit();
    Throwable exception = expectThrows(exceptionType,
        () -> new DataTableImplV4().deserializeStringDictionary(buffer));
    assertSame(exception.getClass(), exceptionType);
    assertEquals(buffer.position(), BUFFER_PREFIX_SIZE + consumedBytes);
    assertEquals(buffer.limit(), initialLimit);
  }

  private static byte[] dictionaryPayload(byte[]... entries) {
    int payloadSize = Integer.BYTES * 2;
    for (byte[] entry : entries) {
      payloadSize += Integer.BYTES + entry.length;
    }
    ByteBuffer payload = ByteBuffer.allocate(payloadSize).putInt(entries.length);
    for (byte[] entry : entries) {
      payload.putInt(entry.length).put(entry);
    }
    return payload.putInt(TRAILING_SENTINEL).array();
  }

  private static ByteBuffer inputBuffer(byte[] payload, boolean direct, boolean readOnly, boolean sliced) {
    int sliceOffset = sliced ? 7 : 0;
    int capacity = sliceOffset + BUFFER_PREFIX_SIZE + payload.length + 5;
    ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
    buffer.position(sliceOffset + BUFFER_PREFIX_SIZE);
    buffer.put(payload);
    buffer.limit(buffer.position());
    buffer.position(sliceOffset);
    if (sliced) {
      buffer = buffer.slice();
    }
    buffer.position(BUFFER_PREFIX_SIZE);
    return readOnly ? buffer.asReadOnlyBuffer() : buffer;
  }
}
