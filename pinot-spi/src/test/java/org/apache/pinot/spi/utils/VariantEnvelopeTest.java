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
package org.apache.pinot.spi.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class VariantEnvelopeTest {
  private static final byte[] METADATA = new byte[]{1, 2, 3};
  private static final byte[] VALUE = new byte[]{0, 10, 20, 30};
  private static final byte[] VERSION_ONE_FROZEN = new byte[]{
      'P', 'V', 'A', 'R', 1, 0, 0, 0,
      0, 0, 0, 3, 0, 0, 0, 4,
      1, 2, 3, 0, 10, 20, 30
  };

  @Test
  public void testRoundTripUsesRemainingBytesWithoutMutatingInputs() {
    ByteBuffer metadata = ByteBuffer.wrap(new byte[]{99, 1, 2, 3, 98});
    metadata.position(1);
    metadata.limit(4);
    ByteBuffer value = ByteBuffer.wrap(new byte[]{97, 0, 10, 20, 30, 96});
    value.position(1);
    value.limit(5);

    byte[] envelope = VariantEnvelope.encode(metadata, value);

    assertEquals(metadata.position(), 1);
    assertEquals(value.position(), 1);
    assertEquals(envelope.length, VariantEnvelope.HEADER_SIZE + METADATA.length + VALUE.length);
    assertEquals(Arrays.copyOfRange(envelope, 0, 4), new byte[]{'P', 'V', 'A', 'R'});
    assertEquals(Byte.toUnsignedInt(envelope[4]), Byte.toUnsignedInt(VariantEnvelope.VERSION));
    assertEquals(envelope[5], VariantEnvelope.FLAGS);
    assertEquals(ByteBuffer.wrap(envelope).order(ByteOrder.BIG_ENDIAN).getInt(8), METADATA.length);
    assertEquals(ByteBuffer.wrap(envelope).order(ByteOrder.BIG_ENDIAN).getInt(12), VALUE.length);

    VariantEnvelope.Decoded decoded = VariantEnvelope.decode(envelope);
    assertEquals(readBytes(decoded.getMetadata()), METADATA);
    assertEquals(readBytes(decoded.getValue()), VALUE);
    assertEquals(VariantEnvelope.validateAndGetMetadataLength(envelope), METADATA.length);
    assertTrue(VariantEnvelope.isEnvelope(envelope));
  }

  @Test
  public void testRoundTripFromArraySlices() {
    byte[] metadata = new byte[]{99, 1, 2, 3, 98};
    byte[] value = new byte[]{97, 0, 10, 20, 30, 96};

    byte[] envelope = VariantEnvelope.encode(metadata, 1, 3, value, 1, 4);

    VariantEnvelope.Decoded decoded = VariantEnvelope.decode(envelope);
    assertEquals(readBytes(decoded.getMetadata()), METADATA);
    assertEquals(readBytes(decoded.getValue()), VALUE);
    assertEquals(VariantEnvelope.validateAndGetMetadataLength(envelope), METADATA.length);
    assertThrows(IllegalArgumentException.class, () -> VariantEnvelope.encode(metadata, -1, 3, value, 1, 4));
    assertThrows(IllegalArgumentException.class, () -> VariantEnvelope.encode(metadata, 1, 5, value, 1, 4));
    assertThrows(IllegalArgumentException.class, () -> VariantEnvelope.encode(metadata, 1, 3, value, 1, 6));
  }

  @Test
  public void testArrayBackedBuffersHonorArrayOffsetPositionAndLimit() {
    ByteBuffer metadata = ByteBuffer.wrap(new byte[]{88, 99, 1, 2, 3, 98, 87});
    metadata.position(1);
    metadata.limit(6);
    metadata = metadata.slice();
    metadata.position(1);
    metadata.limit(4);

    ByteBuffer value = ByteBuffer.wrap(new byte[]{86, 97, 0, 10, 20, 30, 96, 85});
    value.position(1);
    value.limit(7);
    value = value.slice();
    value.position(1);
    value.limit(5);

    assertTrue(metadata.hasArray());
    assertTrue(value.hasArray());
    assertEquals(metadata.arrayOffset(), 1);
    assertEquals(value.arrayOffset(), 1);
    assertFrozenEncodingPreservesState(metadata, value);
  }

  @Test
  public void testDirectBuffersUseFallbackWithoutChangingState() {
    ByteBuffer metadata = ByteBuffer.allocateDirect(5);
    metadata.put(new byte[]{99, 1, 2, 3, 98});
    metadata.position(1);
    metadata.limit(4);
    ByteBuffer value = ByteBuffer.allocateDirect(6);
    value.put(new byte[]{97, 0, 10, 20, 30, 96});
    value.position(1);
    value.limit(5);

    assertFalse(metadata.hasArray());
    assertFalse(value.hasArray());
    assertFrozenEncodingPreservesState(metadata, value);
  }

  @Test
  public void testReadOnlyBuffersUseFallbackWithoutChangingState() {
    ByteBuffer metadata = ByteBuffer.wrap(new byte[]{99, 1, 2, 3, 98}).asReadOnlyBuffer();
    metadata.position(1);
    metadata.limit(4);
    ByteBuffer value = ByteBuffer.wrap(new byte[]{97, 0, 10, 20, 30, 96}).asReadOnlyBuffer();
    value.position(1);
    value.limit(5);

    assertFalse(metadata.hasArray());
    assertFalse(value.hasArray());
    assertFrozenEncodingPreservesState(metadata, value);
  }

  @Test
  public void testVersionOneFrozenBytes() {
    assertEquals(VariantEnvelope.encode(ByteBuffer.wrap(METADATA), ByteBuffer.wrap(VALUE)), VERSION_ONE_FROZEN);
    assertEquals(VariantEnvelope.encode(METADATA, 0, METADATA.length, VALUE, 0, VALUE.length), VERSION_ONE_FROZEN);
    VariantEnvelope.Decoded decoded = VariantEnvelope.decode(VERSION_ONE_FROZEN);
    assertEquals(readBytes(decoded.getMetadata()), METADATA);
    assertEquals(readBytes(decoded.getValue()), VALUE);
  }

  @Test
  public void testDirectProducerAllocation() {
    byte[] envelope = VariantEnvelope.allocate(METADATA.length, VALUE.length);
    System.arraycopy(METADATA, 0, envelope, VariantEnvelope.HEADER_SIZE, METADATA.length);
    System.arraycopy(VALUE, 0, envelope, VariantEnvelope.HEADER_SIZE + METADATA.length, VALUE.length);

    assertEquals(envelope, VERSION_ONE_FROZEN);
    assertThrows(IllegalArgumentException.class, () -> VariantEnvelope.allocate(-1, 0));
    assertThrows(IllegalArgumentException.class, () -> VariantEnvelope.allocate(0, -1));
    assertThrows(IllegalArgumentException.class, () -> VariantEnvelope.allocate(Integer.MAX_VALUE, 0));
  }

  @Test
  public void testDecodedBuffersAreReadOnlyAndHaveIndependentPositions() {
    VariantEnvelope.Decoded decoded =
        VariantEnvelope.decode(VariantEnvelope.encode(ByteBuffer.wrap(METADATA), ByteBuffer.wrap(VALUE)));

    ByteBuffer firstMetadata = decoded.getMetadata();
    assertThrows(ReadOnlyBufferException.class, () -> firstMetadata.put((byte) 0));
    firstMetadata.get();
    assertEquals(decoded.getMetadata().position(), 0);

    ByteBuffer firstValue = decoded.getValue();
    assertThrows(ReadOnlyBufferException.class, () -> firstValue.put((byte) 0));
    firstValue.get();
    assertEquals(decoded.getValue().position(), 0);
  }

  @Test
  public void testDecodedBuffersAliasInputEnvelope() {
    byte[] envelope = VariantEnvelope.encode(ByteBuffer.wrap(METADATA), ByteBuffer.wrap(VALUE));
    VariantEnvelope.Decoded decoded = VariantEnvelope.decode(envelope);

    envelope[VariantEnvelope.HEADER_SIZE] = 42;
    envelope[VariantEnvelope.HEADER_SIZE + METADATA.length] = 43;

    assertEquals(decoded.getMetadata().get(0), (byte) 42);
    assertEquals(decoded.getValue().get(0), (byte) 43);
  }

  @Test
  public void testEmptyPayloadBuffersStillProduceEnvelope() {
    byte[] envelope = VariantEnvelope.encode(ByteBuffer.allocate(0), ByteBuffer.allocate(0));
    assertEquals(envelope.length, VariantEnvelope.HEADER_SIZE);
    assertTrue(VariantEnvelope.isEnvelope(envelope));
    assertEquals(VariantEnvelope.decode(envelope).getMetadata().remaining(), 0);
    assertEquals(VariantEnvelope.decode(envelope).getValue().remaining(), 0);

    assertFalse(VariantEnvelope.isEnvelope(new byte[0]));
    assertThrows(IllegalArgumentException.class, () -> VariantEnvelope.decode(new byte[0]));
  }

  @Test
  public void testRejectsInvalidHeaderFields() {
    byte[] valid = VariantEnvelope.encode(ByteBuffer.wrap(METADATA), ByteBuffer.wrap(VALUE));

    byte[] invalidMagic = valid.clone();
    invalidMagic[0] = 'X';
    assertInvalid(invalidMagic, "magic");

    byte[] invalidVersion = valid.clone();
    invalidVersion[4] = 2;
    assertInvalid(invalidVersion, "version");

    byte[] invalidFlags = valid.clone();
    invalidFlags[5] = 1;
    assertInvalid(invalidFlags, "flags");

    byte[] invalidReserved = valid.clone();
    invalidReserved[7] = 1;
    assertInvalid(invalidReserved, "reserved");
  }

  @Test
  public void testRejectsInvalidLengths() {
    byte[] valid = VariantEnvelope.encode(ByteBuffer.wrap(METADATA), ByteBuffer.wrap(VALUE));

    byte[] negativeMetadataLength = valid.clone();
    ByteBuffer.wrap(negativeMetadataLength).order(ByteOrder.BIG_ENDIAN).putInt(8, -1);
    assertInvalid(negativeMetadataLength, "lengths");

    byte[] negativeValueLength = valid.clone();
    ByteBuffer.wrap(negativeValueLength).order(ByteOrder.BIG_ENDIAN).putInt(12, -1);
    assertInvalid(negativeValueLength, "lengths");

    assertInvalid(Arrays.copyOf(valid, valid.length - 1), "length mismatch");
    assertInvalid(Arrays.copyOf(valid, valid.length + 1), "length mismatch");
  }

  private static void assertInvalid(byte[] envelope, String messageFragment) {
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> VariantEnvelope.decode(envelope));
    assertTrue(exception.getMessage().contains(messageFragment), exception.getMessage());
    assertThrows(IllegalArgumentException.class, () -> VariantEnvelope.validateAndGetMetadataLength(envelope));
    assertFalse(VariantEnvelope.isEnvelope(envelope));
  }

  private static void assertFrozenEncodingPreservesState(ByteBuffer metadata, ByteBuffer value) {
    int metadataPosition = metadata.position();
    int metadataLimit = metadata.limit();
    int valuePosition = value.position();
    int valueLimit = value.limit();

    assertEquals(VariantEnvelope.encode(metadata, value), VERSION_ONE_FROZEN);
    assertEquals(metadata.position(), metadataPosition);
    assertEquals(metadata.limit(), metadataLimit);
    assertEquals(value.position(), valuePosition);
    assertEquals(value.limit(), valueLimit);
  }

  private static byte[] readBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }
}
