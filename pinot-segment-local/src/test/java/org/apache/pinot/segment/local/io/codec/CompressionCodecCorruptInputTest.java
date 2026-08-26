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
package org.apache.pinot.segment.local.io.codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;
import org.xerial.snappy.Snappy;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Negative-path coverage for the compression codecs' corrupt-frame defenses (length-prefix /
/// footer / sanity-cap branches). Happy-path round-trips are covered by [CodecPipelineExecutorTest].
public class CompressionCodecCorruptInputTest {

  private static final CodecContext CTX = new CodecContext(DataType.INT);

  private static ByteBuffer garbage(int n) {
    ByteBuffer b = ByteBuffer.allocateDirect(n);
    for (int i = 0; i < n; i++) {
      b.put((byte) 0xFF);
    }
    b.flip();
    return b;
  }

  @Test
  public void testLz4DecodeRejectsGarbage() {
    // A 0xFF length-prefix decodes to a negative/huge length → out-of-range guard must fire.
    assertThrows(Exception.class,
        () -> Lz4CodecDefinition.INSTANCE.decode(Lz4CodecDefinition.OPTIONS, CTX, garbage(64)));
  }

  @Test
  public void testZstdDecodeRejectsGarbage() {
    assertThrows(Exception.class,
        () -> ZstdCodecDefinition.INSTANCE.decode(ZstdCodecDefinition.INSTANCE.parseOptions(List.of()), CTX,
            garbage(64)));
  }

  @Test
  public void testZstdDecodeIntoRejectsOversizedDeclaredSize() {
    // decodeInto must apply the shared sanity cap before its later destination-capacity check.
    ByteBuffer dst = ByteBuffer.allocateDirect(16);
    IOException exception = expectThrows(IOException.class,
        () -> ZstdCodecDefinition.INSTANCE.decodeInto(
            ZstdCodecDefinition.INSTANCE.parseOptions(List.of()), CTX, oversizedZstdFrame(), dst));
    assertTrue(exception.getMessage().contains("out of range"), exception.getMessage());
  }

  /// Frame header whose content-size field declares (1 << 30) + 1 decompressed bytes.
  private static ByteBuffer oversizedZstdFrame() {
    ByteBuffer frame = ByteBuffer.allocateDirect(9);
    frame.put(new byte[]{
        0x28, (byte) 0xB5, 0x2F, (byte) 0xFD, (byte) 0xA0, 0x01, 0x00, 0x00, 0x40
    });
    frame.flip();
    return frame;
  }

  @Test
  public void testSnappyDecodeRejectsGarbage() {
    assertThrows(Exception.class,
        () -> SnappyCodecDefinition.INSTANCE.decode(SnappyCodecDefinition.OPTIONS, CTX, garbage(64)));
  }

  @Test
  public void testSnappyDecodeRejectsOversizedDeclaredSize() {
    // The sanity cap must fire before any allocation driven by the untrusted header. The declared size is
    // the smallest rejected value (cap + 1), pinning the inclusive boundary: exactly 1 GiB is accepted.
    IOException exception = expectThrows(IOException.class,
        () -> SnappyCodecDefinition.INSTANCE.decode(SnappyCodecDefinition.OPTIONS, CTX, oversizedSnappyFrame()));
    assertTrue(exception.getMessage().contains("out of range"), exception.getMessage());
  }

  @Test
  public void testSnappyDecodeIntoRejectsOversizedDeclaredSize() {
    // decodeInto is the path the bounded executor uses in production; it must reject with the size-cap
    // IOException, not the later dst-capacity IllegalArgumentException.
    ByteBuffer dst = ByteBuffer.allocateDirect(16);
    IOException exception = expectThrows(IOException.class,
        () -> SnappyCodecDefinition.INSTANCE.decodeInto(SnappyCodecDefinition.OPTIONS, CTX,
            oversizedSnappyFrame(), dst));
    assertTrue(exception.getMessage().contains("out of range"), exception.getMessage());
  }

  /// Frame whose varint length header declares (1 << 30) + 1 decompressed bytes — one past the cap.
  private static ByteBuffer oversizedSnappyFrame() {
    ByteBuffer frame = ByteBuffer.allocateDirect(8);
    frame.put(new byte[]{(byte) 0x81, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x04, 0x00, 0x00, 0x00});
    frame.flip();
    return frame;
  }

  @Test
  public void testGzipDecodeRejectsGarbage() {
    IOException exception = expectThrows(IOException.class,
        () -> GzipCodecDefinition.INSTANCE.decode(GzipCodecDefinition.OPTIONS, CTX, garbage(64)));
    assertTrue(exception.getMessage().contains("invalid decompressed size"), exception.getMessage());
  }

  @Test
  public void testGzipDecodeRejectsTruncatedFooter() {
    // Fewer than 4 bytes cannot carry the uncompressed-size footer GZIP appends.
    IOException exception = expectThrows(IOException.class,
        () -> GzipCodecDefinition.INSTANCE.decode(GzipCodecDefinition.OPTIONS, CTX, garbage(2)));
    assertTrue(exception.getMessage().contains("too short"), exception.getMessage());
  }

  @Test
  public void testGzipDecodeRejectsUnderReportedSize() throws Exception {
    ByteBuffer encoded = encodeGzip(new byte[256]);
    encoded.putInt(encoded.limit() - Integer.BYTES, 128);
    IOException exception = expectThrows(IOException.class,
        () -> GzipCodecDefinition.INSTANCE.decode(GzipCodecDefinition.OPTIONS, CTX, encoded));
    assertTrue(exception.getMessage().contains("expands beyond footer-declared size"), exception.getMessage());
  }

  @Test
  public void testGzipDecodeRejectsZeroSizeForNonEmptyStream() throws Exception {
    ByteBuffer encoded = encodeGzip(new byte[256]);
    encoded.putInt(encoded.limit() - Integer.BYTES, 0);
    IOException exception = expectThrows(IOException.class,
        () -> GzipCodecDefinition.INSTANCE.decode(GzipCodecDefinition.OPTIONS, CTX, encoded));
    assertTrue(exception.getMessage().contains("expands beyond footer-declared size"), exception.getMessage());
  }

  @Test
  public void testGzipDecodeRejectsCorruptChecksum() throws Exception {
    ByteBuffer encoded = encodeGzip(new byte[256]);
    int checksumByte = encoded.limit() - Integer.BYTES - 1;
    encoded.put(checksumByte, (byte) (encoded.get(checksumByte) ^ 0x01));
    IOException exception = expectThrows(IOException.class,
        () -> GzipCodecDefinition.INSTANCE.decode(GzipCodecDefinition.OPTIONS, CTX, encoded));
    assertTrue(exception.getMessage().contains("GZIP decompression failed"), exception.getMessage());
  }

  @Test
  public void testGzipDecodeReadsBigEndianFooterFromLittleEndianView()
      throws Exception {
    byte[] values = new byte[256];
    Arrays.fill(values, (byte) 0x5A);
    ByteBuffer encoded = encodeGzip(values).order(ByteOrder.LITTLE_ENDIAN);

    assertDecodedEquals(values,
        GzipCodecDefinition.INSTANCE.decode(GzipCodecDefinition.OPTIONS, CTX, encoded));
  }

  @Test
  public void testGzipDecodeHonorsNonZeroSourcePosition()
      throws Exception {
    byte[] values = new byte[256];
    Arrays.fill(values, (byte) 0x6B);
    ByteBuffer encoded = encodeGzip(values);
    int prefixLength = 7;
    ByteBuffer framed = ByteBuffer.allocateDirect(prefixLength + encoded.remaining());
    framed.position(prefixLength);
    framed.put(encoded.duplicate()).flip();
    framed.position(prefixLength);

    assertDecodedEquals(values,
        GzipCodecDefinition.INSTANCE.decode(GzipCodecDefinition.OPTIONS, CTX, framed));
  }

  @Test
  public void testGzipMaxEncodedSizeRejectsOverflow() {
    assertThrows(IllegalArgumentException.class,
        () -> GzipCodecDefinition.INSTANCE.maxEncodedSize(GzipCodecDefinition.OPTIONS, Integer.MAX_VALUE));
  }

  @Test
  public void testMultiStageDecodeBoundsInnerSnappyOutput() throws Exception {
    // A valid Snappy frame that expands to 1 MiB is far larger than the four-byte final INT
    // chunk declared by the outer format. The executor must use Snappy.decodeInto() with a
    // four-byte scratch bound instead of letting Snappy.decode() allocate from its inner header.
    byte[] compressed = Snappy.compress(new byte[1024 * 1024]);
    // Keep only a small prefix: it contains Snappy's claimed decoded length and stays below the
    // executor's outer encoded-size bound, so this specifically exercises the inner-frame guard.
    ByteBuffer encoded = ByteBuffer.allocateDirect(16);
    encoded.put(compressed, 0, encoded.capacity()).flip();
    ByteBuffer decoded = ByteBuffer.allocateDirect(Integer.BYTES);
    CodecPipelineExecutor executor = CodecPipelineExecutor.create(
        "DELTA,SNAPPY", CTX, CodecRegistry.DEFAULT);

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> executor.decode(encoded, decoded, Integer.BYTES));
    assertTrue(exception.getMessage().contains("Snappy: decompressed size"), exception.getMessage());
  }

  @Test
  public void testBoundedExecutorRejectsOversizedOuterGzipFrameBeforeCopy() {
    ByteBuffer encoded = garbage(64);
    ByteBuffer decoded = ByteBuffer.allocateDirect(Integer.BYTES);
    CodecPipelineExecutor executor = CodecPipelineExecutor.create("GZIP", CTX, CodecRegistry.DEFAULT);

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> executor.decode(encoded, decoded, Integer.BYTES, 128));
    assertTrue(exception.getMessage().contains("Encoded input size"), exception.getMessage());
  }

  private static ByteBuffer encodeGzip(byte[] values) throws Exception {
    ByteBuffer source = ByteBuffer.allocateDirect(values.length);
    source.put(values).flip();
    return GzipCodecDefinition.INSTANCE.encode(GzipCodecDefinition.OPTIONS, CTX, source);
  }

  private static void assertDecodedEquals(byte[] expected, ByteBuffer decoded) {
    byte[] actual = new byte[decoded.remaining()];
    decoded.get(actual);
    assertTrue(Arrays.equals(actual, expected));
  }
}
