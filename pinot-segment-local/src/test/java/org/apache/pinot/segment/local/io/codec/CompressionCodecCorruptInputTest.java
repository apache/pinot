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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;


/// Negative-path coverage for the compression codecs' corrupt-frame defenses (length-prefix /
/// footer / sanity-cap branches). Happy-path round-trips are covered by `CodecPipelineForwardIndexTest`.
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
  public void testGzipDecodeRejectsGarbage() {
    assertThrows(Exception.class,
        () -> GzipCodecDefinition.INSTANCE.decode(GzipCodecDefinition.OPTIONS, CTX, garbage(64)));
  }

  @Test
  public void testGzipDecodeRejectsTruncatedFooter() {
    // Fewer than 4 bytes cannot carry the uncompressed-size footer GZIP appends.
    assertThrows(Exception.class,
        () -> GzipCodecDefinition.INSTANCE.decode(GzipCodecDefinition.OPTIONS, CTX, garbage(2)));
  }
}
