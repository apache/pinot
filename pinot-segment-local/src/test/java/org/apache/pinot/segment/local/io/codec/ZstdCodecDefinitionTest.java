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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ZstdCodecDefinitionTest {
  private static final ZstdCodecDefinition CODEC = ZstdCodecDefinition.INSTANCE;
  private static final ZstdCodecDefinition.Options OPTIONS = CODEC.parseOptions(List.of());
  private static final CodecContext CONTEXT = new CodecContext(DataType.INT);

  @Test
  public void testEmptyInputRoundTrip() throws Exception {
    ByteBuffer encoded = CODEC.encode(OPTIONS, CONTEXT, ByteBuffer.allocateDirect(0));
    assertTrue(encoded.hasRemaining(), "An empty input should still produce a Zstd frame");

    ByteBuffer decoded = CODEC.decode(OPTIONS, CONTEXT, encoded.duplicate());
    assertEquals(decoded.remaining(), 0);

    ByteBuffer destination = ByteBuffer.allocateDirect(0);
    CODEC.decodeInto(OPTIONS, CONTEXT, encoded.duplicate(), destination);
    assertEquals(destination.position(), 0);
    assertEquals(destination.limit(), 0);
    assertEquals(destination.remaining(), 0);
  }
}
