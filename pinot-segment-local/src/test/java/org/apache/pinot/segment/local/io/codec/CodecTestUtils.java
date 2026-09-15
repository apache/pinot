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


/// Test-owned buffers for codec fixtures. Decoded sizes must come from the fixture, never its frame header.
final class CodecTestUtils {
  private CodecTestUtils() {
  }

  static ByteBuffer encode(CodecPipelineExecutor executor, ByteBuffer source) throws IOException {
    try (CodecPipelineExecutor.EncodeScratch scratch = new CodecPipelineExecutor.EncodeScratch()) {
      ByteBuffer encoded = executor.encode(source, Integer.MAX_VALUE, Long.MAX_VALUE, scratch);
      return ByteBuffer.allocate(encoded.remaining()).put(encoded).flip();
    }
  }

  static void decode(CodecPipelineExecutor executor, ByteBuffer source, ByteBuffer destination, int decodedSize)
      throws IOException {
    decode(executor, source, destination, decodedSize, Integer.MAX_VALUE);
  }

  static void decode(CodecPipelineExecutor executor, ByteBuffer source, ByteBuffer destination, int decodedSize,
      int maxStageSize) throws IOException {
    try (CodecPipelineExecutor.DecodeScratch scratch = new CodecPipelineExecutor.DecodeScratch()) {
      executor.decode(source, destination, decodedSize, maxStageSize, Long.MAX_VALUE, scratch);
    }
  }
}
