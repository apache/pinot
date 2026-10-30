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
package org.apache.pinot.segment.local.io.writer.impl;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies chunk write failures using a separately owned file and a real closed output channel.
public class BaseChunkForwardIndexWriterTest {
  @Test
  public void testClosedOutputPreservesTheWriteFailureAndPreviousChunk()
      throws Exception {
    Path index = Files.createTempFile("closed-chunk-writer", ".raw");
    try {
      FixedByteChunkForwardIndexWriter writer =
          new FixedByteChunkForwardIndexWriter(index.toFile(), ChunkCompressionType.PASS_THROUGH, 2, 1,
              Integer.BYTES, 3);
      try {
        long firstChunkOffset = writer._dataOffset;
        writer.putInt(7);
        writer._dataFile.close();

        RuntimeException failure = expectThrows(RuntimeException.class, () -> writer.putInt(9));
        assertTrue(failure.getCause() instanceof ClosedChannelException);
        try (FileChannel input = FileChannel.open(index, StandardOpenOption.READ)) {
          ByteBuffer previousChunk = ByteBuffer.allocate(Integer.BYTES);
          assertEquals(input.read(previousChunk, firstChunkOffset), Integer.BYTES);
          previousChunk.flip();
          assertEquals(previousChunk.getInt(), 7);
        }
      } finally {
        // The deliberately closed output cannot be flushed by writer.close(). Release its remaining owned resource.
        writer._dataFile.close();
        writer._chunkCompressor.close();
      }
    } finally {
      Files.deleteIfExists(index);
    }
  }
}
