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
package org.apache.pinot.segment.local.segment.index.forward;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.io.codec.CodecPipelineExecutor;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriterV7;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReaderV7;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Freezes the V7 outer framing independently from the reader and writer implementations. The
/// transform-only DELTA fixture is hand-authored from the documented big-endian wire layout, so it
/// has no native-compressor-version dependency.
public class FixedByteChunkSVForwardIndexReaderV7GoldenTest implements PinotBuffersAfterMethodCheckRule {
  private static final String GOLDEN_DELTA_INT =
      "AAAAB8DewN4AAAABAAAABAAAAAQAAAADAAAABQAAACVERUxUQQAAAAAAAAAtAAAADAAAAAwAAAAKAAAAAwAAAAc=";

  @Test
  public void testGoldenV7FramingAndValues()
      throws Exception {
    byte[] goldenBytes = Base64.getDecoder().decode(GOLDEN_DELTA_INT);
    File fixtureFile = new File(FileUtils.getTempDirectory(), "FixedByteChunkSVForwardIndexReaderV7Golden.fwd");
    File generatedFile =
        new File(FileUtils.getTempDirectory(), "FixedByteChunkSVForwardIndexReaderV7Golden_generated.fwd");
    FileUtils.deleteQuietly(fixtureFile);
    FileUtils.deleteQuietly(generatedFile);
    try {
      Files.write(fixtureFile.toPath(), goldenBytes);
      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(fixtureFile)) {
        assertEquals(buffer.getInt(0), FixedByteChunkForwardIndexWriterV7.VERSION);
        assertEquals(buffer.getInt(Integer.BYTES), FixedByteChunkForwardIndexWriterV7.FORMAT_MAGIC);
        assertEquals(buffer.getInt(2L * Integer.BYTES), 1);
        assertEquals(buffer.getInt(3L * Integer.BYTES), 4);
        assertEquals(buffer.getInt(4L * Integer.BYTES), Integer.BYTES);
        assertEquals(buffer.getInt(5L * Integer.BYTES), 3);
        assertEquals(buffer.getInt(6L * Integer.BYTES), 5);
        assertEquals(buffer.getInt(7L * Integer.BYTES), 37);
        byte[] specBytes = new byte[5];
        buffer.copyTo(FixedByteChunkForwardIndexWriterV7.FIXED_HEADER_BYTES, specBytes);
        assertEquals(new String(specBytes, StandardCharsets.UTF_8), "DELTA");
        assertEquals(buffer.getLong(37), 45L);
        assertEquals(buffer.getInt(45), 12);
        assertEquals(buffer.getInt(49), 12);

        try (FixedByteChunkSVForwardIndexReaderV7 reader =
            new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.INT, 3);
            ChunkReaderContext context = reader.createContext()) {
          assertEquals(reader.getCodecSpec(), "DELTA");
          assertEquals(reader.getInt(0, context), 10);
          assertEquals(reader.getInt(1, context), 13);
          assertEquals(reader.getInt(2, context), 20);
        }
      }

      CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTA", DataType.INT);
      try (FixedByteChunkForwardIndexWriterV7 writer =
          new FixedByteChunkForwardIndexWriterV7(generatedFile, executor, 3, 4, Integer.BYTES)) {
        writer.putInt(10);
        writer.putInt(13);
        writer.putInt(20);
      }
      assertEquals(Files.readAllBytes(generatedFile.toPath()), goldenBytes);
    } finally {
      FileUtils.deleteQuietly(fixtureFile);
      FileUtils.deleteQuietly(generatedFile);
    }
  }
}
