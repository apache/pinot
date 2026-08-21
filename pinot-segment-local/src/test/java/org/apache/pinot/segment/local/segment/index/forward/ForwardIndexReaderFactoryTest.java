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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.codec.CodecPipelineExecutor;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriterV7;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReaderV7;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Regression coverage for V7 reader dispatch. A V7 segment on disk must be served through the
/// full [SegmentDirectory.Reader] entry point regardless of the config it is paired with.
public class ForwardIndexReaderFactoryTest {
  private static final String COLUMN_NAME = "testCol";

  @Test
  public void testV7SegmentIsServedThroughSegmentReaderEntryPoint()
      throws Exception {
    File indexFile = new File(FileUtils.getTempDirectory(), "ForwardIndexReaderFactoryTest_v7.fwd");
    FileUtils.deleteQuietly(indexFile);
    try {
      CodecPipelineExecutor executor = CodecPipelineExecutor.create("DELTA,LZ4", DataType.INT);
      try (FixedByteChunkForwardIndexWriterV7 writer =
          new FixedByteChunkForwardIndexWriterV7(indexFile, executor, 100, 32, Integer.BYTES)) {
        for (int i = 0; i < 100; i++) {
          writer.putInt(i * 3);
        }
      }

      ForwardIndexConfig config = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW).build();
      FieldIndexConfigs fieldIndexConfigs = new FieldIndexConfigs.Builder()
          .add(StandardIndexes.forward(), config)
          .build();
      ColumnMetadata metadata = Mockito.mock(ColumnMetadata.class);
      Mockito.when(metadata.getColumnName()).thenReturn(COLUMN_NAME);
      Mockito.when(metadata.getForwardIndexEncoding()).thenReturn(FieldConfig.EncodingType.RAW);
      Mockito.when(metadata.isSingleValue()).thenReturn(true);
      Mockito.when(metadata.getDataType()).thenReturn(DataType.INT);
      Mockito.when(metadata.getTotalDocs()).thenReturn(100);

      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
        SegmentDirectory.Reader segmentReader = Mockito.mock(SegmentDirectory.Reader.class);
        Mockito.when(segmentReader.hasIndexFor(COLUMN_NAME, StandardIndexes.forward())).thenReturn(true);
        Mockito.when(segmentReader.getIndexFor(COLUMN_NAME, StandardIndexes.forward())).thenReturn(buffer);

        try (ForwardIndexReader<?> reader = ForwardIndexReaderFactory.getInstance()
            .createIndexReader(segmentReader, fieldIndexConfigs, metadata)) {
          assertTrue(reader instanceof FixedByteChunkSVForwardIndexReaderV7,
              "V7 segment was routed to " + reader.getClass().getSimpleName());
          assertEquals(reader.getCodecSpec(), "DELTA,LZ4");
        }

        Mockito.when(metadata.getTotalDocs()).thenReturn(101);
        RuntimeException exception = expectThrows(RuntimeException.class,
            () -> ForwardIndexReaderFactory.getInstance().createIndexReader(segmentReader, fieldIndexConfigs,
                metadata));
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
        assertTrue(exception.getCause().getMessage().contains("totalDocs=100"));
        assertTrue(exception.getCause().getMessage().contains("metadata totalDocs=101"));
      }
    } finally {
      FileUtils.deleteQuietly(indexFile);
    }
  }
}
