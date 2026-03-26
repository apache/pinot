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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV6;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkWriter;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV4;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV6;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for V6 forward index format. Inherits all write/read tests from {@link VarByteChunkV5Test}
 * and adds a compression ratio validation test for delta-encoded chunk headers.
 */
public class VarByteChunkV6Test extends VarByteChunkV5Test {
  private static final Random RANDOM = new Random();

  @Override
  protected String getTestDirName() {
    return "VarByteChunkV6Test";
  }

  @Override
  protected VarByteChunkWriter createWriter(File file, ChunkCompressionType compressionType, int chunkSize)
      throws IOException {
    return new VarByteChunkForwardIndexWriterV6(file, compressionType, chunkSize);
  }

  @Override
  protected VarByteChunkForwardIndexReaderV4 createReader(PinotDataBuffer buffer, FieldSpec.DataType dataType,
      boolean isSingleValue) {
    return new VarByteChunkForwardIndexReaderV6(buffer, dataType, isSingleValue);
  }

  @Test
  public void validateCompressionRatioIncrease()
      throws IOException {
    int numDocs = 1000000;
    int maxMVRowSize = 0;
    int valueCounter = 0;
    List<long[]> inputData = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      long[] mvRow = new long[Math.abs((int) Math.floor(RANDOM.nextGaussian()))];
      maxMVRowSize = Math.max(maxMVRowSize, mvRow.length);
      for (int j = 0; j < mvRow.length; j++) {
        mvRow[j] = valueCounter++ % 10;
      }
      inputData.add(mvRow);
    }

    File v4FwdIndexFile = new File(FileUtils.getTempDirectory(), "v6test_v4");
    File v6FwdIndexFile = new File(FileUtils.getTempDirectory(), "v6test_v6");
    FileUtils.deleteQuietly(v4FwdIndexFile);
    FileUtils.deleteQuietly(v6FwdIndexFile);

    try (MultiValueFixedByteRawIndexCreator creator = new MultiValueFixedByteRawIndexCreator(v4FwdIndexFile,
        ChunkCompressionType.ZSTANDARD, numDocs, FieldSpec.DataType.LONG, maxMVRowSize, true, 4)) {
      for (long[] mvRow : inputData) {
        creator.putLongMV(mvRow);
      }
    }
    try (MultiValueFixedByteRawIndexCreator creator = new MultiValueFixedByteRawIndexCreator(v6FwdIndexFile,
        ChunkCompressionType.ZSTANDARD, numDocs, FieldSpec.DataType.LONG, maxMVRowSize, true, 6)) {
      for (long[] mvRow : inputData) {
        creator.putLongMV(mvRow);
      }
    }

    // V6 should be smaller than V4 due to delta-encoded header compressing better
    Assert.assertTrue(v6FwdIndexFile.length() < v4FwdIndexFile.length(),
        "V6 (" + v6FwdIndexFile.length() + ") should be smaller than V4 (" + v4FwdIndexFile.length() + ")");

    FileUtils.deleteQuietly(v4FwdIndexFile);
    FileUtils.deleteQuietly(v6FwdIndexFile);
  }
}
