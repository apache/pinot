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
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV5;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkWriter;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV4;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV5;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for V5 forward index format. Inherits all write/read tests from {@link VarByteChunkV4Test}
 * and adds a compression ratio validation test for implicit-length MV encoding.
 */
public class VarByteChunkV5Test extends VarByteChunkV4Test implements PinotBuffersAfterClassCheckRule {
  private static final Random RANDOM = new Random();

  @Override
  protected String getTestDirName() {
    return "VarByteChunkV5Test";
  }

  @Override
  protected VarByteChunkWriter createWriter(File file, ChunkCompressionType compressionType, int chunkSize)
      throws IOException {
    return new VarByteChunkForwardIndexWriterV5(file, compressionType, chunkSize);
  }

  @Override
  protected VarByteChunkForwardIndexReaderV4 createReader(PinotDataBuffer buffer, FieldSpec.DataType dataType,
      boolean isSingleValue) {
    return new VarByteChunkForwardIndexReaderV5(buffer, dataType, isSingleValue);
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

    // Generate MV fixed byte raw fwd index with explicit length (V4)
    File explicitLengthFwdIndexFile = new File(FileUtils.getTempDirectory(), "v5test_v4");
    FileUtils.deleteQuietly(explicitLengthFwdIndexFile);
    try (MultiValueFixedByteRawIndexCreator creator = new MultiValueFixedByteRawIndexCreator(explicitLengthFwdIndexFile,
        ChunkCompressionType.ZSTANDARD, numDocs, FieldSpec.DataType.LONG, maxMVRowSize, true, 4)) {
      for (long[] mvRow : inputData) {
        creator.putLongMV(mvRow);
      }
    }

    // Generate MV fixed byte raw fwd index with implicit length (V5)
    File implicitLengthFwdIndexFile = new File(FileUtils.getTempDirectory(), "v5test_v5");
    FileUtils.deleteQuietly(implicitLengthFwdIndexFile);
    try (MultiValueFixedByteRawIndexCreator creator = new MultiValueFixedByteRawIndexCreator(implicitLengthFwdIndexFile,
        ChunkCompressionType.ZSTANDARD, numDocs, FieldSpec.DataType.LONG, maxMVRowSize, true, 5)) {
      for (long[] mvRow : inputData) {
        creator.putLongMV(mvRow);
      }
    }

    // V5 implicit length should be at least 2x smaller than V4 explicit length
    long expectedImplicitLengthFwdIndexMaxSize = Math.round(implicitLengthFwdIndexFile.length() * 2.0d);
    Assert.assertTrue(expectedImplicitLengthFwdIndexMaxSize < explicitLengthFwdIndexFile.length());

    FileUtils.deleteQuietly(explicitLengthFwdIndexFile);
    FileUtils.deleteQuietly(implicitLengthFwdIndexFile);
  }
}
