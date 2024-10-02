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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueFixedByteRawIndexCreatorV2;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkMVForwardIndexReaderV2;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV5;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 Similar to MultiValueFixedByteRawIndexCreatorTest, but utilizes the newer version of the forward index creator and
 reader. Additionally, this test class includes a validation test for checking the compression ratio improvement with
 the  forward index creator version upgrade.
 */
public class MultiValueFixedByteRawIndexCreatorV2Test extends MultiValueFixedByteRawIndexCreatorTest {
  @BeforeClass
  public void setup()
      throws Exception {
    _outputDir = System.getProperty("java.io.tmpdir") + File.separator + "mvFixedRawV2Test";
    FileUtils.forceMkdir(new File(_outputDir));
  }

  @Override
  public MultiValueFixedByteRawIndexCreator getMultiValueFixedByteRawIndexCreator(
      ChunkCompressionType compressionType, String column, int numDocs, FieldSpec.DataType dataType, int maxElements,
      int writerVersion)
      throws IOException {
    return new MultiValueFixedByteRawIndexCreatorV2(new File(_outputDir), compressionType, column, numDocs, dataType,
        maxElements, false, writerVersion, 1024 * 1024, 1000);
  }

  @Override
  public ForwardIndexReader getForwardIndexReader(PinotDataBuffer buffer, FieldSpec.DataType dataType,
      int writerVersion) {
    return writerVersion == VarByteChunkForwardIndexWriterV4.VERSION ? new VarByteChunkForwardIndexReaderV5(buffer,
        dataType.getStoredType(), false) : new FixedByteChunkMVForwardIndexReaderV2(buffer, dataType.getStoredType());
  }

  @Test
  public void validateCompressionRatioIncrease()
      throws IOException {
    // Generate input data containing short MV docs with somewhat repetitive data
    int numDocs = 1000000;
    int numElements = 0;
    int maxMVRowSize = 0;
    List<long[]> inputData = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      long[] mvRow = new long[Math.abs((int) Math.floor(RANDOM.nextGaussian()))];
      maxMVRowSize = Math.max(maxMVRowSize, mvRow.length);
      numElements += mvRow.length;
      for (int j = 0; j < mvRow.length; j++, numElements++) {
        mvRow[j] = numElements % 10;
      }
      inputData.add(mvRow);
    }

    for (int writerVersion : List.of(2, 4)) {
      // Generate MV fixed byte raw fwd index with explicit length
      File explicitLengthFwdIndexFile = new File(_outputDir, MultiValueFixedByteRawIndexCreator.class.getSimpleName());
      FileUtils.deleteQuietly(explicitLengthFwdIndexFile);
      try (MultiValueFixedByteRawIndexCreator creator = new MultiValueFixedByteRawIndexCreator(
          explicitLengthFwdIndexFile, ChunkCompressionType.ZSTANDARD, numDocs, FieldSpec.DataType.LONG, numElements,
          true, writerVersion)) {
        for (long[] mvRow : inputData) {
          creator.putLongMV(mvRow);
        }
      }

      // Generate MV fixed byte raw fwd index with implicit length
      File implicitLengthFwdIndexFile =
          new File(_outputDir, MultiValueFixedByteRawIndexCreatorV2.class.getSimpleName());
      FileUtils.deleteQuietly(implicitLengthFwdIndexFile);
      try (MultiValueFixedByteRawIndexCreatorV2 creator = new MultiValueFixedByteRawIndexCreatorV2(
          implicitLengthFwdIndexFile, ChunkCompressionType.ZSTANDARD, numDocs, FieldSpec.DataType.LONG, numElements,
          true, writerVersion)) {
        for (long[] mvRow : inputData) {
          creator.putLongMV(mvRow);
        }
      }

      // For the input data, the explicit length compressed MV fixed byte raw forward index is expected to be:
      // 1. At least 15% larger than the implicit length variant when using Writer Version 2
      // 2. At least 200% larger than the implicit length variant when using Writer Version 4
      long expectedImplicitLengthFwdIndexMaxSize;
      if (writerVersion == 2) {
        expectedImplicitLengthFwdIndexMaxSize = Math.round(implicitLengthFwdIndexFile.length() * 1.15d);
      } else {
        expectedImplicitLengthFwdIndexMaxSize = Math.round(implicitLengthFwdIndexFile.length() * 2.0d);
      }
      Assert.assertTrue(expectedImplicitLengthFwdIndexMaxSize < explicitLengthFwdIndexFile.length());
    }
  }
}
