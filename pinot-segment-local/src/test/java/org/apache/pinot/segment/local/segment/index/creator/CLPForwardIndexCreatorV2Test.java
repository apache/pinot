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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.CLPForwardIndexCreatorV2;
import org.apache.pinot.segment.local.segment.index.forward.mutable.VarByteSVMutableForwardIndexTest;
import org.apache.pinot.segment.local.segment.index.readers.forward.CLPForwardIndexReaderV2;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CLPForwardIndexCreatorV2Test {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), CLPForwardIndexCreatorV2Test.class.getSimpleName());
  private PinotDataBufferMemoryManager _memoryManager;
  private List<String> _logMessages = new ArrayList<>();

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    _memoryManager = new DirectMemoryManager(VarByteSVMutableForwardIndexTest.class.getName());

    ObjectMapper objectMapper = new ObjectMapper();
    try (GzipCompressorInputStream gzipInputStream = new GzipCompressorInputStream(
        getClass().getClassLoader().getResourceAsStream("data/log.jsonl.gz"));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gzipInputStream))) {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        JsonNode jsonNode = objectMapper.readTree(line);
        _logMessages.add(jsonNode.get("message").asText());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
  }

  @Test
  public void testCLPWriter()
      throws IOException {
    // Create and ingest into a clp mutable forward indexes
    CLPMutableForwardIndexV2 clpMutableForwardIndexV2 = new CLPMutableForwardIndexV2("column1", _memoryManager);
    int rawSizeBytes = 0;
    for (int i = 0; i < _logMessages.size(); i++) {
      clpMutableForwardIndexV2.setString(i, _logMessages.get(i));
      rawSizeBytes += _logMessages.get(i).length();
    }

    validateImmutableForwardIndex(clpMutableForwardIndexV2, ChunkCompressionType.LZ4, rawSizeBytes, 40);
    validateImmutableForwardIndex(clpMutableForwardIndexV2, ChunkCompressionType.ZSTANDARD, rawSizeBytes, 66);
  }

  private void validateImmutableForwardIndex(CLPMutableForwardIndexV2 clpMutableForwardIndexV2,
      ChunkCompressionType compressor, int rawSizeBytes, float minCompressionRatio)
      throws IOException {
    // Create a immutable forward index from mutable forward index
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    CLPForwardIndexCreatorV2 clpForwardIndexCreatorV2 =
        new CLPForwardIndexCreatorV2(TEMP_DIR, clpMutableForwardIndexV2, compressor);
    for (int i = 0; i < _logMessages.size(); i++) {
      clpForwardIndexCreatorV2.putString(clpMutableForwardIndexV2.getString(i));
    }
    clpForwardIndexCreatorV2.seal();
    clpForwardIndexCreatorV2.close();

    // Read from immutable forward index and validate the content
    File indexFile = new File(TEMP_DIR, "column1" + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    PinotDataBuffer pinotDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    CLPForwardIndexReaderV2 clpForwardIndexReaderV2 = new CLPForwardIndexReaderV2(pinotDataBuffer, _logMessages.size());
    CLPForwardIndexReaderV2.CLPReaderContext clpForwardIndexReaderV2Context = clpForwardIndexReaderV2.createContext();
    for (int i = 0; i < _logMessages.size(); i++) {
      Assert.assertEquals(clpForwardIndexReaderV2.getString(i, clpForwardIndexReaderV2Context), _logMessages.get(i));
    }

    // We expect to achieve a compression ratio >=66x with default configuration
    float compressionRatio = (float) rawSizeBytes / indexFile.length();
    Assert.assertTrue(compressionRatio >= minCompressionRatio);
  }
}
