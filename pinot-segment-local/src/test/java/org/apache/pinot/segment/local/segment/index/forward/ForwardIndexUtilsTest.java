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

import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV6;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.ForwardIndexUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ForwardIndexUtilsTest {

  @Test(dataProvider = "dynamicTargetChunkSizeProvider")
  public void testDynamicTargetChunkSize(Integer maxLength, Integer targetDocsPerChunk, Integer targetMaxChunkSizeBytes,
      Integer expectedChunkSize) {
    int chunkSize = ForwardIndexUtils.getDynamicTargetChunkSize(maxLength, targetDocsPerChunk, targetMaxChunkSizeBytes);
    assertEquals(chunkSize, expectedChunkSize);
  }

  @DataProvider(name = "dynamicTargetChunkSizeProvider")
  public Integer[][] dynamicTargetChunkSizeProvider() {
    return new Integer[][]{
        {100, 1000, 1024 * 1024, 1000 * 100}, // small maxValue returns dynamic chunk
        {100, Integer.MAX_VALUE, 1024 * 1024, 1024 * 1024}, // overflow falls back to targetMaxChunkSizeBytes
        {100, -1, 1024 * 1024, 1024 * 1024}, // negative targetDocsPerChunk falls back to targetMaxChunkSizeBytes
        {2000, 1000, 1024 * 1024, 1024 * 1024}, // large maxValue limited by targetMaxChunkSizeBytes
        {100, -1, 1024, 4 * 1024} // tiny targetMaxChunkSizeBytes falls back to TARGET_MIN_CHUNK_SIZE
    };
  }

  @Test(dataProvider = "docsPerChunkCapProvider")
  public void testDocsPerChunkCap(Integer targetDocsPerChunk, Boolean enforce, Integer expectedCap) {
    assertEquals(ForwardIndexUtils.getDocsPerChunkCap(targetDocsPerChunk, enforce), (int) expectedCap);
  }

  @DataProvider(name = "docsPerChunkCapProvider")
  public Object[][] docsPerChunkCapProvider() {
    int noCap = VarByteChunkForwardIndexWriterV6.DISABLE_DOCS_PER_CHUNK;
    return new Object[][]{
        {250, true, 250}, // enforced: the target becomes a hard cap
        {250, false, noCap}, // not enforced: no cap, byte-driven flush only
        {-1, true, noCap}, // enforcing a disabled target is still no cap
        {0, true, noCap} // a zero target would stall the writer, so treat it as no cap
    };
  }
}
