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
package org.apache.pinot.segment.spi.creator;

import java.io.IOException;
import org.apache.pinot.segment.spi.index.IndexingOverrides;
import org.apache.pinot.segment.spi.index.creator.BloomFilterCreator;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class IndexingOverridesTest {

  @Test
  public void indexingOverridesLoadableWithoutDefaultImplementation()
      throws IOException {
    BloomFilterCreator mockBloomFilterCreator = mock(BloomFilterCreator.class);
    BloomFilterReader mockBloomFilterReader = mock(BloomFilterReader.class);
    assertTrue(IndexingOverrides.registerProvider(new IndexingOverrides.Default() {
      @Override
      public BloomFilterCreator newBloomFilterCreator(IndexCreationContext.BloomFilter context) {
        return mockBloomFilterCreator;
      }

      @Override
      public BloomFilterReader newBloomFilterReader(PinotDataBuffer dataBuffer, boolean onHeap) {
        return mockBloomFilterReader;
      }
    }));
    // it's ok to load external overrides without an internal implementation present, e.g. for testing
    assertEquals(mockBloomFilterCreator, IndexingOverrides.getIndexCreatorProvider()
        .newBloomFilterCreator(mock(IndexCreationContext.BloomFilter.class)));
    assertEquals(mockBloomFilterReader, IndexingOverrides.getIndexReaderProvider()
        .newBloomFilterReader(mock(PinotDataBuffer.class), false));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void whenDefaultImplementationMissingThrowUnsupportedOperationExceptionCreator()
      throws IOException {
    // the implementation is missing so no indexes will be created anyway...
    new IndexingOverrides.Default().newBloomFilterCreator(mock(IndexCreationContext.BloomFilter.class));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void whenDefaultImplementationMissingThrowUnsupportedOperationExceptionReader()
      throws IOException {
    // the implementation is missing so no indexes will be created anyway...
    new IndexingOverrides.Default().newBloomFilterReader(mock(PinotDataBuffer.class), true);
  }
}
