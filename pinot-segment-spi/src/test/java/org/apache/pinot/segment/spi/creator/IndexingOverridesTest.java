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
import org.apache.pinot.segment.spi.index.mutable.MutableTextIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class IndexingOverridesTest {

  @Test
  public void indexingOverridesLoadableWithoutDefaultImplementation() {
    MutableTextIndex mockMutableTextIndex = mock(MutableTextIndex.class);
    assertTrue(IndexingOverrides.registerProvider(new IndexingOverrides.Default() {
      @Override
      public MutableTextIndex newTextIndex(MutableIndexContext.Text context) {
        return mockMutableTextIndex;
      }
    }));
    // it's ok to load external overrides without an internal implementation present, e.g. for testing
    assertSame(mockMutableTextIndex, IndexingOverrides.getMutableIndexProvider()
        .newTextIndex(mock(MutableIndexContext.Text.class)));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void whenDefaultImplementationMissingThrowUnsupportedOperationExceptionCreator()
      throws IOException {
    // the implementation is missing so no indexes will be created anyway...
    new IndexingOverrides.Default().newTextIndex(mock(MutableIndexContext.Text.class));
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void whenDefaultImplementationMissingThrowUnsupportedOperationExceptionReader()
      throws IOException {
    // the implementation is missing so no indexes will be created anyway...
    new IndexingOverrides.Default().newTextIndex(mock(MutableIndexContext.Text.class));
  }
}
