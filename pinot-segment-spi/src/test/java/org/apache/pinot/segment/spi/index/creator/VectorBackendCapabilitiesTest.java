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
package org.apache.pinot.segment.spi.index.creator;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link VectorBackendCapabilities} and its integration with {@link VectorBackendType}.
 */
public class VectorBackendCapabilitiesTest {

  @Test
  public void testHnswCapabilities() {
    VectorBackendCapabilities caps = VectorBackendType.HNSW.getCapabilities();
    assertNotNull(caps);
    assertTrue(caps.supportsTopKAnn());
    assertTrue(caps.supportsFilterAwareSearch());
    assertFalse(caps.supportsApproximateRadius());
    assertTrue(caps.supportsExactRerank());
    assertFalse(caps.supportsRuntimeSearchParams());
  }

  @Test
  public void testIvfFlatCapabilities() {
    VectorBackendCapabilities caps = VectorBackendType.IVF_FLAT.getCapabilities();
    assertNotNull(caps);
    assertTrue(caps.supportsTopKAnn());
    assertTrue(caps.supportsFilterAwareSearch());
    assertFalse(caps.supportsApproximateRadius());
    assertTrue(caps.supportsExactRerank());
    assertTrue(caps.supportsRuntimeSearchParams());
  }

  @Test
  public void testIvfPqCapabilities() {
    VectorBackendCapabilities caps = VectorBackendType.IVF_PQ.getCapabilities();
    assertNotNull(caps);
    assertTrue(caps.supportsTopKAnn());
    assertTrue(caps.supportsFilterAwareSearch());
    assertFalse(caps.supportsApproximateRadius());
    assertTrue(caps.supportsExactRerank());
    assertTrue(caps.supportsRuntimeSearchParams());
  }

  @Test
  public void testConsistencyWithLegacyMethods() {
    // Verify that capabilities are consistent with the existing VectorBackendType methods
    for (VectorBackendType type : VectorBackendType.values()) {
      VectorBackendCapabilities caps = type.getCapabilities();
      assertEquals(caps.supportsRuntimeSearchParams(), type.supportsNprobe(),
          "supportsRuntimeSearchParams should match supportsNprobe for " + type);
    }
  }

  @Test
  public void testBuilderDefaults() {
    VectorBackendCapabilities caps = new VectorBackendCapabilities.Builder().build();
    assertFalse(caps.supportsTopKAnn());
    assertFalse(caps.supportsFilterAwareSearch());
    assertFalse(caps.supportsApproximateRadius());
    assertFalse(caps.supportsExactRerank());
    assertFalse(caps.supportsRuntimeSearchParams());
  }

  @Test
  public void testBuilderCustom() {
    VectorBackendCapabilities caps = new VectorBackendCapabilities.Builder()
        .supportsTopKAnn(true)
        .supportsFilterAwareSearch(true)
        .supportsApproximateRadius(true)
        .supportsExactRerank(true)
        .supportsRuntimeSearchParams(true)
        .build();
    assertTrue(caps.supportsTopKAnn());
    assertTrue(caps.supportsFilterAwareSearch());
    assertTrue(caps.supportsApproximateRadius());
    assertTrue(caps.supportsExactRerank());
    assertTrue(caps.supportsRuntimeSearchParams());
  }

  @Test
  public void testToString() {
    VectorBackendCapabilities caps = VectorBackendType.HNSW.getCapabilities();
    String str = caps.toString();
    assertTrue(str.contains("topKAnn=true"));
    assertTrue(str.contains("filterAwareSearch=true"));
    assertTrue(str.contains("runtimeSearchParams=false"));
  }

  @Test
  public void testAllBackendsHaveCapabilities() {
    for (VectorBackendType type : VectorBackendType.values()) {
      assertNotNull(type.getCapabilities(), "Capabilities must not be null for " + type);
      assertTrue(type.getCapabilities().supportsTopKAnn(),
          "All current backends should support top-K ANN for " + type);
    }
  }
}
