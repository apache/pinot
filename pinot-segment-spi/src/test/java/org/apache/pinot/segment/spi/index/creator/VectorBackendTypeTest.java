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
import static org.testng.Assert.assertTrue;


/**
 * Tests for the {@link VectorBackendType} enum.
 */
public class VectorBackendTypeTest {

  @Test
  public void testFromStringHnsw() {
    assertEquals(VectorBackendType.fromString("HNSW"), VectorBackendType.HNSW);
    assertEquals(VectorBackendType.fromString("hnsw"), VectorBackendType.HNSW);
    assertEquals(VectorBackendType.fromString("Hnsw"), VectorBackendType.HNSW);
  }

  @Test
  public void testFromStringIvfFlat() {
    assertEquals(VectorBackendType.fromString("IVF_FLAT"), VectorBackendType.IVF_FLAT);
    assertEquals(VectorBackendType.fromString("ivf_flat"), VectorBackendType.IVF_FLAT);
    assertEquals(VectorBackendType.fromString("Ivf_Flat"), VectorBackendType.IVF_FLAT);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*Unknown vector backend type.*INVALID.*")
  public void testFromStringInvalid() {
    VectorBackendType.fromString("INVALID");
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must not be null.*")
  public void testFromStringNull() {
    VectorBackendType.fromString(null);
  }

  @Test
  public void testIsValid() {
    assertTrue(VectorBackendType.isValid("HNSW"));
    assertTrue(VectorBackendType.isValid("hnsw"));
    assertTrue(VectorBackendType.isValid("IVF_FLAT"));
    assertTrue(VectorBackendType.isValid("ivf_flat"));

    assertFalse(VectorBackendType.isValid("INVALID"));
    assertFalse(VectorBackendType.isValid(""));
    assertFalse(VectorBackendType.isValid(null));
  }

  @Test
  public void testDescription() {
    assertTrue(VectorBackendType.HNSW.getDescription().contains("Hierarchical"));
    assertTrue(VectorBackendType.IVF_FLAT.getDescription().contains("flat"));
  }
}
