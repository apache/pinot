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
package org.apache.pinot.spi.config.instance;

import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class InstanceDataManagerConfigTest {

  /// Simulates an [InstanceDataManagerConfig] implementation compiled before [#getMaxMmapPrefetchBytes()] was
  /// added: since it's a default method, such an implementation must fall back to it instead of throwing
  /// `AbstractMethodError`.
  @Test
  public void testGetMaxMmapPrefetchBytesDefaultsForPreexistingImplementations() {
    InstanceDataManagerConfig config = Mockito.mock(InstanceDataManagerConfig.class, Mockito.CALLS_REAL_METHODS);

    assertEquals(config.getMaxMmapPrefetchBytes(), CommonConstants.Server.DEFAULT_MMAP_PREFETCH_MAX_SIZE_BYTES);
  }
}
