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
package org.apache.pinot.segment.local.io.codec;

import java.lang.reflect.Modifier;
import org.apache.pinot.segment.spi.codec.ChunkCodecHandler;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;


/// Tests for the immutable production registry and its package-scoped mutable test fixture.
public class CodecRegistryTest {

  @Test
  public void testMutableRegistryEntryPointsRemainAvailableToSamePackageTests() {
    CodecRegistry registry = new CodecRegistry();

    assertSame(registry.register(DeltaCodecDefinition.INSTANCE), registry);
    assertSame(registry.get("delta"), DeltaCodecDefinition.INSTANCE);
  }

  @Test
  public void testMutableRegistryEntryPointsAreNotPublic() throws ReflectiveOperationException {
    assertFalse(Modifier.isPublic(CodecRegistry.class.getDeclaredConstructor().getModifiers()));
    assertFalse(Modifier.isPublic(
        CodecRegistry.class.getDeclaredMethod("register", ChunkCodecHandler.class).getModifiers()));
  }

  @Test
  public void testDefaultRegistryRejectsMutation() {
    assertThrows(UnsupportedOperationException.class,
        () -> CodecRegistry.DEFAULT.register(DeltaCodecDefinition.INSTANCE));
  }
}
