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

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests for the immutable production registry and its package-scoped mutable test fixture.
public class CodecRegistryTest {

  @Test
  public void testDefaultRegistryContainsBuiltInHandlersAndAlias() {
    assertSame(CodecRegistry.DEFAULT.get("lz4"), Lz4CodecDefinition.INSTANCE);
    assertSame(CodecRegistry.DEFAULT.get("SNAPPY"), SnappyCodecDefinition.INSTANCE);
    assertSame(CodecRegistry.DEFAULT.get("gzip"), GzipCodecDefinition.INSTANCE);
    assertSame(CodecRegistry.DEFAULT.get("zstd"), ZstdCodecDefinition.INSTANCE);
    assertSame(CodecRegistry.DEFAULT.get("zstandard"), ZstdCodecDefinition.INSTANCE);
    assertSame(CodecRegistry.DEFAULT.get("delta"), DeltaCodecDefinition.INSTANCE);
    assertSame(CodecRegistry.DEFAULT.get("DELTADELTA"), DeltaDeltaCodecDefinition.INSTANCE);
    assertSame(CodecRegistry.DEFAULT.get("t64"), T64CodecDefinition.INSTANCE);
    assertSame(CodecRegistry.DEFAULT.get("GORILLA"), GorillaCodecDefinition.INSTANCE);
    assertNull(CodecRegistry.DEFAULT.get("NOSUCHCODEC"));
  }

  @Test
  public void testMutableRegistryEntryPointsRemainAvailableToSamePackageTests() {
    CodecRegistry registry = new CodecRegistry();

    assertSame(registry.register(Lz4CodecDefinition.INSTANCE), registry);
    assertSame(registry.get("lz4"), Lz4CodecDefinition.INSTANCE);
  }

  @Test
  public void testMutableRegistryEntryPointsAreNotPublic() throws ReflectiveOperationException {
    assertFalse(Modifier.isPublic(CodecRegistry.class.getModifiers()));
    assertFalse(Modifier.isPublic(CodecRegistry.class.getDeclaredConstructor().getModifiers()));
    assertFalse(Modifier.isPublic(
        CodecRegistry.class.getDeclaredMethod("register", ChunkCodecHandler.class).getModifiers()));
    assertFalse(Modifier.isPublic(
        CodecRegistry.class.getDeclaredMethod("get", String.class).getModifiers()));
  }

  @Test
  public void testDefaultRegistryRejectsMutation() {
    assertThrows(UnsupportedOperationException.class,
        () -> CodecRegistry.DEFAULT.register(Lz4CodecDefinition.INSTANCE));
  }

  @Test
  public void testMutableRegistryRejectsDuplicateNameIgnoringCase() {
    CodecRegistry registry = new CodecRegistry().register(Lz4CodecDefinition.INSTANCE);
    assertThrows(IllegalArgumentException.class, () -> registry.register(Lz4CodecDefinition.INSTANCE));
  }

  @Test
  public void testMutableRegistryRejectsReservedWrapperName() {
    CodecRegistry registry = new CodecRegistry();
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> registry.register(new ReservedNameStub()));
    assertTrue(exception.getMessage().contains("reserved"), exception.getMessage());
  }

  /// Stub whose name collides with the reserved wrapper keyword; every behavior method is unreachable.
  private static final class ReservedNameStub implements ChunkCodecHandler<CodecOptions> {
    @Override
    public String name() {
      return "codec";
    }

    @Override
    public CodecKind kind() {
      return CodecKind.COMPRESSION;
    }

    @Override
    public CodecOptions parseOptions(List<String> args) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void validateContext(CodecOptions options, CodecContext ctx) {
    }

    @Override
    public String canonicalize(CodecOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer encode(CodecOptions options, CodecContext ctx, ByteBuffer src)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer decode(CodecOptions options, CodecContext ctx, ByteBuffer src)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void decodeInto(CodecOptions options, CodecContext ctx, ByteBuffer src, ByteBuffer dst)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int maxEncodedSize(CodecOptions options, int inputSize) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean requiresDirectDstBuffer() {
      return false;
    }
  }
}
