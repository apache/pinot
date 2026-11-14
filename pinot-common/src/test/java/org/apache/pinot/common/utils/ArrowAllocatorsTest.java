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
package org.apache.pinot.common.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.encoder.ArrowResponseEncoder;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.testng.annotations.Test;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies allocation limits and encoder recovery in an isolated JVM.
public class ArrowAllocatorsTest {
  @Test
  public void testReservationRetriesAfterAnotherAllocatorChangesTheBudget()
      throws Exception {
    // Initialize shared Arrow/Netty dependencies before intercepting the isolated allocator's counter constructor.
    try (RootAllocator allocator = ArrowAllocators.newRootAllocator(1024); ArrowBuf buffer = allocator.buffer(64)) {
      buffer.setLong(0, 7L);
      assertEquals(buffer.getLong(0), 7L);
    }
    String allocatorClassName = ArrowAllocators.class.getName();
    ClassLoader loader = new ClassLoader(ArrowAllocators.class.getClassLoader()) {
      @Override
      protected Class<?> loadClass(String name, boolean resolve)
          throws ClassNotFoundException {
        if (!name.equals(allocatorClassName) && !name.startsWith(allocatorClassName + "$")) {
          return super.loadClass(name, resolve);
        }
        synchronized (getClassLoadingLock(name)) {
          Class<?> loaded = findLoadedClass(name);
          if (loaded == null) {
            try (InputStream stream = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
              if (stream == null) {
                throw new ClassNotFoundException(name);
              }
              byte[] bytes = stream.readAllBytes();
              loaded = defineClass(name, bytes, 0, bytes.length, ArrowAllocators.class.getProtectionDomain());
            } catch (IOException e) {
              throw new ClassNotFoundException(name, e);
            }
          }
          if (resolve) {
            resolveClass(loaded);
          }
          return loaded;
        }
      }
    };
    Class<?> isolatedAllocator;
    AtomicLong reservations;
    try (MockedConstruction<AtomicLong> counters = mockConstruction(AtomicLong.class)) {
      isolatedAllocator = Class.forName(allocatorClassName, true, loader);
      assertEquals(counters.constructed().size(), 1);
      reservations = counters.constructed().get(0);
    }
    assertEquals(isolatedAllocator.getProtectionDomain().getCodeSource().getLocation(),
        ArrowAllocators.class.getProtectionDomain().getCodeSource().getLocation());
    // A competing reservation changes the shared budget from 0 to 32 between the first read and CAS.
    when(reservations.get()).thenReturn(0L, 32L);
    when(reservations.compareAndSet(0, 64)).thenReturn(false);
    when(reservations.compareAndSet(32, 96)).thenReturn(true);
    clearInvocations(reservations);
    try (RootAllocator allocator =
        (RootAllocator) isolatedAllocator.getMethod("newRootAllocator", long.class).invoke(null, 1024L)) {
      try (ArrowBuf buffer = allocator.buffer(64)) {
        buffer.setLong(0, 42L);
        assertEquals(buffer.getLong(0), 42L);
        assertEquals(allocator.getAllocatedMemory(), 64L);
      }
      assertEquals(allocator.getAllocatedMemory(), 0L);
    }
    InOrder reservationOrder = inOrder(reservations);
    reservationOrder.verify(reservations).get();
    reservationOrder.verify(reservations).compareAndSet(0, 64);
    reservationOrder.verify(reservations).get();
    reservationOrder.verify(reservations).compareAndSet(32, 96);
    reservationOrder.verify(reservations).addAndGet(-64);
  }

  @Test
  public void testProcessMemoryLimitAndEncoderRecovery()
      throws Exception {
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Xmx64m", "-XX:MaxDirectMemorySize=1m", "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "-Dio.netty.maxDirectMemory=1048576", "-Dio.netty.tryReflectionSetAccessible=true",
        "-Dio.netty.allocator.numDirectArenas=1", "-Dio.netty.allocator.maxOrder=4", "-cp", classpath,
        MemoryLimitProbe.class.getName()).redirectErrorStream(true).start();
    try {
      assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Arrow memory limit probe timed out");
      String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertEquals(process.exitValue(), 0, output);
    } finally {
      process.destroyForcibly();
    }
  }

  /// Exercises the production allocator and encoder in a JVM with a bounded direct-memory limit.
  public static final class MemoryLimitProbe {
    private MemoryLimitProbe() {
    }

    public static void main(String[] args)
        throws Exception {
      testSharedBudgetAndChildren();
      testNettyBudgetAndRetainedReferences();
      testEncoderRecovery();
    }

    private static void testSharedBudgetAndChildren() {
      try (RootAllocator first = ArrowAllocators.newRootAllocator(Long.MAX_VALUE);
          RootAllocator second = ArrowAllocators.newRootAllocator(Long.MAX_VALUE);
          BufferAllocator child = first.newChildAllocator("query", 0, Long.MAX_VALUE)) {
        ArrowBuf firstBuffer = child.buffer(512 * 1024);
        ArrowBuf secondBuffer = second.buffer(256 * 1024);
        try {
          assertEquals(ArrowAllocators.getAllocatedMemory(), 768 * 1024L);
          expectThrows(OutOfMemoryException.class, () -> second.buffer(512 * 1024));
          assertEquals(second.getAllocatedMemory(), 256 * 1024L);
          firstBuffer.close();
          firstBuffer = null;
          try (ArrowBuf recovered = second.buffer(512 * 1024)) {
            recovered.setLong(0, 42L);
            assertEquals(recovered.getLong(0), 42L);
            assertEquals(recovered.memoryAddress() % Long.BYTES, 0L);
          }
          expectThrows(OutOfMemoryException.class, () -> child.buffer((long) Integer.MAX_VALUE + 1));
        } finally {
          if (firstBuffer != null) {
            firstBuffer.close();
          }
          secondBuffer.close();
        }
      }
      assertEquals(ArrowAllocators.getAllocatedMemory(), 0L);
    }

    private static void testNettyBudgetAndRetainedReferences() {
      try (RootAllocator allocator = ArrowAllocators.newRootAllocator(Long.MAX_VALUE)) {
        ByteBuf occupied = PooledByteBufAllocator.DEFAULT.directBuffer(1024 * 1024, 1024 * 1024);
        try {
          expectThrows(OutOfMemoryException.class, () -> allocator.buffer(4096));
          assertEquals(allocator.getAllocatedMemory(), 0L);
          assertEquals(ArrowAllocators.getAllocatedMemory(), 0L);
        } finally {
          occupied.release();
        }
        ArrowBuf buffer = allocator.buffer(512 * 1024);
        buffer.getReferenceManager().retain();
        buffer.close();
        assertEquals(ArrowAllocators.getAllocatedMemory(), 512 * 1024L);
        buffer.close();
        assertEquals(ArrowAllocators.getAllocatedMemory(), 0L);
      }
    }

    private static void testEncoderRecovery()
        throws Exception {
      ArrowResponseEncoder encoder = new ArrowResponseEncoder();
      DataSchema schema = new DataSchema(new String[]{"id", "value"},
          new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
      ResultTable oversized = new ResultTable(schema, Collections.singletonList(new Object[]{1,
          "x".repeat(2 * 1024 * 1024)}));
      expectThrows(OutOfMemoryException.class, () -> encoder.encodeResultTable(oversized, 0, 1));
      assertEquals(ArrowAllocators.getAllocatedMemory(), 0L);
      ResultTable small = new ResultTable(schema, Collections.singletonList(new Object[]{1, "ok"}));
      byte[] encoded = encoder.encodeResultTable(small, 0, 1);
      ResultTable decoded = encoder.decodeResultTable(encoded, 1, schema);
      assertEquals(decoded.getRows().size(), 1);
      assertEquals(decoded.getRows().get(0), small.getRows().get(0));
      assertEquals(ArrowAllocators.getAllocatedMemory(), 0L);
    }
  }
}
