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
package org.apache.pinot.segment.spi.memory.unsafe;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests finalizer cleanup synchronously, without depending on garbage collection. Each invocation owns its resources
/// and does not share mutable state with other tests.
public class MemoryFinalizationTest {
  @DataProvider
  public Object[][] closedBeforeFinalization() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "closedBeforeFinalization")
  public void testDirectMemoryFinalization(boolean closeBeforeFinalization)
      throws Throwable {
    try (TrackingDirectMemory memory = new TrackingDirectMemory()) {
      if (closeBeforeFinalization) {
        memory.close();
      }
      memory.finalize();
      assertEquals(memory._closeCalls, 1, "Finalization must close an open allocation and leave a closed one alone");
      memory.finalize();
      assertEquals(memory._closeCalls, 1, "Finalization must not close the same allocation twice");
    }
  }

  @Test(dataProvider = "closedBeforeFinalization")
  public void testMmapMemoryFinalization(boolean closeBeforeFinalization)
      throws Throwable {
    Path file = Files.createTempFile("memory-finalization", ".bin");
    try {
      try (TrackingMmapMemory memory = new TrackingMmapMemory(file.toFile())) {
        if (closeBeforeFinalization) {
          memory.close();
        }
        memory.finalize();
        assertEquals(memory._closeCalls, 1, "Finalization must close an open mapping and leave a closed one alone");
        memory.finalize();
        assertEquals(memory._closeCalls, 1, "Finalization must not close the same mapping twice");
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  /// Tracks cleanup calls while retaining the real allocation and release behavior; used by one test at a time.
  private static class TrackingDirectMemory extends DirectMemory {
    private int _closeCalls;

    TrackingDirectMemory() {
      super(64);
    }

    @Override
    public synchronized void close() {
      super.close();
      _closeCalls++;
    }
  }

  /// Tracks cleanup calls while retaining the real mapping and unmap behavior; used by one test at a time.
  private static class TrackingMmapMemory extends MmapMemory {
    private int _closeCalls;

    TrackingMmapMemory(File file) {
      super(file, false, 0, 64);
    }

    @Override
    public synchronized void close() {
      super.close();
      _closeCalls++;
    }
  }
}
