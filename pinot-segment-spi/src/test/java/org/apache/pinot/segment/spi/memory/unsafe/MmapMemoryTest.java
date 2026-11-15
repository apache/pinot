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
import net.openhft.posix.MSyncFlag;
import net.openhft.posix.PosixAPI;
import org.apache.commons.io.FileUtils;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Tests [MmapMemory#flush()]: read-write mappings are msynced on a page-aligned range and fail loudly, while
/// read-only, empty and closed mappings are skipped.
public class MmapMemoryTest {
  private static final File TEMP_FILE = new File(FileUtils.getTempDirectory(), "MmapMemoryTest_" + System.nanoTime());

  @BeforeMethod
  public void setUp() {
    FileUtils.deleteQuietly(TEMP_FILE);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_FILE);
  }

  @Test
  public void testFlushThrowsOnMsyncFailure()
      throws Exception {
    MmapMemory memory = new MmapMemory(TEMP_FILE, false, 1, 1024);
    try (MockedStatic<PosixAPI> mockedPosixApi = Mockito.mockStatic(PosixAPI.class)) {
      PosixAPI posix = Mockito.mock(PosixAPI.class);
      mockedPosixApi.when(PosixAPI::posix).thenReturn(posix);
      Mockito.when(posix.msync(Mockito.anyLong(), Mockito.anyLong(), Mockito.any(MSyncFlag.class))).thenReturn(-1);
      Mockito.when(posix.lastErrorStr()).thenReturn("Input/output error");

      long pageSize = Unsafer.UNSAFE.pageSize();
      long pageOffset = memory.getAddress() % pageSize;

      try {
        memory.flush();
        fail("Expected RuntimeException");
      } catch (RuntimeException e) {
        assertTrue(e.getMessage().contains("msync failed"));
        assertTrue(e.getMessage().contains("Input/output error"));
      }

      Mockito.verify(posix).msync(eq(memory.getAddress() - pageOffset), eq(1024L + pageOffset),
          eq(MSyncFlag.MS_SYNC));
    } finally {
      memory.close();
    }
  }

  @Test
  public void testFlushSkipsReadOnlyMapping()
      throws Exception {
    FileUtils.writeByteArrayToFile(TEMP_FILE, new byte[4096]);
    MmapMemory memory = new MmapMemory(TEMP_FILE, true, 1, 1024);
    try {
      assertFlushSkipsMsync(memory);
    } finally {
      memory.close();
    }
  }

  @Test
  public void testFlushSkipsEmptyMapping() {
    MmapMemory memory = new MmapMemory(TEMP_FILE, false, 0, 0);
    try {
      assertFlushSkipsMsync(memory);
    } finally {
      memory.close();
    }
  }

  @Test
  public void testFlushSkipsClosedMapping() {
    MmapMemory memory = new MmapMemory(TEMP_FILE, false, 0, 1024);
    memory.close();
    assertFlushSkipsMsync(memory);
  }

  @Test
  public void testFlushReadWriteMappingAtUnalignedOffset() {
    // A non page-aligned offset used to make msync fail with EINVAL; the aligned range must now succeed.
    MmapMemory memory = new MmapMemory(TEMP_FILE, false, 1, 1024);
    try {
      Unsafer.UNSAFE.putByte(memory.getAddress(), (byte) 7);
      memory.flush();
    } finally {
      memory.close();
    }
  }

  private static void assertFlushSkipsMsync(MmapMemory memory) {
    try (MockedStatic<PosixAPI> mockedPosixApi = Mockito.mockStatic(PosixAPI.class)) {
      PosixAPI posix = Mockito.mock(PosixAPI.class);
      mockedPosixApi.when(PosixAPI::posix).thenReturn(posix);

      memory.flush();

      Mockito.verify(posix, Mockito.never()).msync(Mockito.anyLong(), Mockito.anyLong(), Mockito.any(MSyncFlag.class));
    }
  }
}
