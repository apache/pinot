/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.indexsegment.utils;

import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.MmapMemoryManager;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MmapMemoryManagerFileCleanupTest {
  private String _tmpDir;

  @BeforeClass
  public void setUp() {
    _tmpDir = System.getProperty("java.io.tmpdir") + "/" + MmapMemoryManagerTest.class.getSimpleName();
    File dir = new File(_tmpDir);
    FileUtils.deleteQuietly(dir);
    dir.mkdir();
    dir.deleteOnExit();
  }

  @AfterClass
  public void tearDown() {
    new File(_tmpDir).delete();
  }

  // Since this file leaves MmapUtils allocation contexts in place, it cannot be included in MmapMemoryManagerTest
  @Test
  public void testFileDelete() throws Exception {
    final String segmentName = "someSegment";
    final String someColumn = "column";
    final long firstAlloc = 20;
    final long allocAfterRestart = 200;
    PinotDataBufferMemoryManager memoryManager = new MmapMemoryManager(_tmpDir, segmentName);
    memoryManager.allocate(firstAlloc, someColumn);
    // Now, if the host restarts, we will have a file left behind for the same consuming segment.
    // and we should not see any exception.
    memoryManager = new MmapMemoryManager(_tmpDir, segmentName);
    memoryManager.allocate(allocAfterRestart, someColumn);
    // We should not see the first allocation in the total.
    Assert.assertEquals(memoryManager.getTotalAllocatedBytes(), allocAfterRestart);
    memoryManager.close();
  }
}
