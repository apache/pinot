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
package org.apache.pinot.segment.local.segment.store;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentDirectoryPathsTest {

  @Test
  public void testSegmentDirectoryFor() {
    File f = new File("f");
    File v1Dir = SegmentDirectoryPaths.segmentDirectoryFor(f, SegmentVersion.v1);
    File v2Dir = SegmentDirectoryPaths.segmentDirectoryFor(f, SegmentVersion.v2);
    File v3Dir = SegmentDirectoryPaths.segmentDirectoryFor(f, SegmentVersion.v3);

    Assert.assertEquals(v1Dir, f);
    Assert.assertEquals(v2Dir, f);
    Assert.assertEquals(v3Dir, new File("f", SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME));
  }

  @Test
  public void testFindMetadataFile()
      throws Exception {
    File indexDir = new File(SegmentDirectoryPaths.class.toString());
    FileUtils.deleteQuietly(indexDir);
    try {
      Assert.assertTrue(indexDir.mkdirs());
      File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
      Assert.assertTrue(v3Dir.mkdir());
      String fileName = V1Constants.MetadataKeys.METADATA_FILE_NAME;
      File v1File = new File(indexDir, fileName);
      FileUtils.touch(v1File);
      File v3File = new File(v3Dir, fileName);
      FileUtils.touch(v3File);

      Assert.assertEquals(SegmentDirectoryPaths.findMetadataFile(indexDir), v3File);

      FileUtils.forceDelete(v3File);
      Assert.assertEquals(SegmentDirectoryPaths.findMetadataFile(indexDir), v1File);

      FileUtils.forceDelete(v1File);
      Assert.assertNull(SegmentDirectoryPaths.findMetadataFile(indexDir));
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }
}
