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
package com.linkedin.pinot.core.segment.store;

import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentDirectoryPathsTest {

  @Test
  public void testSegmentPathFor() {
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
      throws IOException {
    File tempDirectory = null;
    try {
      // setup temp dir, v3 subdir and create metadata.properties in both
      tempDirectory = new File(SegmentDirectoryPaths.class.toString());
      tempDirectory.deleteOnExit();
      FileUtils.forceMkdir(tempDirectory);
      File v3Dir = new File(tempDirectory, "v3");
      FileUtils.forceMkdir(v3Dir);
      File metaFile = new File(tempDirectory, V1Constants.MetadataKeys.METADATA_FILE_NAME);
      try (FileOutputStream outputStream = new FileOutputStream(metaFile)) {
        outputStream.write(10);
      }
      File v3MetaFile = new File(v3Dir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
      FileUtils.copyFile(metaFile, v3MetaFile);

      {
        File testMetaFile = SegmentDirectoryPaths.findMetadataFile(tempDirectory);
        Assert.assertNotNull(testMetaFile);
        Assert.assertEquals(testMetaFile.toString(), metaFile.toString());

        testMetaFile = SegmentDirectoryPaths.findMetadataFile(tempDirectory, SegmentVersion.v1);
        Assert.assertNotNull(testMetaFile);
        Assert.assertEquals(testMetaFile.toString(), metaFile.toString());

        testMetaFile = SegmentDirectoryPaths.findMetadataFile(tempDirectory, SegmentVersion.v3);
        Assert.assertNotNull(testMetaFile);
        Assert.assertEquals(testMetaFile.toString(), v3MetaFile.toString());
      }
      {
        // drop v1 metadata file
        FileUtils.forceDelete(metaFile);
        File testMetaFile = SegmentDirectoryPaths.findMetadataFile(tempDirectory);
        Assert.assertNotNull(testMetaFile);
        Assert.assertEquals(testMetaFile.toString(), v3MetaFile.toString());

        testMetaFile = SegmentDirectoryPaths.findMetadataFile(tempDirectory, SegmentVersion.v1);
        Assert.assertNull(testMetaFile);

        testMetaFile = SegmentDirectoryPaths.findMetadataFile(tempDirectory, SegmentVersion.v3);
        Assert.assertNotNull(testMetaFile);
        Assert.assertEquals(testMetaFile.toString(), v3MetaFile.toString());
      }
      {
        // drop v3 metadata file
        FileUtils.forceDelete(v3MetaFile);
        File testMetaFile = SegmentDirectoryPaths.findMetadataFile(tempDirectory);
        Assert.assertNull(testMetaFile);

        testMetaFile = SegmentDirectoryPaths.findMetadataFile(tempDirectory, SegmentVersion.v1);
        Assert.assertNull(testMetaFile);

        testMetaFile = SegmentDirectoryPaths.findMetadataFile(tempDirectory, SegmentVersion.v3);
        Assert.assertNull(testMetaFile);
      }
    } finally {
      if (tempDirectory != null) {
        FileUtils.deleteQuietly(tempDirectory);
      }
    }
  }
}
