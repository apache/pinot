/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import java.io.File;
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
}
