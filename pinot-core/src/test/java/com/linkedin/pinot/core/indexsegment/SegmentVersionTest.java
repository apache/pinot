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
package com.linkedin.pinot.core.indexsegment;

import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentVersionTest {
  @Test
  public void test1() {
    SegmentVersion version;
    version = SegmentVersion.fromString("v1", SegmentVersion.v3);
    Assert.assertEquals(version, SegmentVersion.v1);
    version = SegmentVersion.fromString("v2", SegmentVersion.v1);
    Assert.assertEquals(version, SegmentVersion.v2);
    version = SegmentVersion.fromString("v3", SegmentVersion.v1);
    Assert.assertEquals(version, SegmentVersion.v3);
    version = SegmentVersion.fromString("badString", SegmentVersion.v1);
    Assert.assertEquals(version, SegmentVersion.v1);
    version = SegmentVersion.fromString(null, SegmentVersion.v3);
    Assert.assertEquals(version, SegmentVersion.v3);
    version = SegmentVersion.fromString("", SegmentVersion.DEFAULT_VERSION);
    Assert.assertEquals(version, SegmentVersion.DEFAULT_VERSION);

    Assert.assertTrue(SegmentVersion.v1.compareTo(SegmentVersion.v2) < 0);
    Assert.assertTrue(SegmentVersion.v2.compareTo(SegmentVersion.v2) == 0);
    Assert.assertTrue(SegmentVersion.v3.compareTo(SegmentVersion.v2) > 0);
  }
}
