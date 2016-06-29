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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestStarTreeIndexBackwardCompatibilityTest extends BaseStarTreeIndexTest {
  private static final String _tmpDirName = "/tmp/starTreeIndexDir";
  private File _tmpDir;
  private IndexSegment _segment;

  @BeforeTest
  void setup()
      throws Exception {
    String compressedIndex = TestUtils.getFileFromResourceUrl(
        TestStarTreeIndexBackwardCompatibilityTest.class.getClassLoader().getResource("data/starTreeSegment.tar.gz"));

    _tmpDir = new File(_tmpDirName);
    if (_tmpDir.exists()) {
      FileUtils.deleteQuietly(_tmpDir);
    }

    TarGzCompressionUtils.unTar(new File(compressedIndex), _tmpDir);
    File segmentFile = _tmpDir.listFiles()[0];
    _segment = Loaders.IndexSegment.load(segmentFile, ReadMode.heap);
  }

  @Test
  public void test() {
    testHardCodedQueries(_segment, _segment.getSegmentMetadata().getSchema());
  }

  @AfterTest
  void tearDown() {
    FileUtils.deleteQuietly(_tmpDir);
  }
}
