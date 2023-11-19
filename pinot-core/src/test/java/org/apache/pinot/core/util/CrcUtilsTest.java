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
package org.apache.pinot.core.util;

import java.io.File;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.utils.CrcUtils;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class CrcUtilsTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "CrcUtilsTest");
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final long EXPECTED_V1_CRC = 3146202196L;
  private static final long EXPECTED_V3_CRC = 2128762224L;

  @Test
  public void testCrc()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String filePath = TestUtils.getFileFromResourceUrl(resource);
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            TimeUnit.DAYS, "testTable");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    File indexDir = driver.getOutputDirectory();
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), EXPECTED_V1_CRC);

    new SegmentV1V2ToV3FormatConverter().convert(indexDir);
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), EXPECTED_V3_CRC);

    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
