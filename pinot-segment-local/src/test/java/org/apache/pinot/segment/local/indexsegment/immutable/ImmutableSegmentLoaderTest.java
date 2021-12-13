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
package org.apache.pinot.segment.local.indexsegment.immutable;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ImmutableSegmentLoaderTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "ImmutableSegmentLoaderTest");

  @BeforeMethod
  public void setUp() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testIfNeedConvertSegmentFormat()
      throws IOException {
    IndexLoadingConfig config = new IndexLoadingConfig();
    assertFalse(ImmutableSegmentLoader.needConvertSegmentFormat(config, null));

    config.setSegmentVersion(SegmentVersion.v3);

    SegmentMetadataImpl metadata = mock(SegmentMetadataImpl.class);
    when(metadata.getIndexDir()).thenReturn(null);
    when(metadata.getVersion()).thenReturn(SegmentVersion.v3);
    assertFalse(ImmutableSegmentLoader.needConvertSegmentFormat(config, metadata));

    metadata = mock(SegmentMetadataImpl.class);
    when(metadata.getIndexDir()).thenReturn(null);
    when(metadata.getVersion()).thenReturn(SegmentVersion.v1);
    assertTrue(ImmutableSegmentLoader.needConvertSegmentFormat(config, metadata));

    File indexDir = new File(TEMP_DIR, "seg01");
    when(metadata.getIndexDir()).thenReturn(indexDir);
    when(metadata.getVersion()).thenReturn(SegmentVersion.v3);
    FileUtils.forceMkdir(new File(indexDir, "v3"));
    assertFalse(ImmutableSegmentLoader.needConvertSegmentFormat(config, metadata));
    FileUtils.deleteQuietly(new File(indexDir, "v3"));
    when(metadata.getVersion()).thenReturn(SegmentVersion.v1);
    assertTrue(ImmutableSegmentLoader.needConvertSegmentFormat(config, metadata));
  }
}
