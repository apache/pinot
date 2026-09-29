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
package org.apache.pinot.segment.local.startree.v2.builder;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION;
import static org.apache.pinot.segment.spi.V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
import static org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants.STAR_TREE_INDEX_FILE_NAME;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class StarTreeIndexSeparatorTest {
  private static final String SEGMENT_PATH = "data/startree/segment";
  private static final StarTreeV2BuilderConfig BUILDER_CONFIG = StarTreeV2BuilderConfig.fromIndexConfig(
      new StarTreeIndexConfig(List.of("AirlineID", "Origin", "Dest"), List.of(), List.of("count__*", "max__ArrDelay"),
          null, 10));
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "StarTreeIndexSeparatorTest");

  @BeforeClass
  public void setUp() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testSeparate()
      throws Exception {
    URL segmentUrl = getClass().getClassLoader().getResource(SEGMENT_PATH);
    assertNotNull(segmentUrl);
    File segmentDir = new File(segmentUrl.getFile());
    try (StarTreeIndexSeparator separator = new StarTreeIndexSeparator(
        new File(segmentDir, StarTreeV2Constants.INDEX_MAP_FILE_NAME),
        new File(segmentDir, StarTreeV2Constants.INDEX_FILE_NAME),
        new SegmentMetadataImpl(segmentDir).getStarTreeV2MetadataList())) {
      separator.separate(TEMP_DIR, BUILDER_CONFIG);
    }
    String[] fileNames = TEMP_DIR.list();
    assertNotNull(fileNames);
    Set<String> fileNameSet = new HashSet<>(Arrays.asList(fileNames));
    assertTrue(fileNameSet.contains(STAR_TREE_INDEX_FILE_NAME));
    BUILDER_CONFIG.getDimensionsSplitOrder()
        .forEach(dimension -> assertTrue(fileNameSet.contains(dimension + UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION)));
    BUILDER_CONFIG.getFunctionColumnPairs()
        .forEach(dimension -> assertTrue(fileNameSet.contains(dimension + RAW_SV_FORWARD_INDEX_FILE_EXTENSION)));
  }
}
