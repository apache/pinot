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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION;
import static org.apache.pinot.segment.spi.V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
import static org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants.STAR_TREE_INDEX_FILE_NAME;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;


public class StarTreeIndexSeparatorTest {

  private static final String SEGMENT_PATH = "data/startree/segment";
  private static final String TOTAL_DOCS_KEY = "startree.v2.0.total.docs";

  private StarTreeIndexSeparator _separator;
  private PropertiesConfiguration _metadataProperties;
  private final StarTreeV2BuilderConfig _builderConfig = StarTreeV2BuilderConfig.fromIndexConfig(
      new StarTreeIndexConfig(
          Lists.newArrayList("AirlineID", "Origin", "Dest"),
          Lists.newArrayList(),
          Lists.newArrayList("count__*", "max__ArrDelay"),
          10));

  @BeforeClass
  public void setup()
      throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    URL segmentUrl = classLoader.getResource(SEGMENT_PATH);
    File segmentDir = new File(segmentUrl.getFile());
    _metadataProperties = CommonsConfigurationUtils.fromFile(
        new File(segmentDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    _separator = new StarTreeIndexSeparator(
        new File(segmentDir, StarTreeV2Constants.INDEX_MAP_FILE_NAME),
        new File(segmentDir, StarTreeV2Constants.INDEX_FILE_NAME),
        _metadataProperties);
  }

  @Test
  public void extractTotalDocsListTest() {
    assertNotNull(_separator);
    List<Integer> docsList = _separator.extractTotalDocsList(_metadataProperties);
    assertNotNull(docsList);
    assertEquals(docsList, Lists.newArrayList(_metadataProperties.getInt(TOTAL_DOCS_KEY)));
  }

  @Test
  public void extractBuilderConfigsTest() {
    List<StarTreeV2BuilderConfig> builderConfigList = _separator.extractBuilderConfigs(_metadataProperties);
    assertEquals(builderConfigList, Lists.newArrayList(_builderConfig));
  }

  @Test
  public void separateTest()
      throws IOException {
    File tempDir = new File(FileUtils.getTempDirectory(), "separateTest");
    _separator.separate(tempDir, _builderConfig);
    List<String> files = Arrays.asList(Objects.requireNonNull(tempDir.list()));
    assertTrue(files.contains(STAR_TREE_INDEX_FILE_NAME));
    _builderConfig.getDimensionsSplitOrder()
        .forEach(dimension -> assertTrue(files.contains(dimension + UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION)));
    _builderConfig.getFunctionColumnPairs()
        .forEach(dimension -> assertTrue(files.contains(dimension + RAW_SV_FORWARD_INDEX_FILE_EXTENSION)));
    FileUtils.forceDelete(tempDir);
  }
}
