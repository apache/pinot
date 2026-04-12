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
package org.apache.pinot.segment.local.segment.creator.impl;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.spi.config.table.CompressionCodecSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


/**
 * Unit tests for forward-index compression metadata helpers in {@link BaseSegmentCreator}.
 */
public class BaseSegmentCreatorTest {
  private static final String COLUMN = "rawCol";
  private static final String METADATA_KEY = V1Constants.MetadataKeys.Column.getKeyFor(COLUMN,
      V1Constants.MetadataKeys.Column.FORWARD_INDEX_COMPRESSION_CODEC);

  @Test
  public void testAddsExplicitLeveledCompressionCodecMetadata() {
    ForwardIndexConfig forwardIndexConfig = new ForwardIndexConfig.Builder()
        .withCompressionCodecSpec(CompressionCodecSpec.fromString("zstd(3)"))
        .build();

    PropertiesConfiguration properties = new PropertiesConfiguration();
    BaseSegmentCreator.addForwardIndexCompressionCodecInfo(properties, COLUMN, forwardIndexConfig, false);
    assertEquals(properties.getString(METADATA_KEY), "ZSTANDARD(3)");

    Map<String, String> metadataProperties = new HashMap<>();
    BaseSegmentCreator.addForwardIndexCompressionCodecInfo(metadataProperties, COLUMN, forwardIndexConfig, false);
    assertEquals(metadataProperties.get(METADATA_KEY), "ZSTANDARD(3)");
  }

  @Test
  public void testSkipsImplicitOrDictionaryCompressionCodecMetadata() {
    ForwardIndexConfig plainCodecConfig = new ForwardIndexConfig.Builder()
        .withCompressionCodecSpec(CompressionCodecSpec.fromString("LZ4"))
        .build();
    PropertiesConfiguration properties = new PropertiesConfiguration();
    BaseSegmentCreator.addForwardIndexCompressionCodecInfo(properties, COLUMN, plainCodecConfig, false);
    assertFalse(properties.containsKey(METADATA_KEY));

    Map<String, String> metadataProperties = new HashMap<>();
    BaseSegmentCreator.addForwardIndexCompressionCodecInfo(metadataProperties, COLUMN, plainCodecConfig, false);
    assertFalse(metadataProperties.containsKey(METADATA_KEY));

    ForwardIndexConfig leveledCodecConfig = new ForwardIndexConfig.Builder()
        .withCompressionCodecSpec(CompressionCodecSpec.fromString("GZIP(6)"))
        .build();
    PropertiesConfiguration dictionaryProperties = new PropertiesConfiguration();
    BaseSegmentCreator.addForwardIndexCompressionCodecInfo(dictionaryProperties, COLUMN, leveledCodecConfig, true);
    assertFalse(dictionaryProperties.containsKey(METADATA_KEY));

    Map<String, String> nullConfigProperties = new HashMap<>();
    BaseSegmentCreator.addForwardIndexCompressionCodecInfo(nullConfigProperties, COLUMN, null, false);
    assertFalse(nullConfigProperties.containsKey(METADATA_KEY));
  }
}
