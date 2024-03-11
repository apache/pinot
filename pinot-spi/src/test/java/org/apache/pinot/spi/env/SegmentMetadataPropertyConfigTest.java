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
package org.apache.pinot.spi.env;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class SegmentMetadataPropertyConfigTest {
  private static final String SEGMENT_VERSION_IDENTIFIER = "segment.metadata.version";
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SegmentMetadataPropertyConfigTest");
  private static final File CONFIG_FILE = new File(TEMP_DIR, "config");
  private static final String[] TEST_PROPERTY_KEY = { "test1", "test2_key", "test3_key_", "test3_key_1234" };
  private static final String[] TEST_PROPERTY_KEY_WITH_SPECIAL_CHAR = { "test:1", "test2=key",
      "test3,key_", "test3:=._key_1234" };

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @AfterClass
  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testSegmentMetadataPropertyConfiguration()
      throws ConfigurationException {
    PropertiesConfiguration configuration = CommonsConfigurationUtils.
        segmentMetadataFromFile(CONFIG_FILE, true, true,
        PropertyIOFactoryKind.SegmentMetadataIOFactory, "");

    // setting the random value of the test keys
    for (String key: TEST_PROPERTY_KEY) {
      configuration.setProperty(key, RandomStringUtils.randomAscii(5));
    }
    // recovered keys from the configuration.
    List<String> recoveredKeys = CommonsConfigurationUtils.getKeys(configuration);
    testPropertyKeys(recoveredKeys, TEST_PROPERTY_KEY);

    CommonsConfigurationUtils.saveToFile(configuration, CONFIG_FILE); // save the configuration.

    // reading the configuration from saved file.
    configuration = CommonsConfigurationUtils.segmentMetadataFromFile(CONFIG_FILE, true, true,
        PropertyIOFactoryKind.SegmentMetadataIOFactory, "");
    recoveredKeys = CommonsConfigurationUtils.getKeys(configuration);
    testPropertyKeys(recoveredKeys, TEST_PROPERTY_KEY);
  }

  @Test
  public void testSegmentMetadataPropertyConfigurationWithHeader()
      throws ConfigurationException {
    PropertiesConfiguration configuration = CommonsConfigurationUtils.
        segmentMetadataFromFile(CONFIG_FILE, true, true,
        PropertyIOFactoryKind.SegmentMetadataIOFactory, SEGMENT_VERSION_IDENTIFIER);
    configuration.setHeader("segment.metadata.version=version1");

    // setting the random value of the test keys
    for (String key: TEST_PROPERTY_KEY) {
      configuration.setProperty(key, RandomStringUtils.randomAscii(5));
    }
    // recovered keys from the configuration.
    List<String> recoveredKeys = CommonsConfigurationUtils.getKeys(configuration);
    testPropertyKeys(recoveredKeys, TEST_PROPERTY_KEY);

    CommonsConfigurationUtils.saveToFile(configuration, CONFIG_FILE); // save the configuration.

    // reading the configuration from saved file.
    configuration = CommonsConfigurationUtils.segmentMetadataFromFile(CONFIG_FILE, true, true,
        PropertyIOFactoryKind.SegmentMetadataIOFactory, SEGMENT_VERSION_IDENTIFIER);
    recoveredKeys = CommonsConfigurationUtils.getKeys(configuration);
    testPropertyKeys(recoveredKeys, TEST_PROPERTY_KEY);
  }

  @Test
  public void testSegmentMetadataReaderWithSpecialChars()
      throws ConfigurationException {
    PropertiesConfiguration configuration = CommonsConfigurationUtils.segmentMetadataFromFile(CONFIG_FILE, true, true,
        PropertyIOFactoryKind.SegmentMetadataIOFactory, SEGMENT_VERSION_IDENTIFIER);

    // setting the random value of the test keys
    for (String key: TEST_PROPERTY_KEY_WITH_SPECIAL_CHAR) {
      configuration.setProperty(key, RandomStringUtils.randomAscii(5));
    }
    // recovered keys from the configuration.
    List<String> recoveredKeys = CommonsConfigurationUtils.getKeys(configuration);
    testPropertyKeys(recoveredKeys, TEST_PROPERTY_KEY_WITH_SPECIAL_CHAR);

    CommonsConfigurationUtils.saveToFile(configuration, CONFIG_FILE); // save the configuration.

    // reading the configuration from saved file.
    configuration = CommonsConfigurationUtils.segmentMetadataFromFile(CONFIG_FILE, true, true,
        PropertyIOFactoryKind.SegmentMetadataIOFactory, SEGMENT_VERSION_IDENTIFIER);
    recoveredKeys = CommonsConfigurationUtils.getKeys(configuration);
    testPropertyKeys(recoveredKeys, TEST_PROPERTY_KEY_WITH_SPECIAL_CHAR);
  }

  @Test
  public void testOldSegmentMetadataBackwardCompatability()
      throws ConfigurationException {
    File oldSegmentProperties = new File(
        Objects.requireNonNull(
            PropertiesConfiguration.class.getClassLoader()
                .getResource("segment-metadata-without-version-header.properties")).getFile());
    PropertiesConfiguration configuration = CommonsConfigurationUtils.
        segmentMetadataFromFile(oldSegmentProperties, true, true,
        PropertyIOFactoryKind.SegmentMetadataIOFactory, SEGMENT_VERSION_IDENTIFIER);

    testSegmentMetadataContent(configuration);
  }

  private static void testSegmentMetadataContent(PropertiesConfiguration configuration) {
    // getting all the keys, length of the list should be equal to the number of lines in the segment metadata
    List<String> keys = CommonsConfigurationUtils.getKeys(configuration);
    assertEquals(keys.size(), 123);

    // asserting table name property from segment metadata
    String tableName = configuration.getString("segment.table.name");
    assertNotNull(tableName);
    assertEquals(tableName, "fineFoodReviews");

    // asserting table name property from segment metadata
    String segmentName = configuration.getString("segment.name");
    assertNotNull(tableName);
    assertEquals(segmentName, "fineFoodReviews_OFFLINE_0");

    // asserting segment dimension column names from segment metadata
    String[] segmentDimensionColumnNames = configuration.getStringArray("segment.dimension.column.names");
    assertNotNull(segmentDimensionColumnNames);
    assertEquals(segmentDimensionColumnNames.length, 8);
    assertEquals(String.join(",", segmentDimensionColumnNames),
        "ProductId,Score,Summary,Text,UserId,combined,embedding,n_tokens");
  }

  private static void testPropertyKeys(List<String> recoveredKeys, String[] actualKeys) {
    assertEquals(recoveredKeys.size(), actualKeys.length);
    for (int i = 1; i < recoveredKeys.size(); i++) {
      String recoveredValue = recoveredKeys.get(i);
      assertEquals(recoveredValue, actualKeys[i]);
    }
  }
}
