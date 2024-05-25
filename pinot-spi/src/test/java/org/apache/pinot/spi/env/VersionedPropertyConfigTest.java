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
import static org.testng.Assert.assertNull;


public class VersionedPropertyConfigTest {

  private static final String COLON_SEPARATOR = ":";
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "VersionedPropertyConfigTest");
  private static final File CONFIG_FILE = new File(TEMP_DIR, "config");
  private static final String[] TEST_PROPERTY_KEY = { "test1", "test2_key", "test3_key_",
      "test4_key_1234", "test-1", "test.1" };
  private static final String[] TEST_PROPERTY_KEY_WITH_SPECIAL_CHAR = { "test:1", "test2=key",
      "test3,key_", "test4:=._key_1234", "test5-1=" };

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
  public void testVersionedPropertyConfiguration()
      throws ConfigurationException {
    testVersionedPropertiesConfiguration(null, TEST_PROPERTY_KEY, false);
  }

  @Test
  public void testVersionedPropertyConfigurationWithDefaultHeaderVersion()
      throws ConfigurationException {
    testVersionedPropertiesConfiguration(
        CommonsConfigurationUtils.DEFAULT_PROPERTIES_CONFIGURATION_HEADER_VERSION, TEST_PROPERTY_KEY, false);
  }

  @Test
  public void testVersionedPropertyConfigurationWithHeaderVersion2()
      throws ConfigurationException {
    testVersionedPropertiesConfiguration(CommonsConfigurationUtils.PROPERTIES_CONFIGURATION_HEADER_VERSION_2,
        TEST_PROPERTY_KEY, false);
  }

  @Test
  public void testVersionedReaderWithSpecialCharsPropertyKeys()
      throws ConfigurationException {
    testVersionedPropertiesConfiguration(null, TEST_PROPERTY_KEY_WITH_SPECIAL_CHAR, false);
  }

  @Test
  public void testVersionedReaderWithSpecialCharsPropertyKeysWithDefaultHeader()
      throws ConfigurationException {
    testVersionedPropertiesConfiguration(CommonsConfigurationUtils.DEFAULT_PROPERTIES_CONFIGURATION_HEADER_VERSION,
        TEST_PROPERTY_KEY_WITH_SPECIAL_CHAR, false);
  }

  @Test
  public void testVersionedReaderWithSpecialCharsPropertyKeysWithHeaderVersion2()
      throws ConfigurationException {
    testVersionedPropertiesConfiguration(CommonsConfigurationUtils.PROPERTIES_CONFIGURATION_HEADER_VERSION_2,
        TEST_PROPERTY_KEY_WITH_SPECIAL_CHAR, false);
  }

  @Test
  public void testVersionedReaderWithDifferentSeparator() throws ConfigurationException {
    testVersionedPropertiesConfiguration(CommonsConfigurationUtils.PROPERTIES_CONFIGURATION_HEADER_VERSION_2,
        TEST_PROPERTY_KEY, true);
  }

  @Test
  //Test requires 'segment-metadata-without-version-header.properties' sample segment metadata file in resources folder
  public void testOldSegmentMetadataBackwardCompatability()
      throws ConfigurationException {
    File oldSegmentProperties = new File(
        Objects.requireNonNull(
            PropertiesConfiguration.class.getClassLoader()
                .getResource("segment-metadata-without-version-header.properties")).getFile());
    PropertiesConfiguration configuration =
        CommonsConfigurationUtils.getSegmentMetadataFromFile(oldSegmentProperties, true);

    // assert that Header is null for the config.
    assertNull(configuration.getHeader());

    // assert that configuration has DefaultIOFactory
    assertEquals(configuration.getIOFactory().getClass(), PropertiesConfiguration.DefaultIOFactory.class);

    testSegmentMetadataContent(configuration);

    // asserting escaped value ('column-ProductId-maxValue')
    String productIDMaxValue = configuration.getString("column-ProductId-maxValue");
    assertNotNull(productIDMaxValue);
    assertEquals(productIDMaxValue, "B009,WVB40S");
  }

  @Test
  //Test requires 'segment-metadata-with-version-header.properties' sample segment metadata file in resources folder
  public void testSegmentMetadataWithVersionHeader()
      throws ConfigurationException {
    File oldSegmentProperties = new File(
        Objects.requireNonNull(
            PropertiesConfiguration.class.getClassLoader()
                .getResource("segment-metadata-with-version-header.properties")).getFile());
    PropertiesConfiguration configuration =
        CommonsConfigurationUtils.getSegmentMetadataFromFile(oldSegmentProperties, true);

    // assert that Header is equals to '# version = 2'
    assertEquals(configuration.getHeader(), "# version = 2");

    // assert that configuration has SegmentMetadataPropertyIOFactory
    assertEquals(configuration.getIOFactory().getClass(), VersionedIOFactory.class);

    testSegmentMetadataContent(configuration);
  }

  private static void testSegmentMetadataContent(PropertiesConfiguration configuration) {
    // getting all the keys, length of the list should be equal to the number of lines in the segment metadata
    List<String> keys = CommonsConfigurationUtils.getKeys(configuration);
    assertEquals(keys.size(), 123);

    // asserting table name property from segment metadata
    String tableName = configuration.getString("segment.table.name");
    assertEquals(tableName, "fineFoodReviews");

    // asserting table name property from segment metadata
    String segmentName = configuration.getString("segment.name");
    assertEquals(segmentName, "fineFoodReviews_OFFLINE_0");

    // asserting segment dimension column names from segment metadata
    String[] segmentDimensionColumnNames = configuration.getStringArray("segment.dimension.column.names");
    assertEquals(segmentDimensionColumnNames.length, 8);
    assertEquals(String.join(",", segmentDimensionColumnNames),
        "ProductId,Score,Summary,Text,UserId,combined,embedding,n_tokens");

    // asserting segment.index.version
    String segmentIndexVersion = configuration.getString("segment.index.version");
    assertEquals(segmentIndexVersion, "v3");
  }

  private static void testVersionedPropertiesConfiguration(String versionHeader, String[] keysArray,
      boolean setDifferentSeparator)
      throws ConfigurationException {
    PropertiesConfiguration configuration =
        CommonsConfigurationUtils.getSegmentMetadataFromFile(CONFIG_FILE, true);

    // setting the random value of the test keys
    for (String key: keysArray) {
      configuration.setProperty(key, RandomStringUtils.randomAscii(5));

      // setting it at the key level as well for testing
      if (setDifferentSeparator) {
        configuration.getLayout().setSeparator(key, COLON_SEPARATOR);
      }
    }

    // set the different separator, other than '='
    if (setDifferentSeparator) {
      configuration.getLayout().setGlobalSeparator(COLON_SEPARATOR);
    }

    // recovered keys from the configuration.
    List<String> recoveredKeys = CommonsConfigurationUtils.getKeys(configuration);
    testPropertyKeys(recoveredKeys, keysArray);

    // save the configuration.
    CommonsConfigurationUtils.saveSegmentMetadataToFile(configuration, CONFIG_FILE, versionHeader);

    if (versionHeader != null) {
      String expectedHeader = CommonsConfigurationUtils.getVersionHeaderString(versionHeader);
      assertEquals(configuration.getHeader(), expectedHeader);
    }

    // reading the configuration from saved file.
    configuration = CommonsConfigurationUtils.getSegmentMetadataFromFile(CONFIG_FILE, true);
    recoveredKeys = CommonsConfigurationUtils.getKeys(configuration);
    testPropertyKeys(recoveredKeys, keysArray);
  }

  private static void testPropertyKeys(List<String> recoveredKeys, String[] actualKeys) {
    assertEquals(recoveredKeys.size(), actualKeys.length);
    for (int i = 0; i < recoveredKeys.size(); i++) {
      String recoveredValue = recoveredKeys.get(i);
      assertEquals(recoveredValue, actualKeys[i]);
    }
  }
}
