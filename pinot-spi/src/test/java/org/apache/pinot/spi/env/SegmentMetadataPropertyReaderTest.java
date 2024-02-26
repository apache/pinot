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
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SegmentMetadataPropertyReaderTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SegmentMetadataPropertyReaderTest");
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
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testSegmentMetadataReader()
      throws ConfigurationException, IOException {
    PropertiesConfiguration configuration = CommonsConfigurationUtils.fromFile(CONFIG_FILE, true, true,
        PropertyReaderKind.SegmentMetadataPropertyReader);

    // setting the random value of the test keys
    for (String key: TEST_PROPERTY_KEY) {
      configuration.setProperty(key, RandomStringUtils.randomAscii(5));
    }
    // recovered keys from the configuration.
    List<String> recoveredKeys = CommonsConfigurationUtils.getKeys(configuration);
    testPropertyKeys(recoveredKeys, TEST_PROPERTY_KEY, false);

    CommonsConfigurationUtils.saveToFile(configuration, CONFIG_FILE); // save the configuration.

    // reading the configuration from saved file.
    configuration = CommonsConfigurationUtils.fromFile(CONFIG_FILE, true, true,
        PropertyReaderKind.SegmentMetadataPropertyReader);
    recoveredKeys = CommonsConfigurationUtils.getKeys(configuration);
    testPropertyKeys(recoveredKeys, TEST_PROPERTY_KEY, false);

    FileUtils.deleteDirectory(TEMP_DIR); // clearing for next test.
  }

  @Test
  public void testSegmentMetadataReaderWithSpecialChars()
      throws ConfigurationException, IOException {
    PropertiesConfiguration configuration = CommonsConfigurationUtils.fromFile(CONFIG_FILE, true, true,
        PropertyReaderKind.SegmentMetadataPropertyReader);

    // setting the random value of the test keys
    for (String key: TEST_PROPERTY_KEY_WITH_SPECIAL_CHAR) {
      configuration.setProperty(key, RandomStringUtils.randomAscii(5));
    }
    // recovered keys from the configuration.
    List<String> recoveredKeys = CommonsConfigurationUtils.getKeys(configuration);
    testPropertyKeys(recoveredKeys, TEST_PROPERTY_KEY_WITH_SPECIAL_CHAR, false);

    CommonsConfigurationUtils.saveToFile(configuration, CONFIG_FILE); // save the configuration.

    // reading the configuration from saved file.
    configuration = CommonsConfigurationUtils.fromFile(CONFIG_FILE, true, true,
        PropertyReaderKind.SegmentMetadataPropertyReader);
    recoveredKeys = CommonsConfigurationUtils.getKeys(configuration);
    testPropertyKeys(recoveredKeys, TEST_PROPERTY_KEY_WITH_SPECIAL_CHAR, true); // require escaping.

    FileUtils.deleteDirectory(TEMP_DIR); // clearing for next test.
  }

  private static void testPropertyKeys(List<String> recoveredKeys, String[] actualKeys, boolean escapeKeys) {
    assertEquals(recoveredKeys.size(), actualKeys.length);
    for (int i = 1; i < recoveredKeys.size(); i++) {
      String recoveredValue = recoveredKeys.get(i);
      if (escapeKeys) {
        recoveredValue = StringEscapeUtils.unescapeJava(recoveredValue);
      }
      assertEquals(recoveredValue, actualKeys[i]);
    }
  }
}
