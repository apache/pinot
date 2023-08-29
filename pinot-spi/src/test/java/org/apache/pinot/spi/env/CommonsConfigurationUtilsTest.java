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
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class CommonsConfigurationUtilsTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "CommonsConfigurationUtilsTest");
  private static final File CONFIG_FILE = new File(TEMP_DIR, "config");
  private static final String PROPERTY_KEY = "testKey";
  private static final int NUM_ROUNDS = 1000;

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
  public void testPropertyValueWithSpecialCharacters()
      throws ConfigurationException {
    // Leading/trailing whitespace
    testPropertyValueWithSpecialCharacters(" a");
    testPropertyValueWithSpecialCharacters("a ");
    testPropertyValueWithSpecialCharacters(" a ");
    testPropertyValueWithSpecialCharacters("  a  ");
    testPropertyValueWithSpecialCharacters(" a\t");
    testPropertyValueWithSpecialCharacters("\na ");

    // Whitespace in the middle
    testPropertyValueWithSpecialCharacters("a\t b");
    testPropertyValueWithSpecialCharacters("a \nb");

    // List separator (comma)
    testPropertyValueWithSpecialCharacters("a,b,c");
    testPropertyValueWithSpecialCharacters(",a b");
    testPropertyValueWithSpecialCharacters(",a,,,b,,c,");

    // List separator with backslashes
    testPropertyValueWithSpecialCharacters("a\\,b,\\c");
    testPropertyValueWithSpecialCharacters("\\a\\,,b\\, \\c");
    testPropertyValueWithSpecialCharacters("a\\\\,, ,b");
    testPropertyValueWithSpecialCharacters("a\\\\\\,b");

    // Empty string
    testPropertyValueWithSpecialCharacters("");

    // Variable substitution (should be disabled)
    testPropertyValueWithSpecialCharacters("$${testKey}");

    // Escape character for variable substitution
    testPropertyValueWithSpecialCharacters("$${");

    for (int i = 0; i < NUM_ROUNDS; i++) {
      testPropertyValueWithSpecialCharacters(RandomStringUtils.randomAscii(5));
      testPropertyValueWithSpecialCharacters(StringUtils.remove(RandomStringUtils.random(5), '\0'));
    }
  }

  private void testPropertyValueWithSpecialCharacters(String value)
      throws ConfigurationException {
    String replacedValue = CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(value);
    PropertiesConfiguration configuration = new PropertiesConfiguration(CONFIG_FILE);
    configuration.setProperty(PROPERTY_KEY, replacedValue);
    String recoveredValue = CommonsConfigurationUtils.recoverSpecialCharacterInPropertyValue(
        (String) configuration.getProperty(PROPERTY_KEY));
    assertEquals(recoveredValue, value);
    configuration.save();
    configuration = new PropertiesConfiguration(CONFIG_FILE);
    recoveredValue = CommonsConfigurationUtils.recoverSpecialCharacterInPropertyValue(
        (String) configuration.getProperty(PROPERTY_KEY));
    assertEquals(recoveredValue, value);
  }
}
