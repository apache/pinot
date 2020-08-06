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
package org.apache.pinot.core.segment.creator.impl;

import java.io.File;
import java.io.IOException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class SegmentColumnarIndexCreatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SegmentColumnarIndexCreatorTest");
  private static final File CONFIG_FILE = new File(TEMP_DIR, "config");
  private static final String PROPERTY_KEY = "testKey";
  private static final String COLUMN_NAME = "testColumn";
  private static final String COLUMN_PROPERTY_KEY_PREFIX =
      V1Constants.MetadataKeys.Column.COLUMN_PROPS_KEY_PREFIX + COLUMN_NAME + ".";
  private static final int NUM_ROUNDS = 1000;

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testPropertyValueWithSpecialCharacters()
      throws Exception {
    // Leading/trailing whitespace
    assertFalse(SegmentColumnarIndexCreator.isValidPropertyValue(" a"));
    assertFalse(SegmentColumnarIndexCreator.isValidPropertyValue("a\t"));
    assertFalse(SegmentColumnarIndexCreator.isValidPropertyValue("\na"));

    // Whitespace in the middle
    assertTrue(SegmentColumnarIndexCreator.isValidPropertyValue("a\t b"));
    testPropertyValueWithSpecialCharacters("a\t b");
    assertTrue(SegmentColumnarIndexCreator.isValidPropertyValue("a \nb"));
    testPropertyValueWithSpecialCharacters("a \nb");

    // List separator
    assertFalse(SegmentColumnarIndexCreator.isValidPropertyValue("a,b,c"));
    assertFalse(SegmentColumnarIndexCreator.isValidPropertyValue(",a b"));

    // Empty string
    assertTrue(SegmentColumnarIndexCreator.isValidPropertyValue(""));
    testPropertyValueWithSpecialCharacters("");

    // Variable substitution (should be disabled)
    assertTrue(SegmentColumnarIndexCreator.isValidPropertyValue("$${testKey}"));
    testPropertyValueWithSpecialCharacters("$${testKey}");

    // Escape character for variable substitution
    assertTrue(SegmentColumnarIndexCreator.isValidPropertyValue("$${"));
    testPropertyValueWithSpecialCharacters("$${");

    for (int i = 0; i < NUM_ROUNDS; i++) {
      testPropertyValueWithSpecialCharacters(RandomStringUtils.randomAscii(5));
      testPropertyValueWithSpecialCharacters(RandomStringUtils.random(5));
    }
  }

  private void testPropertyValueWithSpecialCharacters(String value)
      throws Exception {
    if (SegmentColumnarIndexCreator.isValidPropertyValue(value)) {
      PropertiesConfiguration configuration = new PropertiesConfiguration(CONFIG_FILE);
      configuration.setProperty(PROPERTY_KEY, value);
      assertEquals(configuration.getProperty(PROPERTY_KEY), value);
      configuration.save();

      configuration = new PropertiesConfiguration(CONFIG_FILE);
      assertEquals(configuration.getProperty(PROPERTY_KEY), value);
    }
  }

  @Test
  public void testRemoveColumnMetadataInfo()
      throws Exception {
    PropertiesConfiguration configuration = new PropertiesConfiguration(CONFIG_FILE);
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "a", "foo");
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "b", "bar");
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "c", "foobar");
    configuration.save();

    configuration = new PropertiesConfiguration(CONFIG_FILE);
    assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
    SegmentColumnarIndexCreator.removeColumnMetadataInfo(configuration, COLUMN_NAME);
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
    configuration.save();

    configuration = new PropertiesConfiguration(CONFIG_FILE);
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
