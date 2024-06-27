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
package org.apache.pinot.segment.local.io.util;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Unit test for {@link VarLengthValueReader} and {@link VarLengthValueWriter}.
 */
public class VarLengthValueReaderWriterTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "VarLengthValueReaderWriterTest");
  private static final int MAX_STRING_LENGTH = 200;
  private static final int NUM_VALUES = 1000;

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testEmptyDictionary()
      throws IOException {
    File dictionaryFile = new File(TEMP_DIR, "empty");
    VarLengthValueWriter writer = new VarLengthValueWriter(dictionaryFile, 0);
    writer.close();
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(dictionaryFile)) {
      assertTrue(VarLengthValueReader.isVarLengthValueBuffer(dataBuffer));
      try (VarLengthValueReader reader = new VarLengthValueReader(dataBuffer)) {
        assertEquals(reader.getNumValues(), 0);
      }
    }
  }

  @Test
  public void testSingleValueDictionary()
      throws IOException {
    File dictionaryFile = new File(TEMP_DIR, "single");
    String value = RandomStringUtils.randomAlphanumeric(MAX_STRING_LENGTH);
    byte[] valueBytes = value.getBytes(UTF_8);
    try (VarLengthValueWriter writer = new VarLengthValueWriter(dictionaryFile, 1)) {
      writer.add(valueBytes);
    }
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(dictionaryFile)) {
      assertTrue(VarLengthValueReader.isVarLengthValueBuffer(dataBuffer));
      try (VarLengthValueReader reader = new VarLengthValueReader(dataBuffer)) {
        assertEquals(reader.getNumValues(), 1);
        byte[] buffer = new byte[MAX_STRING_LENGTH];
        assertEquals(reader.getUnpaddedString(0, MAX_STRING_LENGTH, buffer), value);
        assertEquals(reader.getBytes(0, MAX_STRING_LENGTH), valueBytes);
      }
    }
  }

  @Test
  public void testMultiValueDictionary()
      throws IOException {
    File dictionaryFile = new File(TEMP_DIR, "multi");
    String[] values = new String[NUM_VALUES];
    byte[][] valueBytesArray = new byte[NUM_VALUES][];
    for (int i = 0; i < NUM_VALUES; i++) {
      String value = RandomStringUtils.randomAlphanumeric(MAX_STRING_LENGTH);
      values[i] = value;
      valueBytesArray[i] = value.getBytes(UTF_8);
    }
    try (VarLengthValueWriter writer = new VarLengthValueWriter(dictionaryFile, NUM_VALUES)) {
      for (byte[] valueBytes : valueBytesArray) {
        writer.add(valueBytes);
      }
    }
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(dictionaryFile)) {
      assertTrue(VarLengthValueReader.isVarLengthValueBuffer(dataBuffer));
      try (VarLengthValueReader reader = new VarLengthValueReader(dataBuffer)) {
        assertEquals(reader.getNumValues(), NUM_VALUES);
        byte[] buffer = new byte[MAX_STRING_LENGTH];
        for (int i = 0; i < NUM_VALUES; i++) {
          assertEquals(reader.getUnpaddedString(i, MAX_STRING_LENGTH, buffer), values[i]);
          assertEquals(reader.getBytes(i, MAX_STRING_LENGTH), valueBytesArray[i]);
        }
      }
    }
  }
}
