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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.creator.impl.inv.text.LuceneFSTIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.LuceneFSTIndexReader;
import org.apache.pinot.segment.local.utils.fst.FSTBuilder;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.LUCENE_V912_FST_INDEX_FILE_EXTENSION;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class LuceneFSTIndexCreatorTest implements PinotBuffersAfterMethodCheckRule {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "LuceneFSTIndex");

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testIndexWriterReader()
      throws IOException {
    String[] uniqueValues = new String[3];
    uniqueValues[0] = "hello-world";
    uniqueValues[1] = "hello-world123";
    uniqueValues[2] = "still";

    try (LuceneFSTIndexCreator creator = new LuceneFSTIndexCreator(INDEX_DIR, "testFSTColumn", "myTable_OFFLINE", false,
        uniqueValues)) {
      creator.seal();
    }
    File fstFile = new File(INDEX_DIR, "testFSTColumn" + LUCENE_V912_FST_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(fstFile);
        LuceneFSTIndexReader reader = new LuceneFSTIndexReader(dataBuffer)) {
      int[] matchedDictIds = reader.getDictIds("hello.*").toArray();
      assertEquals(matchedDictIds.length, 2);
      assertEquals(matchedDictIds[0], 0);
      assertEquals(matchedDictIds[1], 1);

      matchedDictIds = reader.getDictIds(".*llo").toArray();
      assertEquals(matchedDictIds.length, 0);

      matchedDictIds = reader.getDictIds("st.*").toArray();
      assertEquals(matchedDictIds.length, 1);
      assertEquals(matchedDictIds[0], 2);
    }
  }

  @Test
  public void testIndexWriterReaderWithAddExceptionsContinueOnErrorTrue()
      throws IOException {
    String[] uniqueValues = new String[3];
    uniqueValues[0] = "hello-world";
    uniqueValues[1] = "hello-world123";
    uniqueValues[2] = "still";

    FSTBuilder fstBuilder = spy(new FSTBuilder());
    // For the word "still" throw an exception so it is not indexed
    doThrow(IOException.class).when(fstBuilder).addEntry(eq("still"), anyInt());
    try (LuceneFSTIndexCreator creator = new LuceneFSTIndexCreator(INDEX_DIR, "testFSTColumn", "myTable_OFFLINE", true,
        uniqueValues, fstBuilder)) {
      creator.seal();
    }
    File fstFile = new File(INDEX_DIR, "testFSTColumn" + LUCENE_V912_FST_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(fstFile);
        LuceneFSTIndexReader reader = new LuceneFSTIndexReader(dataBuffer)) {
      int[] matchedDictIds = reader.getDictIds("hello.*").toArray();
      assertEquals(matchedDictIds.length, 2);
      assertEquals(matchedDictIds[0], 0);
      assertEquals(matchedDictIds[1], 1);

      matchedDictIds = reader.getDictIds(".*llo").toArray();
      assertEquals(matchedDictIds.length, 0);

      // Validate that nothing matches st.*
      matchedDictIds = reader.getDictIds("st.*").toArray();
      assertEquals(matchedDictIds.length, 0);
    }
  }

  @Test
  public void testIndexWriterReaderWithAddExceptionsContinueOnErrorFalse()
      throws IOException {
    String[] uniqueValues = new String[3];
    uniqueValues[0] = "hello-world";
    uniqueValues[1] = "hello-world123";
    uniqueValues[2] = "still";

    FSTBuilder fstBuilder = spy(new FSTBuilder());
    // For the word "still" throw an exception so it is not indexed
    doThrow(IOException.class).when(fstBuilder).addEntry(eq("still"), anyInt());
    assertThrows(IOException.class,
        () -> new LuceneFSTIndexCreator(INDEX_DIR, "testFSTColumn", "myTable_OFFLINE", false, uniqueValues,
            fstBuilder));
  }
}
