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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.inv.text.LuceneFSTIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.LuceneFSTIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.FST_INDEX_FILE_EXTENSION;


public class LuceneFSTIndexCreatorTest {
  private static final File INDEX_DIR = new File("/Users/atrisharma/");

  /**
   * A comparator comparing full byte arrays. Unsigned byte comparisons ('C'-locale).
   */
  public static final Comparator<String> LEXICAL_ORDERING = (o1, o2) -> o1.compareToIgnoreCase(o2);

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    //FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testIndexWriterReader()
      throws IOException {
    String[] uniqueValues = new String[3];
    uniqueValues[0] = "hello-world";
    uniqueValues[1] = "hello-world123";
    uniqueValues[2] = "still";

    LuceneFSTIndexCreator creator = new LuceneFSTIndexCreator(INDEX_DIR, "testFSTColumn", uniqueValues);
    creator.seal();
    File fstFile = new File(INDEX_DIR, "testFSTColumn" + FST_INDEX_FILE_EXTENSION);
    PinotDataBuffer pinotDataBuffer =
        PinotDataBuffer.mapFile(fstFile, true, 0, fstFile.length(), ByteOrder.BIG_ENDIAN, "fstIndexFile");
    LuceneFSTIndexReader reader = new LuceneFSTIndexReader(pinotDataBuffer);
    int[] matchedDictIds = reader.getDictIds("hello.*").toArray();
    Assert.assertEquals(2, matchedDictIds.length);
    Assert.assertEquals(0, matchedDictIds[0]);
    Assert.assertEquals(1, matchedDictIds[1]);

    matchedDictIds = reader.getDictIds(".*llo").toArray();
    Assert.assertEquals(0, matchedDictIds.length);
  }

  @Test
  public void testIndexWriterReader2()
      throws IOException {
    List<String> inputStrings = new ArrayList<>();
    InputStream fileInputStream = null;
    InputStreamReader inputStreamReader = null;
    BufferedReader bufferedReader = null;

    File file = new File("./src/test/resources/data/words.txt");

    fileInputStream = new FileInputStream(file);
    inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
    bufferedReader = new BufferedReader(inputStreamReader);

    String currentWord;
    while((currentWord = bufferedReader.readLine()) != null) {
      inputStrings.add(currentWord);
    }

    String[] array = inputStrings.toArray(new String[0]);

    Arrays.sort(array, LEXICAL_ORDERING);

    LuceneFSTIndexCreator creator = new LuceneFSTIndexCreator(INDEX_DIR, "testFSTColumn", array);
    creator.seal();
    File fstFile = new File(INDEX_DIR, "testFSTColumn" + FST_INDEX_FILE_EXTENSION);
    PinotDataBuffer pinotDataBuffer =
        PinotDataBuffer.mapFile(fstFile, true, 0, fstFile.length(), ByteOrder.BIG_ENDIAN, "fstIndexFile");
    LuceneFSTIndexReader reader = new LuceneFSTIndexReader(pinotDataBuffer);
    int[] matchedDictIds = reader.getDictIds("hello.*").toArray();
    Assert.assertEquals(2, matchedDictIds.length);
    Assert.assertEquals(0, matchedDictIds[0]);
    Assert.assertEquals(1, matchedDictIds[1]);

    matchedDictIds = reader.getDictIds(".*llo").toArray();
    Assert.assertEquals(0, matchedDictIds.length);
  }
}
