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
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.creator.impl.inv.text.LuceneIFSTIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.LuceneIFSTIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.LUCENE_V912_IFST_INDEX_FILE_EXTENSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class LuceneIFSTIndexCreatorTest implements PinotBuffersAfterMethodCheckRule {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "LuceneIFSTIndex");

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
    String[] uniqueValues = {
        "Hello-World",
        "HELLO-WORLD",
        "hello-world123",
        "HELLO-WORLD123",
        "Still",
        "STILL"
    };

    try (LuceneIFSTIndexCreator creator = new LuceneIFSTIndexCreator(INDEX_DIR, "testIFSTColumn", "myTable_OFFLINE",
        false, uniqueValues)) {
      creator.seal();
    }
    File ifstFile = new File(INDEX_DIR, "testIFSTColumn" + LUCENE_V912_IFST_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(ifstFile);
        LuceneIFSTIndexReader reader = new LuceneIFSTIndexReader(dataBuffer)) {
      int[] matchedDictIds = reader.getDictIds("hello.*world").toArray();
      assertEquals(matchedDictIds.length, 2);
      assertEquals(matchedDictIds[0], 0);
      assertEquals(matchedDictIds[1], 1);

      matchedDictIds = reader.getDictIds("HELLO.*WORLD123").toArray();
      assertEquals(matchedDictIds.length, 2);
      assertEquals(matchedDictIds[0], 2);
      assertEquals(matchedDictIds[1], 3);

      matchedDictIds = reader.getDictIds("still").toArray();
      assertEquals(matchedDictIds.length, 2);
      assertEquals(matchedDictIds[0], 4);
      assertEquals(matchedDictIds[1], 5);
    }
  }

  @Test
  public void testSerializeDeserializeDictionaryIds() {
    List<Integer> empty = new ArrayList<>();
    BytesRef serializedEmpty = LuceneIFSTIndexCreator.serializeDictionaryIds(empty);
    assertTrue(LuceneIFSTIndexCreator.deserializeDictionaryIds(serializedEmpty).isEmpty());

    List<Integer> values = List.of(42, -7, 0, Integer.MAX_VALUE, Integer.MIN_VALUE);
    BytesRef serializedValues = LuceneIFSTIndexCreator.serializeDictionaryIds(values);
    assertEquals(LuceneIFSTIndexCreator.deserializeDictionaryIds(serializedValues), values);

    try {
      LuceneIFSTIndexCreator.serializeDictionaryIds(null);
      Assert.fail("Expected IllegalArgumentException for null dictionary id list");
    } catch (IllegalArgumentException e) {
      // expected
    }

    assertTrue(LuceneIFSTIndexCreator.deserializeDictionaryIds(null).isEmpty());
    assertTrue(LuceneIFSTIndexCreator.deserializeDictionaryIds(new BytesRef()).isEmpty());

    try {
      LuceneIFSTIndexCreator.deserializeDictionaryIds(new BytesRef(new byte[3]));
      Assert.fail("Expected RuntimeException for truncated IFST payload");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("not enough bytes for dictionary id list size"));
    }
  }
}
