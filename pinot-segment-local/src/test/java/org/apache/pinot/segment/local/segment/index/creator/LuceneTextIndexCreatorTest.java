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
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LuceneTextIndexCreatorTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), LuceneTextIndexCreatorTest.class.toString());

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);

    TextIndexConfig config = new TextIndexConfig(false, null, null, false, false, null, null, true, 500, null, null,
        null, null, false, false, 0, false, null);
    try (LuceneTextIndexCreator creator = new LuceneTextIndexCreator("foo", INDEX_DIR, true, false, null, null,
        config)) {
      creator.add("{\"clean\":\"this\"}");
      creator.add("{\"retain\":\"this\"}");
      creator.add("{\"keep\":\"this\"}");
      creator.add("{\"hold\":\"this\"}");
      creator.add("{\"clean\":\"that\"}");
      creator.seal();
    }
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testIndexWriterReaderMatchClean()
      throws IOException {
    // Use TextIndex reader to validate that reads work
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader("foo", INDEX_DIR, 5, new HashMap<>())) {
      int[] matchedDocIds = reader.getDocIds("clean").toArray();
      Assert.assertEquals(matchedDocIds.length, 2);
      Assert.assertEquals(matchedDocIds[0], 0);
      Assert.assertEquals(matchedDocIds[1], 4);
    }
  }

  @Test
  public void testIndexWriterReaderMatchHold()
      throws IOException {
    // Use TextIndex reader to validate that reads work
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader("foo", INDEX_DIR, 5, new HashMap<>())) {
      int[] matchedDocIds = reader.getDocIds("hold").toArray();
      Assert.assertEquals(matchedDocIds.length, 1);
      Assert.assertEquals(matchedDocIds[0], 3);
    }
  }

  @Test
  public void testIndexWriterReaderMatchRetain()
      throws IOException {
    // Use TextIndex reader to validate that reads work
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader("foo", INDEX_DIR, 5, new HashMap<>())) {
      int[] matchedDocIds = reader.getDocIds("retain").toArray();
      Assert.assertEquals(matchedDocIds.length, 1);
      Assert.assertEquals(matchedDocIds[0], 1);
    }
  }

  @Test
  public void testIndexWriterReaderMatchWithOrClause()
      throws IOException {
    // Use TextIndex reader to validate that reads work
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader("foo", INDEX_DIR, 5, new HashMap<>())) {
      int[] matchedDocIds = reader.getDocIds("retain|keep").toArray();
      Assert.assertEquals(matchedDocIds.length, 2);
      Assert.assertEquals(matchedDocIds[0], 1);
      Assert.assertEquals(matchedDocIds[1], 2);
    }
  }
}
