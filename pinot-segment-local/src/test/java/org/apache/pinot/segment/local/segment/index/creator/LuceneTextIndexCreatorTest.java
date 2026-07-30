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
import java.nio.file.Files;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.V1Constants;
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

    TextIndexConfig config = new TextIndexConfigBuilder().build();
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

  /**
   * Regression test for the AIOOB triggered when a prior segment-conversion attempt left stale
   * Lucene files at the v1 destination directory and the reuse-mutable-index path is taken.
   */
  @Test
  public void testConvertMutableSegmentCleansStaleDestFiles()
      throws Exception {
    String column = "foo";
    String segmentName = "regSeg";
    int numMutableDocs = 3;
    int numStaleDocs = 10;

    File scratch = Files.createTempDirectory(
        "LuceneTextIndexCreatorTest_stale_dest_").toFile();
    try {
      // 1. Build the mutable Lucene index
      File consumerDir = new File(scratch, "consumerDir");
      File mutableIndexDir = new File(new File(consumerDir, segmentName),
          column + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
      FileUtils.forceMkdir(mutableIndexDir);
      writeLuceneDocs(mutableIndexDir, /* startDocId */ 0, numMutableDocs, /* multiSegment */ false);

      // 2. segmentIndexDir's parent
      File tmpSegmentParent = new File(scratch, "tmp-" + segmentName + "-1");
      File segmentIndexDir = new File(tmpSegmentParent, segmentName);
      FileUtils.forceMkdir(segmentIndexDir);

      // 3. Pre-populate the v1 destination with stale Lucene files that simulate a prior crashed
      //    or killed conversion
      File destIndexDir = new File(segmentIndexDir,
          column + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
      FileUtils.forceMkdir(destIndexDir);
      writeLuceneDocs(destIndexDir, /* startDocId */ 100, numStaleDocs, /* multiSegment */ true);

      File staleSentinel = new File(destIndexDir, "stale_from_prior_attempt.bin");
      FileUtils.writeByteArrayToFile(staleSentinel, new byte[]{0x01, 0x02, 0x03});

      // 4. commit=true && realtimeConversion=true && reuseMutableIndex=true triggers
      //    convertMutableSegment.
      TextIndexConfig config = new TextIndexConfigBuilder().withReuseMutableIndex(true).build();
      try (LuceneTextIndexCreator creator = new LuceneTextIndexCreator(column, segmentIndexDir,
          /* commit */ true, /* realtimeConversion */ true, consumerDir,
          /* mutableSegmentCompacted */ false, /* immutableToMutableIdMap */ null,
          /* combineAndCleanupFiles */ false, config)) {
        // Constructor performs the conversion; no add()/seal() on the reuse path.
      }

      // 5a. Dest wipe: sentinel must be gone.
      Assert.assertFalse(staleSentinel.exists(),
          "Stale file at v1 destination must be removed before conversion to "
              + "avoid CREATE_OR_APPEND picking up leftovers from a prior attempt");

      // 5b. Lucene's view of the destination must match the mutable input's doc count, not the
      //     stale leftover's. Pre-fix, Lucene opens stale's higher-N segments file and reports
      //     numStaleDocs instead.
      try (Directory destDir = FSDirectory.open(destIndexDir.toPath());
          DirectoryReader reader = DirectoryReader.open(destDir)) {
        Assert.assertEquals(reader.numDocs(), numMutableDocs,
            "Converted Lucene index must contain exactly the mutable input's document count, not "
                + "stale leftovers from a prior attempt");
      }

      // 5c. DocId mapping file is sized by Lucene's numDocs at write time and remapped by the
      //     segment's numDocs at read time - a mismatch here is what produced AIOOB in production.
      File mappingFile = new File(segmentIndexDir,
          column + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
      Assert.assertTrue(mappingFile.exists(), "DocId mapping file should be built during conversion");
      Assert.assertEquals(mappingFile.length(), (long) Integer.BYTES * numMutableDocs,
          "DocId mapping file must be sized for the segment's numDocs only");

      // 5d. End-to-end read
      try (LuceneTextIndexReader reader = new LuceneTextIndexReader(column, segmentIndexDir,
          numMutableDocs, config)) {
        int[] matched = reader.getDocIds("doc").toArray();
        for (int docId : matched) {
          Assert.assertTrue(docId >= 0 && docId < numMutableDocs,
              "Returned Pinot docId " + docId + " must be within [0, " + numMutableDocs + ")");
        }
      }
    } finally {
      FileUtils.deleteDirectory(scratch);
    }
  }

  private static void writeLuceneDocs(File indexDir, int startDocId, int count, boolean multiSegment)
      throws IOException {
    try (Directory directory = FSDirectory.open(indexDir.toPath());
        IndexWriter writer = new IndexWriter(directory,
            new IndexWriterConfig(new StandardAnalyzer())
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE))) {
      int splitAt = multiSegment ? Math.max(1, count / 2) : count;
      for (int i = 0; i < splitAt; i++) {
        writer.addDocument(buildDoc(startDocId + i));
      }
      if (multiSegment) {
        writer.commit();
        for (int i = splitAt; i < count; i++) {
          writer.addDocument(buildDoc(startDocId + i));
        }
        writer.commit();
        writer.forceMerge(1, true);
      }
      writer.commit();
    }
  }

  private static Document buildDoc(int docId) {
    Document doc = new Document();
    doc.add(new TextField("foo", "doc " + docId, Field.Store.NO));
    doc.add(new StoredField(LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME, docId));
    return doc;
  }
}
