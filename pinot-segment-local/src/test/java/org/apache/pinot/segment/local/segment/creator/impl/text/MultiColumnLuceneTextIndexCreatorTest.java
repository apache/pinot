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
package org.apache.pinot.segment.local.segment.creator.impl.text;

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
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
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextIndexConstants;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultiColumnLuceneTextIndexCreatorTest {

  @Test
  public void testConvertMutableSegmentCleansStaleDestFiles()
      throws Exception {
    String column = "foo";
    String segmentName = "regSegMc";
    int numMutableDocs = 3;
    int numStaleDocs = 10;

    File scratch = Files.createTempDirectory(
        "MultiColumnLuceneTextIndexCreatorTest_stale_dest_").toFile();
    try {
      // 1. Build the mutable multi-column Lucene index at the location
      File consumerDir = new File(scratch, "consumerDir");
      File mutableIndexDir = new File(new File(consumerDir, segmentName),
          MultiColumnTextIndexConstants.INDEX_DIR_NAME
              + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
      FileUtils.forceMkdir(mutableIndexDir);
      writeLuceneDocs(mutableIndexDir, column, /* startDocId */ 0, numMutableDocs, /* multiSegment */ false);

      // 2. segmentIndexDir's parent must follow the "tmp-<segmentName>-<ts>" pattern
      File tmpSegmentParent = new File(scratch, "tmp-" + segmentName + "-1");
      File segmentIndexDir = new File(tmpSegmentParent, segmentName);
      FileUtils.forceMkdir(segmentIndexDir);

      // 3. Pre-populate the v1 destination with a multi-segment force-merged stale Lucene index
      File destIndexDir = new File(segmentIndexDir,
          MultiColumnTextIndexConstants.INDEX_DIR_NAME
              + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
      FileUtils.forceMkdir(destIndexDir);
      writeLuceneDocs(destIndexDir, column, /* startDocId */ 100, numStaleDocs, /* multiSegment */ true);
      File staleSentinel = new File(destIndexDir, "stale_from_prior_attempt.bin");
      FileUtils.writeByteArrayToFile(staleSentinel, new byte[]{0x01, 0x02, 0x03});

      // 4. Drive the conversion path: commit=true && realtimeConversion=true with reuseMutableIndex
      //    in the config triggers convertMutableSegment.
      MultiColumnTextIndexConfig mcConfig = new MultiColumnTextIndexConfig(
          List.of(column),
          Map.of(FieldConfig.TEXT_INDEX_LUCENE_REUSE_MUTABLE_INDEX, "true"),
          null);
      BooleanList columnsSV = new BooleanArrayList(new boolean[]{true});
      try (MultiColumnLuceneTextIndexCreator creator = new MultiColumnLuceneTextIndexCreator(
          List.of(column), columnsSV, segmentIndexDir, /* commit */ true,
          /* realtimeConversion */ true, consumerDir, /* immutableToMutableIdMap */ null,
          mcConfig)) {
        // Constructor performs the conversion; no add()/seal() on the reuse path.
      }

      // 5a. Dest wipe: sentinel must be gone.
      Assert.assertFalse(staleSentinel.exists(),
          "Stale file at v1 destination must be removed before conversion to "
              + "avoid CREATE_OR_APPEND picking up leftovers from a prior attempt");

      // 5b. Lucene's view of the destination must match the mutable input's doc count, not the
      //     stale leftover's.
      try (Directory destDir = FSDirectory.open(destIndexDir.toPath());
          DirectoryReader reader = DirectoryReader.open(destDir)) {
        Assert.assertEquals(reader.numDocs(), numMutableDocs,
            "Converted Lucene index must contain exactly the mutable input's document count, not "
                + "stale leftovers from a prior attempt");
      }

      // 5c. DocId mapping file (shared multi-column form) must be sized for the segment's numDocs.
      File mappingFile = new File(segmentIndexDir, MultiColumnTextIndexConstants.DOCID_MAPPING_FILE_NAME);
      Assert.assertTrue(mappingFile.exists(),
          "Shared multi-column docId mapping file should be built during conversion");
      Assert.assertEquals(mappingFile.length(), (long) Integer.BYTES * numMutableDocs,
          "DocId mapping file must be sized for the segment's numDocs only");
    } finally {
      FileUtils.deleteDirectory(scratch);
    }
  }

  private static void writeLuceneDocs(File indexDir, String column, int startDocId, int count,
      boolean multiSegment)
      throws IOException {
    try (Directory directory = FSDirectory.open(indexDir.toPath());
        IndexWriter writer = new IndexWriter(directory,
            new IndexWriterConfig(new StandardAnalyzer())
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE))) {
      int splitAt = multiSegment ? Math.max(1, count / 2) : count;
      for (int i = 0; i < splitAt; i++) {
        writer.addDocument(buildDoc(column, startDocId + i));
      }
      if (multiSegment) {
        writer.commit();
        for (int i = splitAt; i < count; i++) {
          writer.addDocument(buildDoc(column, startDocId + i));
        }
        writer.commit();
        writer.forceMerge(1, true);
      }
      writer.commit();
    }
  }

  private static Document buildDoc(String column, int docId) {
    Document doc = new Document();
    doc.add(new TextField(column, "doc " + docId, Field.Store.NO));
    doc.add(new StoredField(MultiColumnLuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME, docId));
    return doc;
  }
}
