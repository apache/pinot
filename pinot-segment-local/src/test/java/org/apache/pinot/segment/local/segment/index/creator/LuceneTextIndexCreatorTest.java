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
import java.nio.ByteOrder;
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
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexBufferReader;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.TextIndexConfig.DocIdTranslatorMode;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


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
      assertEquals(matchedDocIds.length, 2);
      assertEquals(matchedDocIds[0], 0);
      assertEquals(matchedDocIds[1], 4);
    }
  }

  @Test
  public void testIndexWriterReaderMatchHold()
      throws IOException {
    // Use TextIndex reader to validate that reads work
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader("foo", INDEX_DIR, 5, new HashMap<>())) {
      int[] matchedDocIds = reader.getDocIds("hold").toArray();
      assertEquals(matchedDocIds.length, 1);
      assertEquals(matchedDocIds[0], 3);
    }
  }

  @Test
  public void testIndexWriterReaderMatchRetain()
      throws IOException {
    // Use TextIndex reader to validate that reads work
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader("foo", INDEX_DIR, 5, new HashMap<>())) {
      int[] matchedDocIds = reader.getDocIds("retain").toArray();
      assertEquals(matchedDocIds.length, 1);
      assertEquals(matchedDocIds[0], 1);
    }
  }

  @Test
  public void testIndexWriterReaderMatchWithOrClause()
      throws IOException {
    // Use TextIndex reader to validate that reads work
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader("foo", INDEX_DIR, 5, new HashMap<>())) {
      int[] matchedDocIds = reader.getDocIds("retain|keep").toArray();
      assertEquals(matchedDocIds.length, 2);
      assertEquals(matchedDocIds[0], 1);
      assertEquals(matchedDocIds[1], 2);
    }
  }

  @Test
  public void testDocValuesRetainStoredIdsForSingleAndMultiValueDocuments()
      throws IOException {
    String column = "values";
    TextIndexConfig config = new TextIndexConfigBuilder().build();
    try (LuceneTextIndexCreator creator =
        new LuceneTextIndexCreator(column, INDEX_DIR, true, false, null, null, config)) {
      creator.add("");
      creator.add(new String[]{"alpha", "beta"}, 2);
      creator.add(new String[0], 0);
      creator.add("gamma");
      creator.seal();
    }

    File indexDir = new File(INDEX_DIR, column + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    try (Directory directory = FSDirectory.open(indexDir.toPath());
        DirectoryReader reader = DirectoryReader.open(directory)) {
      NumericDocValues ids =
          MultiDocValues.getNumericValues(reader, LuceneTextIndexCreator.PINOT_DOC_ID_DOC_VALUES_FIELD_NAME);
      assertNull(MultiDocValues.getNumericValues(reader, LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME));
      assertEquals(reader.maxDoc(), 4);
      for (int i = 0; i < reader.maxDoc(); i++) {
        assertTrue(ids.advanceExact(i));
        // The retained stored field is precisely what an older Pinot reader consults.
        assertEquals(ids.longValue(),
            Long.parseLong(reader.storedFields().document(i)
                .get(LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME)));
      }
    }
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(column, INDEX_DIR, 4, config)) {
      assertEquals(reader.getDocIds("alpha", null).toArray(), new int[]{1});
      assertEquals(reader.getDocIds("gamma", null).toArray(), new int[]{3});
      assertEquals(reader.getDocIds("*:* ", null).toArray(), new int[]{0, 1, 2, 3});
    }
  }

  @DataProvider
  public Object[][] combinedMappingModes() {
    return new Object[][]{{null}, {DocIdTranslatorMode.Default}, {DocIdTranslatorMode.TryOptimize},
        {DocIdTranslatorMode.Skip}};
  }

  @Test(dataProvider = "combinedMappingModes")
  public void testCombinedIndexMapping(DocIdTranslatorMode translatorMode)
      throws IOException {
    String column = "combined";
    TextIndexConfig config = new TextIndexConfigBuilder().setStoreInSegmentFile(true)
        .withDocIdTranslatorMode(translatorMode == null ? null : translatorMode.name()).build();
    try (LuceneTextIndexCreator creator =
        new LuceneTextIndexCreator(column, INDEX_DIR, true, false, null, false, null, true, config)) {
      creator.add("alpha");
      creator.add("beta");
      creator.add("gamma");
      creator.seal();
      try (DirectoryReader reader = DirectoryReader.open(creator.getIndexWriter())) {
        assertNull(MultiDocValues.getNumericValues(reader, LuceneTextIndexCreator.PINOT_DOC_ID_DOC_VALUES_FIELD_NAME));
      }
    }

    File combinedFile = new File(INDEX_DIR, column + V1Constants.Indexes.LUCENE_COMBINE_TEXT_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer buffer = PinotDataBuffer.mapFile(combinedFile, true, 0, combinedFile.length(),
        ByteOrder.LITTLE_ENDIAN, "combined text index test")) {
      boolean mappingExpected = translatorMode == null || translatorMode == DocIdTranslatorMode.Default;
      assertEquals(LuceneTextIndexBufferReader.hasFile(buffer,
          column + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION), mappingExpected);
      DocIdTranslatorMode persistedMode =
          translatorMode == DocIdTranslatorMode.TryOptimize ? DocIdTranslatorMode.Skip : translatorMode;
      assertEquals(LuceneTextIndexBufferReader.extractProperties(buffer, column)
              .getProperty(FieldConfig.TEXT_INDEX_LUCENE_DOC_ID_TRANSLATOR_MODE),
          persistedMode == null ? null : persistedMode.name());
      try (LuceneTextIndexReader reader = new LuceneTextIndexReader(column, buffer, 3, config)) {
        assertEquals(reader.getDocIds("alpha", null).toArray(), new int[]{0});
        assertEquals(reader.getDocIds("gamma", null).toArray(), new int[]{2});
        assertEquals(reader.getDocIds("*:*", null).toArray(), new int[]{0, 1, 2});
      }
    }
  }

  @Test
  public void testDocValuesFieldNameColumnWithEmptyValues()
      throws IOException {
    String column = LuceneTextIndexCreator.PINOT_DOC_ID_DOC_VALUES_FIELD_NAME;
    TextIndexConfig config = new TextIndexConfigBuilder().build();
    try (LuceneTextIndexCreator creator =
        new LuceneTextIndexCreator(column, INDEX_DIR, true, false, null, null, config)) {
      creator.add(new String[0], 0);
      creator.add(new String[]{"alpha"}, 1);
      creator.seal();
    }
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(column, INDEX_DIR, 2, config)) {
      assertEquals(reader.getDocIds("alpha", null).toArray(), new int[]{1});
      assertEquals(reader.getDocIds("*:* ", null).toArray(), new int[]{0, 1});
    }
  }

  @Test
  public void testMutableIndexRetainsStoredFieldFallback()
      throws IOException {
    String column = "mutableValues";
    TextIndexConfig config = new TextIndexConfigBuilder().build();
    try (LuceneTextIndexCreator creator =
        new LuceneTextIndexCreator(column, INDEX_DIR, false, false, null, null, config)) {
      creator.add("alpha");
      creator.add(new String[0], 0);
      creator.getIndexWriter().commit();
      try (DirectoryReader reader = DirectoryReader.open(creator.getIndexWriter())) {
        assertNull(MultiDocValues.getNumericValues(reader, LuceneTextIndexCreator.PINOT_DOC_ID_DOC_VALUES_FIELD_NAME));
        assertEquals(reader.storedFields().document(0)
            .get(LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME), "0");
      }
      try (LuceneTextIndexReader reader = new LuceneTextIndexReader(column, INDEX_DIR, 2, config)) {
        assertEquals(reader.getDocIds("alpha", null).toArray(), new int[]{0});
        assertEquals(reader.getDocIds("*:* ", null).toArray(), new int[]{0, 1});
      }
    }
  }

  @DataProvider
  public Object[][] mutableMappingModes() {
    return new Object[][]{
        {null, new int[]{0, 1, 2}, null, true},
        {DocIdTranslatorMode.TryOptimize, new int[]{0, 1, 2}, null, false},
        {DocIdTranslatorMode.TryOptimize, new int[]{0, 1, 2}, new int[]{2, 0, 1}, true},
        // Only the final immutable IDs are sequential in this case.
        {DocIdTranslatorMode.TryOptimize, new int[]{2, 0, 1}, new int[]{1, 2, 0}, false}
    };
  }

  /// Regression test for stale destination files, including mappings made identity or nonidentity by conversion.
  @Test(dataProvider = "mutableMappingModes")
  public void testConvertMutableSegmentCleansStaleDestFiles(DocIdTranslatorMode translatorMode, int[] mutableDocIds,
      int[] mutableToImmutableDocIdMap, boolean mappingExpected)
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
      try (Directory directory = FSDirectory.open(mutableIndexDir.toPath());
          IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {
        for (int mutableDocId : mutableDocIds) {
          writer.addDocument(buildDoc(mutableDocId));
        }
      }

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
      TextIndexConfig config = new TextIndexConfigBuilder().withReuseMutableIndex(true)
          .withDocIdTranslatorMode(translatorMode == null ? null : translatorMode.name()).build();
      try (LuceneTextIndexCreator creator = new LuceneTextIndexCreator(column, segmentIndexDir,
          /* commit */ true, /* realtimeConversion */ true, consumerDir,
          /* mutableSegmentCompacted */ false, mutableToImmutableDocIdMap,
          /* combineAndCleanupFiles */ false, config)) {
        // Constructor performs the conversion; no add()/seal() on the reuse path.
      }

      // 5a. Dest wipe: sentinel must be gone.
      assertFalse(staleSentinel.exists(),
          "Stale file at v1 destination must be removed before conversion to "
              + "avoid CREATE_OR_APPEND picking up leftovers from a prior attempt");

      // 5b. Lucene's view of the destination must match the mutable input's doc count, not the
      //     stale leftover's. Pre-fix, Lucene opens stale's higher-N segments file and reports
      //     numStaleDocs instead.
      try (Directory destDir = FSDirectory.open(destIndexDir.toPath());
          DirectoryReader reader = DirectoryReader.open(destDir)) {
        assertEquals(reader.numDocs(), numMutableDocs,
            "Converted Lucene index must contain exactly the mutable input's document count, not "
                + "stale leftovers from a prior attempt");
      }

      // 5c. DocId mapping file is sized by Lucene's numDocs at write time and remapped by the
      //     segment's numDocs at read time - a mismatch here is what produced AIOOB in production.
      File mappingFile = new File(segmentIndexDir,
          column + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
      assertEquals(mappingFile.exists(), mappingExpected);
      if (mappingExpected) {
        assertEquals(mappingFile.length(), (long) Integer.BYTES * numMutableDocs,
            "DocId mapping file must be sized for the segment's numDocs only");
      }
      if (translatorMode == DocIdTranslatorMode.TryOptimize) {
        File propertiesFile = new File(destIndexDir, V1Constants.Indexes.LUCENE_TEXT_INDEX_PROPERTIES_FILE);
        assertEquals(TextIndexUtils.getUpdatedConfigFromPropertiesFile(propertiesFile, config).getDocIdTranslatorMode(),
            mappingExpected ? DocIdTranslatorMode.Default : DocIdTranslatorMode.Skip);
      }

      // 5d. End-to-end read
      try (LuceneTextIndexReader reader = new LuceneTextIndexReader(column, segmentIndexDir,
          numMutableDocs, config)) {
        assertEquals(reader.getDocIds("doc", null).toArray(), new int[]{0, 1, 2});
        assertEquals(reader.getDocIds("0", null).toArray(),
            new int[]{mutableToImmutableDocIdMap == null ? 0 : mutableToImmutableDocIdMap[0]});
      }
    } finally {
      FileUtils.deleteDirectory(scratch);
    }
  }

  @Test
  public void testConvertMutableSegmentRejectsMissingStoredDocId()
      throws IOException {
    String column = "foo";
    String segmentName = "missingDocId";
    File consumerDir = new File(INDEX_DIR, "consumerDir");
    File mutableIndexDir = new File(new File(consumerDir, segmentName),
        column + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    FileUtils.forceMkdir(mutableIndexDir);
    try (Directory directory = FSDirectory.open(mutableIndexDir.toPath());
        IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {
      writer.addDocument(buildDoc(0));
      Document corruptDocument = buildDoc(1);
      corruptDocument.removeField(LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME);
      writer.addDocument(corruptDocument);
      writer.commit();
    }

    File segmentIndexDir = new File(new File(INDEX_DIR, "tmp-" + segmentName + "-1"), segmentName);
    FileUtils.forceMkdir(segmentIndexDir);
    TextIndexConfig config = new TextIndexConfigBuilder().withReuseMutableIndex(true).build();
    RuntimeException exception = expectThrows(RuntimeException.class, () -> {
      try (LuceneTextIndexCreator creator =
          new LuceneTextIndexCreator(column, segmentIndexDir, true, true, consumerDir, null, config)) {
        // Conversion must reject the legacy index before a reader can consume its mapping.
      }
    });
    Throwable rootCause = exception;
    while (rootCause.getCause() != null) {
      rootCause = rootCause.getCause();
    }
    assertTrue(rootCause instanceof IOException);
    assertEquals(rootCause.getMessage(), "Missing Pinot document id for Lucene document: 1");
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
