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
package org.apache.pinot.segment.local.segment.index.readers.text;

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.text.MultiColumnLuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.TextIndexConfig.DocIdTranslatorMode;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextScoreStats;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.exception.QueryException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Unit tests for [LuceneDocScorer], [LuceneTextIndexReader#createDocScorer], and
/// [MultiColumnLuceneTextIndexReader#createDocScorer] covering both the identity ([NoOpDocIdTranslator]) and
/// non-identity ([DefaultDocIdTranslator]) execution paths.
///
/// Not thread-safe: shares a per-instance temp directory across test methods.
public class LuceneDocScorerTest {
  private static final String COLUMN = "title";
  private static final String BODY_COLUMN = "body";

  private static final String[] SAMPLE_DOCS = {
      "usb charger",
      "usb charger braided cable wall adapter travel kit pouch",
      "usb usb usb hub",
      "usb power bank hub",
      "hdmi monitor cable",
      "wireless mouse",
      "charger usb reversed order",
      "usb charger usb charger compact",
      "cable adapter dock",
      "keyboard mechanical"
  };

  private File _tempDir;
  private File _identityIndexDir;
  private File _multiColIdentityDir;
  private File _multiColDefaultDir;

  @BeforeClass
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "LuceneDocScorerTest-" + UUID.randomUUID());
    _identityIndexDir = new File(_tempDir, "single-identity");
    FileUtils.forceMkdir(_identityIndexDir);
    TextIndexConfig tryOptimizeConfig =
        new TextIndexConfigBuilder().withDocIdTranslatorMode(DocIdTranslatorMode.TryOptimize.name()).build();
    try (LuceneTextIndexCreator creator = new LuceneTextIndexCreator(COLUMN, _identityIndexDir, true, false, null, null,
        tryOptimizeConfig)) {
      for (String doc : SAMPLE_DOCS) {
        creator.add(doc);
      }
      creator.seal();
    }

    _multiColIdentityDir = new File(_tempDir, "multi-identity");
    _multiColDefaultDir = new File(_tempDir, "multi-default");
    buildMultiColumnIndex(_multiColIdentityDir, DocIdTranslatorMode.TryOptimize);
    buildMultiColumnIndex(_multiColDefaultDir, DocIdTranslatorMode.Default);
  }

  private static void buildMultiColumnIndex(File dir, DocIdTranslatorMode mode)
      throws IOException {
    FileUtils.forceMkdir(dir);
    Map<String, String> sharedProps = Map.of(FieldConfig.TEXT_INDEX_LUCENE_DOC_ID_TRANSLATOR_MODE, mode.name());
    MultiColumnTextIndexConfig mcConfig =
        new MultiColumnTextIndexConfig(List.of(COLUMN, BODY_COLUMN), sharedProps, Map.of());
    try (MultiColumnLuceneTextIndexCreator creator = new MultiColumnLuceneTextIndexCreator(List.of(COLUMN, BODY_COLUMN),
        new BooleanArrayList(new boolean[]{true, true}), dir, true, false, null, null, mcConfig)) {
      for (int i = 0; i < SAMPLE_DOCS.length; i++) {
        creator.add(List.of(SAMPLE_DOCS[i], SAMPLE_DOCS[SAMPLE_DOCS.length - 1 - i]));
      }
      creator.seal();
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  @Test
  public void testIdentityPathQueriesAndGlobalStatsParity()
      throws IOException {
    TextIndexConfig config =
        new TextIndexConfigBuilder().withDocIdTranslatorMode(DocIdTranslatorMode.TryOptimize.name()).build();
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, _identityIndexDir, SAMPLE_DOCS.length,
        config)) {
      List<String> queries = List.of("usb", "usb AND charger", "usb OR cable", "\"usb charger\"", "nonexistentterm");
      for (String query : queries) {
        Int2FloatMap expected = reader.getDocScores(query, null);

        // Single ascending block covering all docs
        LuceneDocScorer fullScorer = (LuceneDocScorer) reader.createDocScorer(query, null, null);
        assertBlockMatches(fullScorer, expected, new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, query);
        assertFalse(fullScorer.isEagerFallbackActive(),
            "Single-block identity query must not materialize map: " + query);

        // Multi-block ascending streaming stays zero-heap for all query shapes on identity segments
        LuceneDocScorer streamedScorer = (LuceneDocScorer) reader.createDocScorer(query, null, null);
        assertBlockMatches(streamedScorer, expected, new int[]{0, 2, 3}, query);
        assertBlockMatches(streamedScorer, expected, new int[]{4, 6, 7}, query);
        assertBlockMatches(streamedScorer, expected, new int[]{8, 9}, query);
        assertFalse(streamedScorer.isEagerFallbackActive(),
            "Ascending streaming must stay zero-heap for query: " + query);

        // Duplicate docIds and out-of-range docIds
        LuceneDocScorer dupScorer = (LuceneDocScorer) reader.createDocScorer(query, null, null);
        assertBlockMatches(dupScorer, expected, new int[]{-5, -1, 0, 0, 1, 6, 6, 7, 7, 10, 999}, query);

        // Global stats parity (using segment's own stats doubled to simulate a 2-segment corpus)
        TextScoreStats stats = reader.getTextScoreStats(query, null);
        TextScoreStats doubledStats = reader.getTextScoreStats(query, null).merge(stats);
        Int2FloatMap expectedGlobal = reader.getDocScores(query, null, doubledStats);
        TextIndexReader.DocScorer globalScorer = reader.createDocScorer(query, null, doubledStats);
        assertBlockMatches(globalScorer, expectedGlobal, new int[]{0, 1, 4, 7, 9}, query + " (global)");
      }
    }
  }

  @Test
  public void testIdentityDescendingAndBackwardsJumpFallback()
      throws IOException {
    TextIndexConfig config =
        new TextIndexConfigBuilder().withDocIdTranslatorMode(DocIdTranslatorMode.TryOptimize.name()).build();
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, _identityIndexDir, SAMPLE_DOCS.length,
        config)) {
      for (String query : List.of("usb", "\"usb charger\"")) {
        Int2FloatMap expected = reader.getDocScores(query, null);

        // Block 1 descending (ReverseDocIdSetOperator): reverse-scans without eager map
        LuceneDocScorer descScorer = (LuceneDocScorer) reader.createDocScorer(query, null, null);
        assertBlockMatches(descScorer, expected, new int[]{9, 7, 7, 6, 1, 0}, query);
        assertFalse(descScorer.isEagerFallbackActive(), "Descending Block 1 must stay lazy: " + query);

        // Block 2 descending switches to eager fallback and matches exact scores
        assertBlockMatches(descScorer, expected, new int[]{8, 5, 3, 2}, query);
        assertTrue(descScorer.isEagerFallbackActive(), "Descending Block 2 must switch to eager: " + query);

        // Ascending Block 1 followed by backwards jump on Block 2 switches to eager fallback
        LuceneDocScorer jumpBackScorer = (LuceneDocScorer) reader.createDocScorer(query, null, null);
        assertBlockMatches(jumpBackScorer, expected, new int[]{5, 7, 9}, query);
        assertFalse(jumpBackScorer.isEagerFallbackActive());
        assertBlockMatches(jumpBackScorer, expected, new int[]{0, 1, 2}, query);
        assertTrue(jumpBackScorer.isEagerFallbackActive());
      }
    }
  }

  @Test
  public void testIdentityMultiBlockDisjunctionStreamsWithoutEagerMap()
      throws IOException {
    TextIndexConfig config =
        new TextIndexConfigBuilder().withDocIdTranslatorMode(DocIdTranslatorMode.TryOptimize.name()).build();
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, _identityIndexDir, SAMPLE_DOCS.length,
        config)) {
      // "usb OR cable" matches 8/10 docs (80%): streaming stays zero-heap and faster than materializing a map
      String query = "usb OR cable";
      Int2FloatMap expected = reader.getDocScores(query, null);
      LuceneDocScorer scorer = (LuceneDocScorer) reader.createDocScorer(query, null, null);
      assertBlockMatches(scorer, expected, new int[]{0, 1, 2, 3}, query);
      assertBlockMatches(scorer, expected, new int[]{4, 5, 6, 7, 8, 9}, query);
      assertFalse(scorer.isEagerFallbackActive(), "Ascending multi-block OR on identity must stream without map");
    }
  }

  @Test
  public void testIdentityMultiLeafWithEmptyLeafAndDeletedDocs()
      throws Exception {
    try (Directory directory = new ByteBuffersDirectory()) {
      IndexWriterConfig writerConfig =
          new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter writer = new IndexWriter(directory, writerConfig)) {
        // Leaf 0: docs 0..2 (has matches, doc 1 will be deleted)
        addDoc(writer, 0, "usb charger");
        addDoc(writer, 1, "usb charger deleted");
        addDoc(writer, 2, "wireless mouse");
        writer.commit();

        // Leaf 1: docs 3..4 (zero matches for "usb")
        addDoc(writer, 3, "hdmi monitor cable");
        addDoc(writer, 4, "keyboard mechanical");
        writer.commit();

        // Leaf 2: docs 5..7 (has phrase and term matches)
        addDoc(writer, 5, "charger usb reversed");
        addDoc(writer, 6, "usb charger compact");
        addDoc(writer, 7, "usb usb hub");
        writer.commit();

        // Delete doc 1 so Leaf 0 has liveDocs != null
        writer.deleteDocuments(new Term(COLUMN, "deleted"));
        writer.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        assertEquals(reader.leaves().size(), 3, "Expected 3 leaves");
        IndexSearcher searcher = new IndexSearcher(reader);
        QueryParser parser = new QueryParser(COLUMN, new StandardAnalyzer());
        for (String queryStr : List.of("usb", "\"usb charger\"", "usb OR cable", "nonexistentterm")) {
          Query query = parser.parse(queryStr);
          Int2FloatMap expected =
              new LuceneDocScoreCollector(NoOpDocIdTranslator.INSTANCE::getPinotDocId).collectScores(searcher, query);

          // Doc 1 is deleted, so it must score 0.0
          assertEquals(expected.get(1), 0f);

          LuceneDocScorer scorer =
              new LuceneDocScorer(searcher, query, NoOpDocIdTranslator.INSTANCE, COLUMN, queryStr);
          // Skip directly across leaves (0 -> 1 -> 2), including deleted doc 1 and non-matching leaf 1
          assertBlockMatches(scorer, expected, new int[]{0, 1, 2, 3, 4, 5, 6, 7}, queryStr);

          // Jump directly to Leaf 2 (skipping Leaves 0 and 1 completely)
          LuceneDocScorer jumpScorer =
              new LuceneDocScorer(searcher, query, NoOpDocIdTranslator.INSTANCE, COLUMN, queryStr);
          assertBlockMatches(jumpScorer, expected, new int[]{6, 7}, queryStr);
        }
      }
    }
  }

  @Test
  public void testNonIdentityDefaultDocIdTranslatorAllAdaptiveBranches()
      throws Exception {
    int numDocs = 5000;
    // Bijection: gcd(37, 5000) == 1, scattering Pinot docIds across Lucene docIds
    try (Directory directory = new ByteBuffersDirectory();
        PinotDataBuffer mappingBuffer =
            PinotDataBuffer.allocateDirect((long) numDocs * Integer.BYTES, ByteOrder.LITTLE_ENDIAN,
                "test-non-identity-mapping")) {
      IndexWriterConfig writerConfig = new IndexWriterConfig(new StandardAnalyzer());
      try (IndexWriter writer = new IndexWriter(directory, writerConfig)) {
        for (int luceneDoc = 0; luceneDoc < numDocs; luceneDoc++) {
          int pinotDoc = (luceneDoc * 37 + 13) % numDocs;
          mappingBuffer.putInt((long) luceneDoc * Integer.BYTES, pinotDoc);

          StringBuilder text = new StringBuilder();
          if (luceneDoc < 500) {
            text.append("rareterm ");
          }
          if (luceneDoc < 1150) {
            text.append("mediumterm ");
          }
          if (luceneDoc < 2600) {
            text.append("broadterm ");
            if (luceneDoc % 3 == 0) {
              text.append("phraseword ");
            } else {
              text.append("filler phraseword ");
            }
          }
          if (text.length() == 0) {
            text.append("other");
          }
          addDoc(writer, pinotDoc, text.toString());
        }
        writer.commit();
      }

      DefaultDocIdTranslator translator = new DefaultDocIdTranslator(mappingBuffer);
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        QueryParser parser = new QueryParser(COLUMN, new StandardAnalyzer());

        int[] selectiveBlock1 = {0, 1, 7, 13, 42, 100, 500, 999, 1500, 2499, 3000, 4999};
        int[] selectiveBlock2 = {2, 14, 55, 1234, 4321};
        int[] unsortedDupAndOutOfRange = {-1, 4999, 13, 13, 5000, 7, 42, 42};

        // 1. Small matches (500 <= 1024): eager map fallback immediately on Block 1
        Query rareQuery = parser.parse("rareterm");
        Int2FloatMap rareExpected =
            new LuceneDocScoreCollector(translator::getPinotDocId).collectScores(searcher, rareQuery);
        LuceneDocScorer rareScorer = new LuceneDocScorer(searcher, rareQuery, translator, COLUMN, "rareterm");
        assertBlockMatches(rareScorer, rareExpected, selectiveBlock1, "rareterm");
        assertTrue(rareScorer.isEagerFallbackActive(), "Small matches (<= 1024) should use eager fallback");

        // 2. Medium selective (1024 < 1150 <= 5000/4 = 1250): filtered collector on Block 1, eager on Block 2
        Query mediumQuery = parser.parse("mediumterm");
        Int2FloatMap mediumExpected =
            new LuceneDocScoreCollector(translator::getPinotDocId).collectScores(searcher, mediumQuery);
        LuceneDocScorer mediumScorer = new LuceneDocScorer(searcher, mediumQuery, translator, COLUMN, "mediumterm");
        assertBlockMatches(mediumScorer, mediumExpected, selectiveBlock1, "mediumterm block 1");
        assertFalse(mediumScorer.isEagerFallbackActive(),
            "Medium selective Block 1 should use filtered collector without eager map");
        assertBlockMatches(mediumScorer, mediumExpected, selectiveBlock2, "mediumterm block 2");
        assertTrue(mediumScorer.isEagerFallbackActive(), "Block 2 on non-identity segment should switch to eager");

        // 3. Broad selective (2600 > 5000/4 = 1250): reverse translator scan + Scorer.advance() on Block 1
        for (String broadQueryStr : List.of("broadterm", "\"broadterm phraseword\"")) {
          Query broadQuery = parser.parse(broadQueryStr);
          Int2FloatMap broadExpected =
              new LuceneDocScoreCollector(translator::getPinotDocId).collectScores(searcher, broadQuery);

          LuceneDocScorer broadScorer = new LuceneDocScorer(searcher, broadQuery, translator, COLUMN, broadQueryStr);
          assertBlockMatches(broadScorer, broadExpected, selectiveBlock1, broadQueryStr + " block 1");
          assertFalse(broadScorer.isEagerFallbackActive(),
              "Broad selective Block 1 should use reverse translator scan + Scorer.advance(): " + broadQueryStr);
          assertBlockMatches(broadScorer, broadExpected, selectiveBlock2, broadQueryStr + " block 2");
          assertTrue(broadScorer.isEagerFallbackActive(),
              "Block 2 on non-identity segment should switch to eager: " + broadQueryStr);

          // Unsorted + duplicate + out-of-range targets on reverse translator scan path
          LuceneDocScorer dupBroadScorer =
              new LuceneDocScorer(searcher, broadQuery, translator, COLUMN, broadQueryStr);
          assertBlockMatches(dupBroadScorer, broadExpected, unsortedDupAndOutOfRange,
              broadQueryStr + " unsorted/dup/oob");
          assertFalse(dupBroadScorer.isEagerFallbackActive());
        }
      }
    }
  }

  @Test
  public void testMultiColumnImmutableCreateDocScorer()
      throws IOException {
    for (File indexDir : List.of(_multiColIdentityDir, _multiColDefaultDir)) {
      DocIdTranslatorMode mode =
          indexDir == _multiColIdentityDir ? DocIdTranslatorMode.TryOptimize : DocIdTranslatorMode.Default;
      Map<String, String> sharedProps = Map.of(FieldConfig.TEXT_INDEX_LUCENE_DOC_ID_TRANSLATOR_MODE, mode.name());
      try (MultiColumnLuceneTextIndexReader reader = new MultiColumnLuceneTextIndexReader(List.of(COLUMN, BODY_COLUMN),
          indexDir, SAMPLE_DOCS.length, sharedProps, Map.of())) {
        for (String col : List.of(COLUMN, BODY_COLUMN)) {
          for (String query : List.of("usb", "\"usb charger\"", "usb OR cable")) {
            Int2FloatMap expected = reader.getDocScores(col, query, (String) null);
            TextIndexReader.DocScorer scorer = reader.createDocScorer(col, query, null, null);
            assertBlockMatches(scorer, expected, new int[]{0, 2, 5, 7, 9}, col + " " + query);

            TextScoreStats stats = reader.getTextScoreStats(col, query, null);
            Int2FloatMap expectedGlobal = reader.getDocScores(col, query, null, stats);
            TextIndexReader.DocScorer globalScorer = reader.createDocScorer(col, query, null, stats);
            assertBlockMatches(globalScorer, expectedGlobal, new int[]{9, 7, 4, 1, 0}, col + " " + query + " global");
          }
        }
      }
    }
  }

  @Test
  public void testCreateDocScorerValidationMatchesGetDocScores()
      throws IOException {
    // Oversized index check
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, _identityIndexDir, SAMPLE_DOCS.length - 2,
        Map.of())) {
      RuntimeException e = expectThrows(RuntimeException.class, () -> reader.createDocScorer("usb", null, null));
      assertTrue(e.getCause() instanceof IllegalStateException);
      assertTrue(e.getCause().getMessage().contains("exceeding segment document count"));
    }

    // Wrong field reference check
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, _identityIndexDir, SAMPLE_DOCS.length,
        Map.of())) {
      QueryException e = expectThrows(QueryException.class, () -> reader.createDocScorer("other:usb", null, null));
      assertTrue(e.getMessage().contains("cannot reference field other"));
    }
  }

  private static void addDoc(IndexWriter writer, int pinotDocId, String text)
      throws IOException {
    Document doc = new Document();
    doc.add(new TextField(COLUMN, text, Field.Store.NO));
    doc.add(new StoredField(LuceneTextIndexCreator.PINOT_DOC_ID_STORED_FIELD_NAME, pinotDocId));
    writer.addDocument(doc);
  }

  private static void assertBlockMatches(TextIndexReader.DocScorer scorer, Int2FloatMap expected, int[] docIds,
      String context) {
    // Pre-fill with a sentinel to verify non-matching docs are explicitly overwritten with 0.0
    double[] actual = new double[docIds.length + 2];
    Arrays.fill(actual, 999.0);
    scorer.scoreDocs(docIds, docIds.length, actual);
    for (int i = 0; i < docIds.length; i++) {
      double expectedScore = expected.getOrDefault(docIds[i], 0f);
      assertEquals(actual[i], expectedScore, 0.0, context + " docId=" + docIds[i] + " at index " + i);
    }
    // Entries beyond length must remain untouched
    assertEquals(actual[docIds.length], 999.0, context + " sentinel overwritten");
  }
}
