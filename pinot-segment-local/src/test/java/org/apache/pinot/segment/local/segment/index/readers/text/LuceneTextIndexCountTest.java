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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.local.utils.LuceneTextIndexUtils;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Covers the `count(*)` short-circuit: [LuceneTextIndexReader#getNumMatchingDocs] and the
/// [LuceneTextIndexUtils#countWithoutMaterializing] it delegates to.
///
/// Not thread-safe: the fixtures write to per-instance temp directories and are shared across the test
/// methods of one instance, which TestNG runs single-threaded by default.
public class LuceneTextIndexCountTest {
  // Unique per run: a fixed path under the shared temp dir races other executors/surefire forks, whose
  // @BeforeClass cleanup would delete a live mmapped index out from under this one.
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), LuceneTextIndexCountTest.class.getSimpleName() + "-" + UUID.randomUUID());
  private static final File INDEX_DIR = new File(TEMP_DIR, "single-leaf");
  private static final File MULTI_LEAF_DIR = new File(TEMP_DIR, "multi-leaf");
  private static final String COLUMN = "body";

  /// Every document carries `sentinelall` so there is a term matching all documents. `to` cannot serve that
  /// role: it is in the default English stop-word set and matches nothing.
  private static final String[] DOCS = {
      "failed to place order for user alice sentinelall",
      "failed to charge card for user bob sentinelall",
      "connection refused by payment service sentinelall",
      "connection established to cache service sentinelall",
      "order confirmation email sent to carol sentinelall",
      "cache miss while loading order details sentinelall",
      "payment accepted for order 4815 sentinelall",
      "unrelated housekeeping log line sentinelall"
  };

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
    TextIndexConfig config = new TextIndexConfigBuilder().build();
    try (LuceneTextIndexCreator creator = new LuceneTextIndexCreator(COLUMN, INDEX_DIR, true, false, null, null,
        config)) {
      for (String doc : DOCS) {
        creator.add(doc);
      }
      creator.seal();
    }

    // LuceneTextIndexCreator.seal() force-merges to one segment, so the fixture above can never exercise
    // cross-leaf accumulation. Build a second index by hand, committing between batches and never merging,
    // so leaves() > 1.
    FileUtils.forceMkdir(MULTI_LEAF_DIR);
    try (Directory directory = FSDirectory.open(MULTI_LEAF_DIR.toPath());
        IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {
      for (String doc : DOCS) {
        Document document = new Document();
        document.add(new TextField(COLUMN, doc, Field.Store.NO));
        writer.addDocument(document);
        writer.commit();
      }
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @DataProvider(name = "queries")
  public Object[][] queries() {
    return new Object[][]{
        {"order", null},
        {"connection", null},
        {"housekeeping", null},
        {"nonexistentterm", null},
        {"sentinelall", null},
        {"to", null},
        {"failed AND order", null},
        {"failed OR connection", null},
        {"order AND NOT failed", null},
        {"\"place order\"", null},
        {"\"failed to place order\"", null},
        {"conn*", null},
        {"c?che", null},
        {"connection~1", null},
        {"/conn.*/", null},
        {"failed order connection", "parser=MATCH,minimumShouldMatch=2"},
        {"*tion", "allowLeadingWildcard=true"}
    };
  }

  /// The count must equal what materializing the doc ids would produce, for every query shape -- both the
  /// shapes Lucene counts from metadata and the ones it has to iterate.
  @Test(dataProvider = "queries")
  public void testCountMatchesMaterializedDocIds(String query, String options)
      throws IOException {
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, INDEX_DIR, DOCS.length, Map.of())) {
      int expected = reader.getDocIds(query, options).getCardinality();
      assertEquals(reader.getNumMatchingDocs(query, options), expected,
          "count disagreed with materialized doc ids for query: " + query);
    }
  }

  @Test
  public void testCountIsExactForKnownQueries()
      throws IOException {
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, INDEX_DIR, DOCS.length, Map.of())) {
      assertEquals(reader.getNumMatchingDocs("order", null), 4);
      assertEquals(reader.getNumMatchingDocs("connection", null), 2);
      assertEquals(reader.getNumMatchingDocs("sentinelall", null), DOCS.length);
      assertEquals(reader.getNumMatchingDocs("nonexistentterm", null), 0);
    }
  }

  /// Pins the precondition the optimization rests on: for a single term Lucene answers from index metadata,
  /// so no document is visited. If this stops holding, counting silently degrades to full iteration and the
  /// feature becomes pure overhead while every other test still passes.
  @Test
  public void testSingleTermIsCountedFromMetadata()
      throws IOException {
    // Uses the hand-built index: LuceneTextIndexCreator nests its Lucene index under a versioned
    // subdirectory, which is a convention this assertion should not depend on.
    try (Directory directory = FSDirectory.open(MULTI_LEAF_DIR.toPath());
        DirectoryReader indexReader = DirectoryReader.open(directory)) {
      IndexSearcher searcher = new IndexSearcher(indexReader);
      Query query = new ConstantScoreQuery(new TermQuery(new Term(COLUMN, "order")));
      Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
      for (LeafReaderContext leaf : indexReader.leaves()) {
        assertTrue(weight.count(leaf) >= 0, "Lucene no longer counts a single term from metadata");
      }
    }
  }

  /// The counting path skips the doc-id translator, on the grounds that a count is invariant under a 1:1
  /// mapping. The creator fixture stores `DocID == i`, so `TryOptimize` collapses to a no-op translator and
  /// that invariant is never actually exercised. Build a translator over a genuine permutation and assert the
  /// count is unchanged by it.
  @Test
  public void testCountIsInvariantUnderDocIdPermutation()
      throws Exception {
    File permutedDir = new File(TEMP_DIR, "permuted");
    FileUtils.forceMkdir(permutedDir);
    TextIndexConfig config = new TextIndexConfigBuilder().build();
    // Reverse order: Lucene doc i maps to Pinot doc (n-1-i), so the mapping is a permutation, not identity.
    int[] luceneToPinot = new int[DOCS.length];
    try (LuceneTextIndexCreator creator = new LuceneTextIndexCreator(COLUMN, permutedDir, true, false, null, null,
        config)) {
      for (int i = 0; i < DOCS.length; i++) {
        creator.add(DOCS[DOCS.length - 1 - i]);
        luceneToPinot[i] = DOCS.length - 1 - i;
      }
      creator.seal();
    }
    try (LuceneTextIndexReader permuted = new LuceneTextIndexReader(COLUMN, permutedDir, DOCS.length, Map.of());
        LuceneTextIndexReader identity = new LuceneTextIndexReader(COLUMN, INDEX_DIR, DOCS.length, Map.of())) {
      for (Object[] row : queries()) {
        String q = (String) row[0];
        String o = (String) row[1];
        assertEquals(permuted.getNumMatchingDocs(q, o), permuted.getDocIds(q, o).getCardinality(),
            "count disagreed with doc ids under a permuted mapping for query: " + q);
        assertEquals(permuted.getNumMatchingDocs(q, o), identity.getNumMatchingDocs(q, o),
            "count changed with doc id order for query: " + q);
      }
    }
  }

  /// A Lucene index larger than the segment means a broken doc-id mapping. Counting does not consult the
  /// translator, so it must refuse the fast path and fall back to the doc-id path, which fails loudly rather
  /// than returning an inflated count.
  @Test
  public void testOversizedIndexFallsBackInsteadOfCountingWrong()
      throws Exception {
    int understatedNumDocs = DOCS.length - 4;
    try (LuceneTextIndexReader reader =
        new LuceneTextIndexReader(COLUMN, INDEX_DIR, understatedNumDocs, Map.of())) {
      try {
        int count = reader.getNumMatchingDocs("sentinelall", null);
        assertTrue(count <= understatedNumDocs,
            "returned a count larger than the segment (" + count + " > " + understatedNumDocs + ")");
      } catch (RuntimeException e) {
        // Falling back and failing loudly is the intended behaviour for a broken mapping. The fallback runs
        // getDocIds, whose translator throws, and getNumMatchingDocs rewraps it with its own wording.
        assertTrue(e.getMessage().contains("text index"), "unexpected failure: " + e.getMessage());
      }
    }
  }

  /// The iterating branch claims to honour the query timeout. Every query shape above completes, so nothing
  /// ever enters that branch and gets interrupted, leaving the claim untested.
  ///
  /// Asserts parity with the doc-id path rather than a specific mechanism: on a plain deadline expiry
  /// `checkTerminationInternal` throws but does not record a termination exception (only an accountant kill
  /// does), so both paths convert to `CollectionTerminatedException` and return a truncated result. What
  /// matters is that counting is interrupted exactly like collecting, not that it reports differently.
  @Test
  public void testIteratingBranchIsInterruptedLikeTheDocIdPath()
      throws Exception {
    String phrase = "\"place order\"";
    int fullCount;
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, INDEX_DIR, DOCS.length, Map.of())) {
      fullCount = reader.getNumMatchingDocs(phrase, null);
      assertTrue(fullCount > 0, "phrase should match without a deadline");
    }

    long expired = System.currentTimeMillis() - 60_000L;
    QueryExecutionContext expiredContext =
        new QueryExecutionContext(QueryExecutionContext.QueryType.SSE, 1L, "cid", Accounting.DEFAULT_WORKLOAD_NAME,
            expired, expired, expired, "brokerId", "instanceId", "");
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, INDEX_DIR, DOCS.length, Map.of());
        QueryThreadContext ignored =
            QueryThreadContext.open(expiredContext, ThreadAccountantUtils.getNoOpAccountant())) {
      // A phrase cannot be counted from index metadata, so this reaches the collecting branch.
      int counted = reader.getNumMatchingDocs(phrase, null);
      int collected = reader.getDocIds(phrase, null).getCardinality();
      assertTrue(counted < fullCount, "counting ignored the expired deadline: " + counted + " of " + fullCount);
      assertEquals(counted, collected, "counting and collecting disagreed under an expired deadline");
    }
  }

  /// Cross-leaf accumulation is the only non-trivial logic in the counting loop, and the force-merged fixture
  /// cannot reach it. A regression that dropped accumulation, or bailed out of the loop instead of continuing,
  /// would return a short count on any multi-leaf index.
  @Test(dataProvider = "queries")
  public void testMultiLeafCountMatchesCollectedCount(String query, String options)
      throws Exception {
    try (Directory directory = FSDirectory.open(MULTI_LEAF_DIR.toPath());
        DirectoryReader indexReader = DirectoryReader.open(directory)) {
      assertTrue(indexReader.leaves().size() > 1, "fixture is not multi-leaf");
      IndexSearcher searcher = new IndexSearcher(indexReader);
      try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, INDEX_DIR, DOCS.length, Map.of())) {
        Query parsed = reader.buildQuery(query, options);
        assertEquals(LuceneTextIndexUtils.countWithoutMaterializing(searcher, parsed), searcher.count(parsed),
            "multi-leaf count disagreed with IndexSearcher#count for query: " + query);
      }
    }
  }
}
