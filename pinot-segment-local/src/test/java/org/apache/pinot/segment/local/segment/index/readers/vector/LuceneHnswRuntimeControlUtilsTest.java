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
package org.apache.pinot.segment.local.segment.index.readers.vector;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Exercises Lucene's filtered ANN and exact fallback with independent readers for each test.
public class LuceneHnswRuntimeControlUtilsTest {
  private static final String VECTOR_FIELD = "vector";
  private static final String SCOPE_FIELD = "scope";

  @Test
  public void testCompletedFilteredAnnDoesNotFallBackToExact()
      throws IOException {
    int topK = 10;
    int leafSize = 1024;
    AtomicInteger exactScorerCount = new AtomicInteger();
    try (Directory directory = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter writer = new IndexWriter(directory, config)) {
        for (int leaf = 0; leaf < 2; leaf++) {
          for (int docId = 0; docId < leafSize; docId++) {
            Document document = new Document();
            // Tied scores also exercise Lucene's second, seeded ANN pass.
            document.add(new KnnFloatVectorField(VECTOR_FIELD, new float[]{1}));
            document.add(new StringField(SCOPE_FIELD, docId == 0 ? "narrow" : "broad", Store.NO));
            writer.addDocument(document);
          }
          writer.commit();
        }
      }
      try (DirectoryReader reader = new ExactScoringTrackingReader(DirectoryReader.open(directory), exactScorerCount)) {
        assertEquals(reader.leaves().size(), 2);
        IndexSearcher searcher = new IndexSearcher(reader);
        TermQuery broadFilter = new TermQuery(new Term(SCOPE_FIELD, "broad"));
        TopDocs broad = searcher.search(LuceneHnswRuntimeControlUtils.createQuery(VECTOR_FIELD, new float[]{0},
            topK, 0, true, true, broadFilter), topK);
        assertEquals(broad.scoreDocs.length, topK);
        assertEquals(exactScorerCount.get(), 0, "Completed ANN should satisfy Lucene's per-leaf candidate quota");
        for (var hit : broad.scoreDocs) {
          assertTrue(hit.doc % leafSize != 0);
        }

        // A filter with only one match in each leaf should still take Lucene's exact path.
        TopDocs narrow = searcher.search(LuceneHnswRuntimeControlUtils.createQuery(VECTOR_FIELD, new float[]{0},
            topK, 0, true, true, new TermQuery(new Term(SCOPE_FIELD, "narrow"))), topK);
        assertEquals(narrow.scoreDocs.length, 2);
        assertTrue(exactScorerCount.get() > 0);

        exactScorerCount.set(0);
        TopDocs budgeted = searcher.search(LuceneHnswRuntimeControlUtils.createQuery(VECTOR_FIELD, new float[]{0},
            topK, 1, true, true, broadFilter), topK);
        assertEquals(budgeted.scoreDocs.length, topK);
        assertEquals(exactScorerCount.get(), 2, "Explicit visit limits should avoid a redundant seeded pass");
      }
    }
  }

  private static final class ExactScoringTrackingReader extends FilterDirectoryReader {
    private final AtomicInteger _exactScorerCount;

    ExactScoringTrackingReader(DirectoryReader reader, AtomicInteger exactScorerCount)
        throws IOException {
      super(reader, new SubReaderWrapper() {
        @Override
        public LeafReader wrap(LeafReader leafReader) {
          return new FilterLeafReader(leafReader) {
            @Override
            public FloatVectorValues getFloatVectorValues(String field)
                throws IOException {
              // A seeded ANN pass also opens a scorer, but only exact search uses the bulk scorer here.
              FloatVectorValues values = spy(super.getFloatVectorValues(field));
              doAnswer(invocation -> {
                VectorScorer scorer = spy((VectorScorer) invocation.callRealMethod());
                doAnswer(bulk -> {
                  exactScorerCount.incrementAndGet();
                  return bulk.callRealMethod();
                }).when(scorer).bulk(any(DocIdSetIterator.class));
                return scorer;
              }).when(values).scorer(any(float[].class));
              return values;
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
              return null;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
              return null;
            }
          };
        }
      });
      _exactScorerCount = exactScorerCount;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader reader)
        throws IOException {
      return new ExactScoringTrackingReader(reader, _exactScorerCount);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }
}
