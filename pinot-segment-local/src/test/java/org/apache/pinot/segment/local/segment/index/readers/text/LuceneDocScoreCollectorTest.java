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

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CancellationException;
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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


public class LuceneDocScoreCollectorTest {

  @Test
  public void testTranslatedScoresAndCancellation()
      throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter writer = new IndexWriter(directory, config)) {
        for (int i = 0; i < 10; i++) {
          Document document = new Document();
          document.add(new TextField("title", i % 2 == 0 ? "usb charger" : "cable", Field.Store.NO));
          document.add(new StoredField(LuceneTextIndexCreator.PINOT_DOC_ID_STORED_FIELD_NAME, 9 - i));
          writer.addDocument(document);
          if (i == 4) {
            writer.commit();
          }
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        TermQuery query = new TermQuery(new Term("title", "usb"));

        Int2FloatMap scores = new LuceneDocScoreCollector(docId -> 9 - docId).collectScores(searcher, query);
        assertEquals(scores.keySet(), Set.of(1, 3, 5, 7, 9));
        assertEquals(LuceneDocScoreCollector.forConsumingSegment().collectScores(searcher, query), scores);

        LuceneDocScoreCollector cancelled = LuceneDocScoreCollector.forConsumingSegment();
        cancelled.markShouldCancel();
        expectThrows(CancellationException.class, () -> cancelled.collectScores(searcher, query));

        // Filtered collection: contiguous range, sparse sorted, unsorted with duplicates, single, empty
        Int2FloatMap contiguous = new LuceneDocScoreCollector(docId -> 9 - docId,
            LuceneDocScoreCollector.allowedDocIdsPredicate(new int[]{0, 1, 2, 3}, 4)).collectScores(searcher, query);
        assertEquals(contiguous.keySet(), Set.of(1, 3));
        assertEquals(contiguous.get(1), scores.get(1));
        assertEquals(contiguous.get(3), scores.get(3));

        Int2FloatMap sparse = new LuceneDocScoreCollector(docId -> 9 - docId,
            LuceneDocScoreCollector.allowedDocIdsPredicate(new int[]{2, 5, 9}, 3)).collectScores(searcher, query);
        assertEquals(sparse.keySet(), Set.of(5, 9));
        assertEquals(sparse.get(5), scores.get(5));
        assertEquals(sparse.get(9), scores.get(9));

        Int2FloatMap unsortedWithDuplicates = LuceneDocScoreCollector.forConsumingSegment(
            LuceneDocScoreCollector.allowedDocIdsPredicate(new int[]{9, 3, 3, 2}, 4)).collectScores(searcher, query);
        assertEquals(unsortedWithDuplicates.keySet(), Set.of(3, 9));
        assertEquals(unsortedWithDuplicates.get(3), scores.get(3));
        assertEquals(unsortedWithDuplicates.get(9), scores.get(9));

        Int2FloatMap single = LuceneDocScoreCollector.forConsumingSegment(
            LuceneDocScoreCollector.allowedDocIdsPredicate(new int[]{7}, 1)).collectScores(searcher, query);
        assertEquals(single.keySet(), Set.of(7));
        assertEquals(single.get(7), scores.get(7));

        Int2FloatMap empty = LuceneDocScoreCollector.forConsumingSegment(
            LuceneDocScoreCollector.allowedDocIdsPredicate(new int[]{1, 3}, 0)).collectScores(searcher, query);
        assertEquals(empty.keySet(), Set.of());

        // copyIfShared=true must isolate non-contiguous sorted arrays from caller buffer mutation
        int[] sharedBuffer = {2, 5, 9};
        var isolatedPredicate = LuceneDocScoreCollector.allowedDocIdsPredicate(sharedBuffer, 3, true);
        sharedBuffer[1] = -1;
        sharedBuffer[2] = -1;
        Int2FloatMap isolated =
            new LuceneDocScoreCollector(docId -> 9 - docId, isolatedPredicate).collectScores(searcher, query);
        assertEquals(isolated.keySet(), Set.of(5, 9));
      }
    }
  }
}
