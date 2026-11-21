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

import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import java.io.IOException;
import java.util.function.IntUnaryOperator;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class LuceneDocScoreCollectorTest {

  @Test
  public void testCollectsScoresUntilCancelled()
      throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {
        for (int i = 0; i < 10; i++) {
          Document document = new Document();
          document.add(new TextField("title", "usb charger " + i, Field.Store.NO));
          writer.addDocument(document);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        TermQuery query = new TermQuery(new Term("title", "usb"));

        Int2FloatOpenHashMap scores = new Int2FloatOpenHashMap();
        searcher.search(query, new LuceneDocScoreCollector(scores, IntUnaryOperator.identity()));
        assertEquals(scores.size(), 10);

        // A query thread that stopped waiting cancels the search: nothing more is collected
        Int2FloatOpenHashMap cancelledScores = new Int2FloatOpenHashMap();
        LuceneDocScoreCollector cancelled = new LuceneDocScoreCollector(cancelledScores, IntUnaryOperator.identity());
        cancelled.markShouldCancel();
        searcher.search(query, cancelled);
        assertTrue(cancelledScores.isEmpty());
      }
    }
  }
}
