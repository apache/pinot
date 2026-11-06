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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermStatistics;
import org.apache.pinot.segment.spi.index.reader.TextScoreStats;


/// Collects and applies cross-segment ("global") BM25 statistics for `TEXT_SCORE`, the Lucene side of
/// [TextScoreStats].
///
/// - [#collect] reads, from one index, the statistics scoring a query would consult: the collection statistics of
///   every field the query targets and the document / total term frequency of every query term.
/// - [#newGlobalStatsSearcher] returns an [IndexSearcher] whose [IndexSearcher#termStatistics] and
///   [IndexSearcher#collectionStatistics] answer from merged statistics instead of the local index. Lucene's
///   similarities build their per-query weights exclusively from these two methods, so a search through this
///   searcher scores every document exactly as a single index holding the whole corpus would (this is how
///   Elasticsearch implements `dfs_query_then_fetch`).
///
/// Stateless; thread-safe.
public final class LuceneTextScoreStatsUtils {
  private LuceneTextScoreStatsUtils() {
  }

  /// Returns the statistics of `query`'s terms in `searcher`'s index. The query is rewritten first, so terms are
  /// extracted from the same query shape the scorer would see. Terms absent from the index are recorded with zero
  /// frequencies, and the collection statistics of their field are recorded even when no term occurs, so that this
  /// index's documents count towards the corpus size and average field length.
  public static TextScoreStats collect(IndexSearcher searcher, Query query)
      throws IOException {
    Set<Term> terms = new HashSet<>();
    searcher.rewrite(query).visit(QueryVisitor.termCollector(terms));
    IndexReader reader = searcher.getIndexReader();
    TextScoreStats stats = new TextScoreStats();
    for (Term term : terms) {
      String field = term.field();
      TextScoreStats.FieldStats fieldStats = stats.getField(field);
      if (fieldStats == null) {
        fieldStats = stats.getOrCreateField(field);
        fieldStats.addCollectionStats(reader.maxDoc(), reader.getDocCount(field), reader.getSumTotalTermFreq(field),
            reader.getSumDocFreq(field));
      }
      fieldStats.addTermStats(term.text(), reader.docFreq(term), reader.totalTermFreq(term));
    }
    return stats;
  }

  /// Returns a searcher over `reader` that scores with `globalStats`. Terms and fields the statistics do not cover
  /// (for example terms a wildcard expands to in this segment) use the local statistics.
  public static IndexSearcher newGlobalStatsSearcher(IndexReader reader, TextScoreStats globalStats) {
    IndexSearcher searcher = new GlobalStatsIndexSearcher(reader, globalStats);
    // Scored searches are not cached by Lucene anyway; keep it explicit so this searcher never holds heap
    searcher.setQueryCache(null);
    return searcher;
  }

  /// [IndexSearcher] answering term and collection statistics from [TextScoreStats].
  private static final class GlobalStatsIndexSearcher extends IndexSearcher {
    private final TextScoreStats _globalStats;

    GlobalStatsIndexSearcher(IndexReader reader, TextScoreStats globalStats) {
      super(reader);
      _globalStats = globalStats;
    }

    @Override
    public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq)
        throws IOException {
      TextScoreStats.FieldStats fieldStats = _globalStats.getField(term.field());
      long[] counts = fieldStats != null ? fieldStats.getTermStats(term.text()) : null;
      // Lucene only asks for terms present in this index (docFreq > 0); merged counts include this index's, so they
      // are at least as large. Guard anyway so inconsistent input degrades to local scoring instead of failing.
      if (counts == null || counts[0] < docFreq || counts[1] < counts[0]) {
        return super.termStatistics(term, docFreq, totalTermFreq);
      }
      return new TermStatistics(term.bytes(), counts[0], counts[1]);
    }

    @Override
    public CollectionStatistics collectionStatistics(String field)
        throws IOException {
      TextScoreStats.FieldStats fieldStats = _globalStats.getField(field);
      if (fieldStats == null || fieldStats.getDocCount() <= 0 || fieldStats.getMaxDoc() < fieldStats.getDocCount()
          || fieldStats.getSumDocFreq() < fieldStats.getDocCount()
          || fieldStats.getSumTotalTermFreq() < fieldStats.getSumDocFreq()) {
        return super.collectionStatistics(field);
      }
      return new CollectionStatistics(field, fieldStats.getMaxDoc(), fieldStats.getDocCount(),
          fieldStats.getSumTotalTermFreq(), fieldStats.getSumDocFreq());
    }
  }
}
