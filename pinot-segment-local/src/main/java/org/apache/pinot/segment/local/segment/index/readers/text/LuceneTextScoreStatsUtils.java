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
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.pinot.segment.spi.index.reader.TextScoreStats;
import org.apache.pinot.spi.exception.QueryErrorCode;


/// Collects and applies cross-segment ("global") BM25 statistics for `TEXT_SCORE`, the Lucene side of
/// [TextScoreStats].
///
/// The collection and scoring passes accept the same query shapes. Queries whose expansions or weights depend on
/// the local index (such as fuzzy and synonym queries) cannot use global statistics.
///
/// Stateless and thread-safe.
public final class LuceneTextScoreStatsUtils {
  private LuceneTextScoreStatsUtils() {
  }

  /// Verifies the field of every clause, including prohibited clauses, before executing a scoring query.
  private static void requireColumn(Query query, String column) {
    query.visit(new QueryVisitor() {
      @Override
      public boolean acceptField(String field) {
        if (!column.equals(field)) {
          throw QueryErrorCode.QUERY_VALIDATION.asException(
              "Text scoring on column " + column + " cannot reference field " + field);
        }
        return true;
      }

      @Override
      public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
        return this;
      }

      @Override
      public void consumeTerms(Query termQuery, Term... terms) {
        for (Term term : terms) {
          acceptField(term.field());
        }
      }

      @Override
      public void consumeTermsMatching(Query termQuery, String field, Supplier<ByteRunAutomaton> automaton) {
        acceptField(field);
      }

      @Override
      public void visitLeaf(Query leaf) {
        if (leaf instanceof FieldExistsQuery exists) {
          acceptField(exists.getField());
        } else if (!(leaf instanceof MatchAllDocsQuery || leaf instanceof MatchNoDocsQuery)) {
          throw QueryErrorCode.QUERY_VALIDATION.asException(
              "Cannot verify the field of " + leaf.getClass().getSimpleName() + " for column " + column);
        }
      }
    });
  }

  /// Collects statistics for the declared column, including its documents when the segment has no matching terms.
  public static TextScoreStats collect(IndexSearcher searcher, Query query, String column)
      throws IOException {
    requireColumn(query, column);
    requireGlobalStatsSupport(query);
    Set<Term> terms = new HashSet<>();
    query.visit(QueryVisitor.termCollector(terms));
    IndexReader reader = searcher.getIndexReader();
    TextScoreStats stats = new TextScoreStats();
    TextScoreStats.FieldStats field = stats.getOrCreateField(column);
    field.addCollectionStats(reader.maxDoc(), reader.getDocCount(column), reader.getSumTotalTermFreq(column),
        reader.getSumDocFreq(column));
    for (Term term : terms) {
      field.addTermStats(term.text(), reader.docFreq(term), reader.totalTermFreq(term));
    }
    return stats;
  }

  /// Uses the original searcher for local scoring, including Lucene's native index-dependent rewrites.
  public static IndexSearcher scoringSearcher(IndexSearcher localSearcher, Query query, String column,
      @Nullable TextScoreStats globalStats) {
    requireColumn(query, column);
    if (globalStats == null || globalStats.getFields().isEmpty()) {
      return localSearcher;
    }
    requireGlobalStatsSupport(query);
    IndexSearcher searcher = new GlobalStatsIndexSearcher(localSearcher.getIndexReader(), globalStats);
    searcher.setSimilarity(localSearcher.getSimilarity());
    searcher.setQueryCache(null);
    return searcher;
  }

  private static void requireGlobalStatsSupport(Query query) {
    if (query instanceof BooleanQuery bool) {
      for (BooleanClause clause : bool) {
        requireGlobalStatsSupport(clause.getQuery());
      }
    } else if (query instanceof BoostQuery boost) {
      requireGlobalStatsSupport(boost.getQuery());
    } else if (query instanceof ConstantScoreQuery constant) {
      requireGlobalStatsSupport(constant.getQuery());
    } else if (!(query instanceof TermQuery || query instanceof PhraseQuery
        || query instanceof MatchAllDocsQuery || query instanceof MatchNoDocsQuery)) {
      // Uncapped constant-score prefix, wildcard and range queries do not use term statistics.
      if (query instanceof MultiTermQuery multi && (multi.getRewriteMethod() == MultiTermQuery.CONSTANT_SCORE_REWRITE
          || multi.getRewriteMethod() == MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE)) {
        return;
      }
      throw QueryErrorCode.QUERY_VALIDATION.asException("Global text score statistics do not support "
          + query.getClass().getSimpleName() + ". Use TEXT_SCORE without global statistics for this query");
    }
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
      if (counts == null || counts[0] < docFreq || counts[0] > fieldStats.getDocCount()
          || counts[1] < counts[0] || counts[1] < totalTermFreq || counts[1] > fieldStats.getSumTotalTermFreq()) {
        throw QueryErrorCode.QUERY_VALIDATION.asException(
            "Global text score statistics are missing or inconsistent for term " + term);
      }
      return new TermStatistics(term.bytes(), counts[0], counts[1]);
    }

    @Override
    public CollectionStatistics collectionStatistics(String field)
        throws IOException {
      TextScoreStats.FieldStats fieldStats = _globalStats.getField(field);
      CollectionStatistics local = super.collectionStatistics(field);
      if (local == null) {
        // Lucene requests collection statistics even for an unindexed field; it has no local scorer.
        return null;
      }
      if (fieldStats == null || fieldStats.getDocCount() <= 0 || fieldStats.getMaxDoc() < fieldStats.getDocCount()
          || fieldStats.getSumDocFreq() < fieldStats.getDocCount()
          || fieldStats.getSumTotalTermFreq() < fieldStats.getSumDocFreq()
          || fieldStats.getMaxDoc() < local.maxDoc() || fieldStats.getDocCount() < local.docCount()
          || fieldStats.getSumDocFreq() < local.sumDocFreq()
          || fieldStats.getSumTotalTermFreq() < local.sumTotalTermFreq()) {
        throw QueryErrorCode.QUERY_VALIDATION.asException(
            "Global text score statistics are missing or inconsistent for field " + field);
      }
      return new CollectionStatistics(field, fieldStats.getMaxDoc(), fieldStats.getDocCount(),
          fieldStats.getSumTotalTermFreq(), fieldStats.getSumDocFreq());
    }
  }
}
