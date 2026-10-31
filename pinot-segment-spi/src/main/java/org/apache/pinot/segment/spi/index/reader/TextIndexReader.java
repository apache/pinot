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
package org.apache.pinot.segment.spi.index.reader;

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public interface TextIndexReader extends IndexReader {
  /// Returns the matching dictionary ids for the given search query (optional).
  ImmutableRoaringBitmap getDictIds(String searchQuery);

  /// Returns the matching document ids for the given search query.
  /// This is the legacy entry point retained for backward compatibility.
  MutableRoaringBitmap getDocIds(String searchQuery);

  /// Returns the matching document ids for the given search query with options string.
  /// This method allows passing options as a string parameter that will be parsed internally.
  /// Lucene-based text index readers should implement this method.
  /// @param searchQuery The search query string
  /// @param optionsString Options string in format "key1=value1,key2=value2", can be null
  /// @return Matching document ids
  default MutableRoaringBitmap getDocIds(String searchQuery, @Nullable String optionsString) {
    // Default implementation falls back to the legacy method for backward compatibility.
    return getDocIds(searchQuery);
  }

  /// Returns the matching document ids, reusing a query-scoped parse when the implementation supports it.
  ///
  /// Parsing a search string depends only on the string and the column's index configuration, so every
  /// segment of one request would otherwise repeat identical work. Implementations that parse should
  /// derive their cache key from the search string, the options and the configuration that affects
  /// parsing. A `null` cache means "do not share", and must behave exactly like the two-argument form.
  ///
  /// Only the *parsed query* is shared. Doc ids are per-segment and must never be cached across readers.
  default MutableRoaringBitmap getDocIds(String searchQuery, @Nullable String optionsString,
      @Nullable QueryScopedCache queryScopedCache) {
    return getDocIds(searchQuery, optionsString);
  }

  /// Returns the number of documents matching the given search query, without materializing the matching
  /// document ids.
  ///
  /// A `count(*)` with a text-match filter only needs the size of the match set, not its contents. Building a
  /// full bitmap of matching doc ids first is wasted work that grows with the number of matches, which on a
  /// high-frequency term over a large segment is most of the query cost. Implementations backed by an inverted
  /// index can usually read the count from index metadata instead.
  ///
  /// The default implementation falls back to materializing the doc ids, so an implementation that cannot do
  /// better keeps its current behaviour.
  ///
  /// @param searchQuery The search query string
  /// @param optionsString Options string in format "key1=value1,key2=value2", can be null
  /// @return the number of matching documents
  default int getNumMatchingDocs(String searchQuery, @Nullable String optionsString) {
    return getDocIds(searchQuery, optionsString).getCardinality();
  }

  /// Counting counterpart of [#getDocIds(String, String, QueryScopedCache)].
  default int getNumMatchingDocs(String searchQuery, @Nullable String optionsString,
      @Nullable QueryScopedCache queryScopedCache) {
    return getNumMatchingDocs(searchQuery, optionsString);
  }

  /// Returns a relevance score for every document matching the given search query, keyed by Pinot doc id.
  ///
  /// Lucene-based readers score with the searcher's similarity (BM25 with `k1 = 1.2`, `b = 0.75` by default).
  /// Documents that do not match are absent from the returned map. Scores are computed from the statistics of
  /// this index only (document frequency, average field length), so they are comparable within one segment but
  /// not across segments.
  ///
  /// The query is parsed exactly like [#getDocIds(String, String)], so a document has a score if and only if
  /// it would be returned by the same `TEXT_MATCH`.
  ///
  /// @param searchQuery The search query string
  /// @param optionsString Options string in format "key1=value1,key2=value2", can be null
  /// @return map from Pinot doc id to score, containing only matching documents
  default Int2FloatMap getDocScores(String searchQuery, @Nullable String optionsString) {
    throw new UnsupportedOperationException(
        "Relevance scoring is not supported by text index reader: " + getClass().getSimpleName());
  }

  /// Like [#getDocScores(String, String)], but when `globalStats` is given the similarity uses those corpus
  /// statistics (document count, average field length, per-term document frequency) instead of this index's own,
  /// so that scores are comparable across segments. Terms or fields the statistics do not cover fall back to this
  /// index's statistics. `globalStats` normally comes from merging [#getTextScoreStats] over every segment.
  ///
  /// @param searchQuery The search query string
  /// @param optionsString Options string in format "key1=value1,key2=value2", can be null
  /// @param globalStats corpus statistics to score with, or `null` to use this index's statistics
  /// @return map from Pinot doc id to score, containing only matching documents
  default Int2FloatMap getDocScores(String searchQuery, @Nullable String optionsString,
      @Nullable TextScoreStats globalStats) {
    if (globalStats == null) {
      return getDocScores(searchQuery, optionsString);
    }
    throw new UnsupportedOperationException(
        "Relevance scoring with global statistics is not supported by text index reader: "
            + getClass().getSimpleName());
  }

  /// Returns this index's statistics for the terms of `searchQuery`, parsed and analyzed exactly as
  /// [#getDocScores(String, String)] parses it: per queried field the collection statistics, and per query term its
  /// document frequency and total term frequency. Merging the result of every segment gives the statistics of
  /// the whole corpus, to pass to [#getDocScores(String, String, TextScoreStats)].
  ///
  /// @param searchQuery The search query string
  /// @param optionsString Options string in format "key1=value1,key2=value2", can be null
  default TextScoreStats getTextScoreStats(String searchQuery, @Nullable String optionsString) {
    throw new UnsupportedOperationException(
        "Text score statistics are not supported by text index reader: " + getClass().getSimpleName());
  }

  /// Marker method that allows to differentiate between single-column and multi-column text index reader .
  default boolean isMultiColumn() {
    return false;
  }

  /// Returns the number of documents that are searchable in the index. For near-realtime indexes
  /// (e.g. Lucene on consuming segments), this may be less than the total segment doc count because
  /// recently ingested documents may not yet be visible to the index searcher. For offline/completed
  /// segments, this returns -1 (meaning all docs are searchable).
  ///
  /// This is used to implement the "searchable doc fence" for NOT filters: when inverting a
  /// text match result, only docs within \[0, searchableDocCount) are considered, preventing
  /// unindexed documents from appearing as false positives.
  ///
  /// @return the number of searchable documents, or -1 if all documents are searchable
  default int getSearchableDocCount() {
    return -1;
  }
}
