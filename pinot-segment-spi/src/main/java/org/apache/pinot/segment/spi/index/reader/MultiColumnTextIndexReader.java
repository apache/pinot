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
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/*
 * TextIndexReader which allows querying a specific column within the index.
 */
public interface MultiColumnTextIndexReader extends TextIndexReader {

  /// Returns the matching document ids for the given search query against given column.
  /// This is the legacy method for backward compatibility.
  default MutableRoaringBitmap getDocIds(String column, String searchQuery) {
    return getDocIds(searchQuery);
  }

  /// Returns the matching document ids for the given search query against given column with options string.
  /// Lucene-based multi-column text index readers should implement this method.
  /// @param column The column name to search
  /// @param searchQuery The search query string
  /// @param optionsString Options string in format "key1=value1,key2=value2", can be null
  /// @return Matching document ids
  MutableRoaringBitmap getDocIds(String column, String searchQuery, @Nullable String optionsString);

  /// Column-aware counterpart of [TextIndexReader#getNumMatchingDocs]: returns the number of documents of
  /// `column` matching the query without materializing their ids.
  ///
  /// Defaults to materializing, so an implementation that cannot count more cheaply keeps its behaviour.
  ///
  /// @param column The column to query
  /// @param searchQuery The search query string
  /// @param optionsString Options string in format "key1=value1,key2=value2", can be null
  /// @return the number of matching documents
  default int getNumMatchingDocs(String column, String searchQuery, @Nullable String optionsString) {
    return getDocIds(column, searchQuery, optionsString).getCardinality();
  }

  /// A multi-column index cannot answer without a column, matching [#getDocIds(String)].
  @Override
  default int getNumMatchingDocs(String searchQuery, @Nullable String optionsString) {
    throw new UnsupportedOperationException("Multi-column text index requires column name to count!");
  }

  /// Column-aware counterpart of [TextIndexReader#getDocScores]: returns the relevance score of every document
  /// of `column` matching the query, keyed by Pinot doc id.
  ///
  /// @param column The column to query
  /// @param searchQuery The search query string
  /// @param optionsString Options string in format "key1=value1,key2=value2", can be null
  /// @return map from Pinot doc id to score, containing only matching documents
  default Int2FloatMap getDocScores(String column, String searchQuery, @Nullable String optionsString) {
    throw new UnsupportedOperationException(
        "Relevance scoring is not supported by text index reader: " + getClass().getSimpleName());
  }

  /// Column-aware counterpart of [TextIndexReader#getDocScores(String, String, TextScoreStats)].
  default Int2FloatMap getDocScores(String column, String searchQuery, @Nullable String optionsString,
      @Nullable TextScoreStats globalStats) {
    if (globalStats == null) {
      return getDocScores(column, searchQuery, optionsString);
    }
    throw new UnsupportedOperationException(
        "Relevance scoring with global statistics is not supported by text index reader: "
            + getClass().getSimpleName());
  }

  /// Column-aware counterpart of [TextIndexReader#getTextScoreStats].
  default TextScoreStats getTextScoreStats(String column, String searchQuery, @Nullable String optionsString) {
    throw new UnsupportedOperationException(
        "Text score statistics are not supported by text index reader: " + getClass().getSimpleName());
  }

  /// A multi-column index cannot score without a column, matching [#getDocIds(String)].
  @Override
  default Int2FloatMap getDocScores(String searchQuery, @Nullable String optionsString) {
    throw new UnsupportedOperationException("Multi-column text index requires column name to score!");
  }

  @Override
  default Int2FloatMap getDocScores(String searchQuery, @Nullable String optionsString,
      @Nullable TextScoreStats globalStats) {
    throw new UnsupportedOperationException("Multi-column text index requires column name to score!");
  }

  @Override
  default TextScoreStats getTextScoreStats(String searchQuery, @Nullable String optionsString) {
    throw new UnsupportedOperationException("Multi-column text index requires column name to collect statistics!");
  }

  default boolean isMultiColumn() {
    return true;
  }
}
