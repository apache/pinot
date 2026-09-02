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
package org.apache.pinot.segment.spi.datasource;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.creator.OpenStructColumnarSource;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.data.ComplexFieldSpec;


/// DataSource for an OPEN_STRUCT column. Provides per-key DataSources that can be used for
/// filtering, aggregation, and projection on individual keys. Distinct from `MapDataSource`,
/// which carries fixed-typed MAP semantics (single value type per column).
public interface OpenStructDataSource extends DataSource {

  /// Returns the OPEN_STRUCT ComplexFieldSpec.
  ComplexFieldSpec getFieldSpec();

  /// Returns the DataSource for the given key's values. The DataSource's value type is the
  /// per-key declared type (from `childFieldSpecs`) when present, otherwise STRING.
  ///
  /// Three possible outcomes:
  /// - **Materialized key** → the dense per-key DataSource (has dictionary / inverted index).
  /// - **Sparse key** → a virtual blob-backed DataSource that parses the sparse JSON column
  ///   per doc and coerces values to the resolved stored type. Returned when the key is inside
  ///   the sparse manifest, or when there is no manifest (pre-manifest segments treat every
  ///   unmaterialized key as potentially sparse).
  /// - **Definitively absent** → `null`. Returned when the segment is fully materialized (no
  ///   sparse column), or when the sparse manifest exists and does not list this key. Callers
  ///   should synthesize a typed all-null source (see `OpenStructNullDataSource.forAbsentKey`).
  @Nullable
  DataSource getDataSource(String key);

  /// Returns whether the given key has a materialized per-key index in this segment. Exact,
  /// O(1) lookup into the materialized key set. Virtual sparse-backed sources do not count as
  /// materialized.
  ///
  /// Query operators use this to choose between the fast path (per-key inverted/dictionary
  /// index) and the fallback (expression scan). Note the fallback does not read the sparse blob:
  /// a non-materialized key resolves to a typed all-null source, so the scan sees NULL at every
  /// document.
  ///
  /// A `false` return combined with a non-null [#getDataSource(String)] means the key exists
  /// in the sparse tier. A `false` return with a null getDataSource means the key is definitively
  /// absent.
  boolean isMaterialized(String key);

  /// Returns whether every key in this segment is materialized — i.e., there is no sparse
  /// blob and the materialized key set is exhaustive.
  ///
  /// When `true`, a `false` return from [#isMaterialized(String)] is a definitive "absent"
  /// and callers can treat the key as present-but-all-null — e.g. evaluate predicates against a
  /// typed all-null DataSource, which yields the correct answer under both null-handling modes
  /// (an absent key reads as its type default with null handling off, and as NULL with it on).
  boolean isFullyMaterialized();

  /// Returns DataSources for all keys present in this segment.
  Map<String, DataSource> getDataSources();

  /// Returns the DataSourceMetadata for the given key, or `null` when [#getDataSource(String)]
  /// returns null for this key. Includes metadata from virtual sparse-backed sources.
  @Nullable
  DataSourceMetadata getDataSourceMetadata(String key);

  /// Returns the ColumnIndexContainer for the given key, or `null` when the key has no
  /// materialized DataSource. Virtual sparse-backed sources do not expose a ColumnIndexContainer
  /// (they use BaseDataSource, not ImmutableDataSource), so this returns null for sparse keys
  /// even when [#getDataSource(String)] is non-null.
  @Nullable
  ColumnIndexContainer getIndexContainer(String key);

  /// Reconstructs the full OPEN_STRUCT value for `docId` as a `Map<String, Object>`, or
  /// `null` when no key is present at that doc. Used by the realtime seal path to re-feed the
  /// OPEN_STRUCT column into the immutable segment build.
  @Nullable
  default Map<String, Object> getMapValue(int docId) {
    throw new UnsupportedOperationException(
        "Per-doc OPEN_STRUCT map reconstruction is not supported by this implementation");
  }

  /// Returns a columnar view of this column when one is available, letting the segment build feed
  /// an [OpenStructColumnarSource] straight to the index creator instead of calling
  /// [#getMapValue(int)] per document. Returns `null` when this implementation has no columnar
  /// form to offer, in which case callers use the per-document path.
  @Nullable
  default OpenStructColumnarSource getColumnarSource() {
    return null;
  }

  /// Whether the per-key dictionary's contents correspond exactly to the values readable from
  /// the key column (absent docs folded as the default included) — i.e. dictionary-based
  /// MIN/MAX/DISTINCTCOUNT over it matches a full scan. Sealed segments build dictionaries
  /// from the folded values, so they are always exact.
  default boolean isKeyDictionaryExact(String key) {
    return true;
  }

  /// JSON index over the sparse blob column, or null when absent.
  @Nullable
  default JsonIndexReader getSparseJsonIndex() {
    return null;
  }
}
