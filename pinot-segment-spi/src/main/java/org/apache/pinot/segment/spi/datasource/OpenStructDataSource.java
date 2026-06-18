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
import org.apache.pinot.spi.data.ComplexFieldSpec;


/// DataSource for an OPEN_STRUCT column. Provides per-key DataSources that can be used for
/// filtering, aggregation, and projection on individual keys. Distinct from `MapDataSource`,
/// which carries fixed-typed MAP semantics (single value type per column).
public interface OpenStructDataSource extends DataSource {

  /// Returns the OPEN_STRUCT ComplexFieldSpec.
  ComplexFieldSpec getFieldSpec();

  /// Returns the DataSource for the given key's values. The DataSource's value type is the
  /// per-key declared type (from `childFieldSpecs`) when present, otherwise auto-derived.
  DataSource getDataSource(String key);

  /// Returns whether the given key has a materialized per-key index in this segment. Exact,
  /// O(1) lookup into the materialized key set.
  ///
  /// Query operators use this to choose between the fast path (per-key inverted/dictionary
  /// index) and the fallback (expression scan over the sparse blob).
  ///
  /// A `false` return is only a definitive "absent" when [#isFullyMaterialized()] is also
  /// `true`; otherwise the key may still exist in the sparse blob.
  boolean isMaterialized(String key);

  /// Returns whether every key in this segment is materialized — i.e., there is no sparse
  /// blob and the materialized key set is exhaustive.
  ///
  /// When `true`, a `false` return from [#isMaterialized(String)] is a definitive "absent"
  /// and callers can short-circuit (e.g. a filter operator returns `EmptyFilterOperator`
  /// for value predicates and `MatchAllFilterOperator` for IS_NULL).
  boolean isFullyMaterialized();

  /// Returns DataSources for all keys present in this segment.
  Map<String, DataSource> getDataSources();

  /// Returns the DataSourceMetadata for the given key's values.
  DataSourceMetadata getDataSourceMetadata(String key);

  /// Returns the ColumnIndexContainer for the given key's values.
  ColumnIndexContainer getIndexContainer(String key);

  /// Reconstructs the full OPEN_STRUCT value for {@code docId} as a {@code Map<String, Object>}, or
  /// {@code null} when no key is present at that doc. Used by the realtime seal path to re-feed the
  /// OPEN_STRUCT column into the immutable segment build.
  @Nullable
  default Map<String, Object> getMapValue(int docId) {
    throw new UnsupportedOperationException(
        "Per-doc OPEN_STRUCT map reconstruction is not supported by this implementation");
  }
}
