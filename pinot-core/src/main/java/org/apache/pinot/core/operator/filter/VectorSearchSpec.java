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
package org.apache.pinot.core.operator.filter;

import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/// Immutable construction-time context for the vector similarity filter operators
/// ([VectorSimilarityFilterOperator] and [ExactVectorScanFilterOperator]).
///
/// Groups the per-query vector search parameters, the column's vector index configuration, whether the
/// predicate is combined with metadata filters, and the required doc-ids filter (the upsert doc-ids
/// snapshot). New construction-time parameters belong here rather than as additional positional
/// constructor arguments.
///
/// This class is thread-safe (immutable).
public final class VectorSearchSpec {

  /// Spec with all defaults: default search params, no index config, no metadata filter, no required filter.
  public static final VectorSearchSpec DEFAULT = new Builder().build();

  private final VectorSearchParams _searchParams;
  @Nullable
  private final VectorIndexConfig _vectorIndexConfig;
  private final boolean _hasMetadataFilter;
  @Nullable
  private final ImmutableRoaringBitmap _requiredDocIds;

  private VectorSearchSpec(VectorSearchParams searchParams, @Nullable VectorIndexConfig vectorIndexConfig,
      boolean hasMetadataFilter, @Nullable ImmutableRoaringBitmap requiredDocIds) {
    _searchParams = searchParams;
    _vectorIndexConfig = vectorIndexConfig;
    _hasMetadataFilter = hasMetadataFilter;
    _requiredDocIds = requiredDocIds;
  }

  public VectorSearchParams getSearchParams() {
    return _searchParams;
  }

  @Nullable
  public VectorIndexConfig getVectorIndexConfig() {
    return _vectorIndexConfig;
  }

  public boolean hasMetadataFilter() {
    return _hasMetadataFilter;
  }

  /// Returns the mandatory candidate filter (upsert doc-ids snapshot), or null when none applies.
  @Nullable
  public ImmutableRoaringBitmap getRequiredDocIds() {
    return _requiredDocIds;
  }

  public static final class Builder {
    private VectorSearchParams _searchParams = VectorSearchParams.DEFAULT;
    @Nullable
    private VectorIndexConfig _vectorIndexConfig;
    private boolean _hasMetadataFilter;
    @Nullable
    private ImmutableRoaringBitmap _requiredDocIds;

    public Builder withSearchParams(VectorSearchParams searchParams) {
      _searchParams = searchParams;
      return this;
    }

    public Builder withVectorIndexConfig(@Nullable VectorIndexConfig vectorIndexConfig) {
      _vectorIndexConfig = vectorIndexConfig;
      return this;
    }

    public Builder withMetadataFilter(boolean hasMetadataFilter) {
      _hasMetadataFilter = hasMetadataFilter;
      return this;
    }

    public Builder withRequiredDocIds(@Nullable ImmutableRoaringBitmap requiredDocIds) {
      _requiredDocIds = requiredDocIds;
      return this;
    }

    public VectorSearchSpec build() {
      return new VectorSearchSpec(_searchParams, _vectorIndexConfig, _hasMetadataFilter, _requiredDocIds);
    }
  }
}
