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
package org.apache.pinot.segment.local.segment.index.openstruct;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.datasource.BaseDataSource;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Per-key {@link DataSource} accessor for sealed (immutable) segments with an OPEN_STRUCT column.
 *
 * <p>Always columnar — there is no blob branch. Every key that was dense enough during segment
 * creation gets its own materialized {@link DataSource} (forward index + optional inverted index /
 * dictionary). Keys that did not meet the density threshold are stored in an optional sparse
 * column; the sparse {@link DataSource} is returned for any unmaterialized key lookup.
 *
 * <p>Use {@link #isMaterialized(String)} and {@link #isFullyMaterialized()} together to choose
 * the query execution path:
 * <ul>
 *   <li>Materialized key → fast path via per-key DataSource (inverted/dictionary index available).
 *   <li>Not materialized + not fully materialized → fall back to the sparse DataSource.
 *   <li>Not materialized + fully materialized → key is definitively absent; short-circuit.
 * </ul>
 *
 * <p>Thread-safety: immutable after construction; safe for concurrent reads.
 */
public class ImmutableOpenStructDataSource extends BaseDataSource implements OpenStructDataSource {
  private final ComplexFieldSpec _fieldSpec;
  private final Map<String, DataSource> _perKeyDataSources;
  @Nullable
  private final DataSource _sparseDataSource;

  public ImmutableOpenStructDataSource(ComplexFieldSpec fieldSpec, Map<String, DataSource> perKeyDataSources,
      @Nullable DataSource sparseDataSource, DataSourceMetadata dataSourceMetadata,
      ColumnIndexContainer indexContainer) {
    super(dataSourceMetadata, indexContainer);
    _fieldSpec = fieldSpec;
    _perKeyDataSources = perKeyDataSources;
    _sparseDataSource = sparseDataSource;
  }

  /**
   * Convenience constructor for segment-load time. Synthesizes a minimal {@link DataSourceMetadata}
   * for the parent OPEN_STRUCT column (which has no on-disk presence of its own) and uses an empty
   * {@link ColumnIndexContainer} — all real readers live on the per-key data sources.
   */
  public ImmutableOpenStructDataSource(ComplexFieldSpec fieldSpec, Map<String, DataSource> perKeyDataSources,
      @Nullable DataSource sparseDataSource, int numDocs) {
    this(fieldSpec, perKeyDataSources, sparseDataSource,
        new ImmutableOpenStructDataSourceMetadata(fieldSpec, numDocs),
        new ColumnIndexContainer.FromMap.Builder().build());
  }

  @Override
  public ComplexFieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  @Override
  @Nullable
  public DataSource getDataSource(String key) {
    DataSource ds = _perKeyDataSources.get(key);
    return ds != null ? ds : _sparseDataSource;
  }

  @Override
  public boolean isMaterialized(String key) {
    return _perKeyDataSources.containsKey(key);
  }

  @Override
  public boolean isFullyMaterialized() {
    return _sparseDataSource == null;
  }

  @Override
  public Map<String, DataSource> getDataSources() {
    return _perKeyDataSources;
  }

  @Override
  @Nullable
  public DataSourceMetadata getDataSourceMetadata(String key) {
    DataSource ds = _perKeyDataSources.get(key);
    return ds != null ? ds.getDataSourceMetadata() : null;
  }

  @Override
  @Nullable
  public ColumnIndexContainer getIndexContainer(String key) {
    DataSource ds = _perKeyDataSources.get(key);
    return ds instanceof ImmutableDataSource immutableDs ? immutableDs.getIndexContainer() : null;
  }

  private static class ImmutableOpenStructDataSourceMetadata implements DataSourceMetadata {
    private final FieldSpec _fieldSpec;
    private final int _numDocs;

    ImmutableOpenStructDataSourceMetadata(FieldSpec fieldSpec, int numDocs) {
      _fieldSpec = fieldSpec;
      _numDocs = numDocs;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _fieldSpec;
    }

    @Override
    public boolean isSorted() {
      return false;
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }

    @Override
    public int getNumValues() {
      return _numDocs;
    }

    @Override
    public int getMaxNumValuesPerMVEntry() {
      return 0;
    }

    @Override
    public int getCardinality() {
      return Constants.UNKNOWN_CARDINALITY;
    }

    @Nullable
    @Override
    public Comparable getMinValue() {
      return null;
    }

    @Nullable
    @Override
    public Comparable getMaxValue() {
      return null;
    }

    @Nullable
    @Override
    public PartitionFunction getPartitionFunction() {
      return null;
    }

    @Nullable
    @Override
    public java.util.Set<Integer> getPartitions() {
      return null;
    }
  }
}
