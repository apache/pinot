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

package org.apache.pinot.segment.local.segment.index.map;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.datasource.BaseDataSource;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.local.segment.index.datasource.NullDataSource;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.spi.data.ComplexFieldSpec;


public abstract class BaseMapDataSource extends BaseDataSource implements MapDataSource {
  protected final Map<String, DataSource> _keyDataSources;

  public BaseMapDataSource(DataSourceMetadata dataSourceMetadata, ColumnIndexContainer indexContainer) {
    super(dataSourceMetadata, indexContainer);
    _keyDataSources = new HashMap<>();
  }

  /// Get the map FieldSpec.
  public ComplexFieldSpec.MapFieldSpec getFieldSpec() {
    return ComplexFieldSpec.toMapFieldSpec((ComplexFieldSpec) getDataSourceMetadata().getFieldSpec());
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return null;
  }

  /// Returns the data source for a single key within this map column. A key absent from the map resolves to an
  /// all-null [NullDataSource] typed as the map's value field.
  public DataSource getDataSource(String key) {
    DataSource dataSource = _keyDataSources.get(key);
    if (dataSource != null) {
      return dataSource;
    }
    MapIndexReader mapIndexReader = getMapIndexReader();
    Map<IndexType, IndexReader> indexes = mapIndexReader.getIndexes(key);
    if (indexes == null) {
      return new NullDataSource(getFieldSpec().getValueFieldSpec(), getDataSourceMetadata().getNumDocs());
    }
    dataSource =
        new ImmutableDataSource(mapIndexReader.getColumnMetadata(key), new ColumnIndexContainer.FromMap(indexes));
    _keyDataSources.put(key, dataSource);
    return dataSource;
  }

  public abstract MapIndexReader getMapIndexReader();

  public Map<String, DataSource> getDataSources() {
    MapIndexReader mapIndexReader = (MapIndexReader) getForwardIndex();
    assert mapIndexReader != null;
    Map<String, DataSource> keyDataSources = new HashMap<>();
    Set<String> allKeys = mapIndexReader.getKeys();
    allKeys.forEach(key -> keyDataSources.put(key, getDataSource(key)));
    return keyDataSources;
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata(String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnIndexContainer getIndexContainer(String key) {
    throw new UnsupportedOperationException();
  }
}
