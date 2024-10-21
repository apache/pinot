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
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseMapDataSource extends BaseDataSource implements MapDataSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseMapDataSource.class);

  protected final Map<String, DataSource> _keyDataSources;

  public BaseMapDataSource(DataSourceMetadata dataSourceMetadata, ColumnIndexContainer indexContainer) {
    super(dataSourceMetadata, indexContainer);
    _keyDataSources = new HashMap<>();
  }

  /**
   * Get the map FieldSpec.
   */
  public ComplexFieldSpec.MapFieldSpec getFieldSpec() {
    return ComplexFieldSpec.toMapFieldSpec((ComplexFieldSpec) getDataSourceMetadata().getFieldSpec());
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return null;
  }

  /**
   * Get the Data Source representation of a single key within this map column.
   *
   * @param key to get the DataSource for
   * @return DataSource for the key
   */
  public DataSource getKeyDataSource(String key) {
    if (_keyDataSources.containsKey(key)) {
      return _keyDataSources.get(key);
    }
    Map<IndexType, IndexReader> indexes = getMapIndexReader().getKeyIndexes(key);

    if (indexes == null) {
      // The key does not exist in the map
      return new NullDataSource(key);
    }

    try (ColumnIndexContainer indexContainer = new ColumnIndexContainer.FromMap(indexes)) {
      ColumnMetadata keyMeta = getMapIndexReader().getKeyMetadata(key);
      ImmutableDataSource dataSource = new ImmutableDataSource(keyMeta, indexContainer);
      _keyDataSources.put(key, dataSource);
      return dataSource;
    } catch (Exception ex) {
      LOGGER.error("Caught exception while creating key data source for key: {}", key, ex);
    }
    return null;
  }

  public abstract MapIndexReader getMapIndexReader();

  public Map<String, DataSource> getKeyDataSources() {
    MapIndexReader mapIndexReader = (MapIndexReader) getForwardIndex();
    assert mapIndexReader != null;
    Map<String, DataSource> keyDataSources = new HashMap<>();
    Set<String> allKeys = mapIndexReader.getKeys();
    allKeys.forEach(key -> keyDataSources.put(key, getKeyDataSource(key)));
    return keyDataSources;
  }

  @Override
  public DataSourceMetadata getKeyDataSourceMetadata(String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnIndexContainer getKeyIndexContainer(String key) {
    throw new UnsupportedOperationException();
  }
}
