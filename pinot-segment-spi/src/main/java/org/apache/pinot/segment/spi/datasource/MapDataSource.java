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
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.spi.data.ComplexFieldSpec;


/// DataSource for a MAP column. Provides per-key DataSources that can be used for filtering,
/// aggregation, and projection on individual MAP keys.
///
/// **Migration note:** the `getKeyXXX` methods are deprecated in favor of shorter names without
/// the `Key` infix (e.g. `getDataSource`, `getDataSources`). New implementations should override
/// the new names and leave the deprecated ones alone. Existing implementations that override only
/// the old names will continue to work — the new-name defaults delegate to the deprecated methods.
public interface MapDataSource extends DataSource {

  /// Returns the map FieldSpec.
  ComplexFieldSpec.MapFieldSpec getFieldSpec();

  // ---- Preferred API (new names) — override these in new implementations ----

  /// Returns the DataSource for the given map key's values.
  default DataSource getDataSource(String key) {
    return getKeyDataSource(key);
  }

  /// Returns DataSources for all keys present in this segment.
  default Map<String, DataSource> getDataSources() {
    return getKeyDataSources();
  }

  /// Returns the DataSourceMetadata for the given key's values.
  default DataSourceMetadata getDataSourceMetadata(String key) {
    return getKeyDataSourceMetadata(key);
  }

  /// Returns the ColumnIndexContainer for the given key's values.
  default ColumnIndexContainer getIndexContainer(String key) {
    return getKeyIndexContainer(key);
  }

  // ---- Deprecated API (old names) — still abstract for backward compatibility ----

  /// @deprecated Use {@link #getDataSource(String)} instead.
  @Deprecated
  DataSource getKeyDataSource(String key);

  /// @deprecated Use {@link #getDataSources()} instead.
  @Deprecated
  Map<String, DataSource> getKeyDataSources();

  /// @deprecated Use {@link #getDataSourceMetadata(String)} instead.
  @Deprecated
  DataSourceMetadata getKeyDataSourceMetadata(String key);

  /// @deprecated Use {@link #getIndexContainer(String)} instead.
  @Deprecated
  ColumnIndexContainer getKeyIndexContainer(String key);
}
