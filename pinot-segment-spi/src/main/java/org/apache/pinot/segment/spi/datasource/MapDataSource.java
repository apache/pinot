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


public interface MapDataSource extends DataSource {

  /**
   * Get the map FieldSpec.
   */
  ComplexFieldSpec.MapFieldSpec getFieldSpec();

  /**
   * Get the Data Source representation of a single key within this map column.
   */
  DataSource getKeyDataSource(String key);

  /**
   * Get the Data Source representation of all keys within this map column.
   */
  Map<String, DataSource> getKeyDataSources();

  /**
   * Get the DataSourceMetadata of a single key within this map column.
   */
  DataSourceMetadata getKeyDataSourceMetadata(String key);

  /**
   * Get the IndexContainer of a single key within this map column.
   */
  ColumnIndexContainer getKeyIndexContainer(String key);
}
