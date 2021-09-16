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
package org.apache.pinot.core.data.table;

import java.util.HashMap;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.utils.DataSchema;


/**
 * {@link Table} implementation for aggregating TableRecords based on combination of keys
 */
@NotThreadSafe
public class SimpleIndexedTable extends IndexedTable {

  public SimpleIndexedTable(DataSchema dataSchema, QueryContext queryContext, int resultSize, int trimSize,
      int trimThreshold) {
    super(dataSchema, queryContext, resultSize, trimSize, trimThreshold, new HashMap<>());
  }

  /**
   * Non thread safe implementation of upsert to insert {@link Record} into the {@link Table}
   */
  @Override
  public boolean upsert(Key key, Record record) {
    if (_hasOrderBy) {
      addOrUpdateRecord(key, record);
      if (_lookupMap.size() >= _trimThreshold) {
        resize();
      }
    } else {
      if (_lookupMap.size() < _resultSize) {
        addOrUpdateRecord(key, record);
      } else {
        updateExistingRecord(key, record);
      }
    }
    return true;
  }
}
