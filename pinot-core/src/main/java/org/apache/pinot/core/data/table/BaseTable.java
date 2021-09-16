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

import java.util.Iterator;
import org.apache.pinot.spi.utils.DataSchema;


/**
 * Base abstract implementation of Table
 */
public abstract class BaseTable implements Table {
  protected final DataSchema _dataSchema;
  protected final int _numColumns;

  protected BaseTable(DataSchema dataSchema) {
    _dataSchema = dataSchema;
    _numColumns = dataSchema.size();
  }

  @Override
  public boolean merge(Table table) {
    Iterator<Record> iterator = table.iterator();
    while (iterator.hasNext()) {
      // NOTE: For performance concern, does not check the return value of the upsert(). Override this method if
      //       upsert() can return false.
      upsert(iterator.next());
    }
    return true;
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }
}
