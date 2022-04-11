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
package org.apache.pinot.core.query.reduce.filter;

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/**
 * Value extractor for a non-post-aggregation column (group-by expression or aggregation).
 */
public class ColumnValueExtractor implements ValueExtractor {
  private final int _index;
  private final DataSchema _dataSchema;

  public ColumnValueExtractor(int index, DataSchema dataSchema) {
    _index = index;
    _dataSchema = dataSchema;
  }

  @Override
  public String getColumnName() {
    return _dataSchema.getColumnName(_index);
  }

  @Override
  public ColumnDataType getColumnDataType() {
    return _dataSchema.getColumnDataType(_index);
  }

  @Override
  public Object extract(Object[] row) {
    return row[_index];
  }
}
