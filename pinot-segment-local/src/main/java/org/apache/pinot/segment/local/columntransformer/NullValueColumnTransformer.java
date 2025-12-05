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
package org.apache.pinot.segment.local.columntransformer;

import org.apache.pinot.segment.local.utils.NullValueTransformerUtils;
import org.apache.pinot.spi.columntransformer.ColumnTransformer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class NullValueColumnTransformer implements ColumnTransformer {

  private final Object _defaultNullValue;

  /**
   * @param tableConfig The table configuration
   * @param fieldSpec The field specification for the column
   * @param schema The schema (required for proper handling of time columns)
   */
  public NullValueColumnTransformer(TableConfig tableConfig, FieldSpec fieldSpec, Schema schema) {
    _defaultNullValue = NullValueTransformerUtils.getDefaultNullValue(fieldSpec, tableConfig, schema);
  }

  @Override
  public boolean isNoOp() {
    return false;
  }

  @Override
  public Object transform(Object value) {
    return NullValueTransformerUtils.transformValue(value, _defaultNullValue);
  }
}
