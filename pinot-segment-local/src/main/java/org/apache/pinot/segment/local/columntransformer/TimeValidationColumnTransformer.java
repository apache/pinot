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

import javax.annotation.Nullable;
import org.apache.pinot.segment.local.utils.TimeValidationTransformerUtils;
import org.apache.pinot.segment.local.utils.TimeValidationTransformerUtils.TimeValidationConfig;
import org.apache.pinot.spi.columntransformer.ColumnTransformer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Column transformer that validates time column values are within the valid range (1971-2071).
 * Invalid values are transformed to null.
 *
 * <p>This transformer should be applied after {@link DataTypeColumnTransformer} so that all values
 * follow the data types in FieldSpec, and before {@link NullValueColumnTransformer} so that
 * invalidated values can be filled with defaults.
 */
public class TimeValidationColumnTransformer implements ColumnTransformer {

  @Nullable
  private final TimeValidationConfig _config;

  /**
   * Create a TimeValidationColumnTransformer for a specific column.
   *
   * @param tableConfig The table configuration
   * @param schema The schema
   * @param columnName The column name this transformer is being applied to
   */
  public TimeValidationColumnTransformer(TableConfig tableConfig, Schema schema, String columnName) {
    String timeColumnName = tableConfig.getValidationConfig() != null
        ? tableConfig.getValidationConfig().getTimeColumnName() : null;
    if (timeColumnName != null && timeColumnName.equals(columnName)) {
      _config = TimeValidationTransformerUtils.getConfig(tableConfig, schema);
    } else {
      _config = null;
    }
  }

  @Override
  public boolean isNoOp() {
    return _config == null;
  }

  @Override
  public Object transform(@Nullable Object value) {
    return TimeValidationTransformerUtils.transformTimeValue(_config, value);
  }
}
