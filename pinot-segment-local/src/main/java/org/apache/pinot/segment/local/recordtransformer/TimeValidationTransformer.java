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
package org.apache.pinot.segment.local.recordtransformer;

import javax.annotation.Nullable;
import org.apache.pinot.segment.local.utils.TimeValidationTransformerUtils;
import org.apache.pinot.segment.local.utils.TimeValidationTransformerUtils.TimeValidationConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;


/**
 * The {@code TimeValidationTransformer} class will validate the time column value.
 * <p>NOTE: should put this after the {@link DataTypeTransformer} so that all values follow the data types in
 * {@link FieldSpec}, and before the {@link NullValueTransformer} so that the invalidated value can be filled.
 */
public class TimeValidationTransformer implements RecordTransformer {

  private final String _timeColumnName;
  @Nullable
  private final TimeValidationConfig _config;

  public TimeValidationTransformer(TableConfig tableConfig, Schema schema) {
    _timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    _config = TimeValidationTransformerUtils.getConfig(tableConfig, schema);
  }

  @Override
  public boolean isNoOp() {
    return _config == null;
  }

  @Override
  public void transform(GenericRow record) {
    Object timeValue = record.getValue(_timeColumnName);
    if (timeValue == null) {
      return;
    }
    Object result = TimeValidationTransformerUtils.transformTimeValue(_config, timeValue);
    if (result == null) {
      record.putValue(_timeColumnName, null);
      record.markIncomplete();
    }
  }
}
