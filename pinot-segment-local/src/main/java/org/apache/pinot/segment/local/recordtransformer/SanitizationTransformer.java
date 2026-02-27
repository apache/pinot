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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.local.utils.SanitizationTransformerUtils;
import org.apache.pinot.segment.local.utils.SanitizationTransformerUtils.SanitizationResult;
import org.apache.pinot.segment.local.utils.SanitizationTransformerUtils.SanitizedColumnInfo;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;


/**
 * The {@code SanitizationTransformer} class will sanitize the values to follow certain rules including:
 * <ul>
 *   <li>No {@code null} characters in string values</li>
 *   <li>String values are within the length limit</li>
 * </ul>
 * <p>NOTE: should put this after the {@link DataTypeTransformer} so that all values follow the data types in
 * {@link FieldSpec}.
 * This uses the MaxLengthExceedStrategy in the {@link FieldSpec} to decide what to do when the value exceeds the max.
 * For TRIM_LENGTH, the value is trimmed to the max length.
 * For SUBSTITUTE_DEFAULT_VALUE, the value is replaced with the default null value string.
 * For ERROR, an exception is thrown and the record is skipped.
 * For NO_ACTION, the value is kept as is if no NULL_CHARACTER present else trimmed till NULL.
 * In the first 2 scenarios, this metric REALTIME_ROWS_SANITIZED can be tracked to know if a trimmed /
 * default record was persisted.
 * In the third scenario, this metric ROWS_WITH_ERRORS can be tracked  to know if a record was skipped.
 * In the last scenario, this metric REALTIME_ROWS_SANITIZED can be tracked to know if a record was trimmed
 * due to having a null character.
 */
public class SanitizationTransformer implements RecordTransformer {
  private final Map<String, SanitizedColumnInfo> _columnToColumnInfoMap = new HashMap<>();

  public SanitizationTransformer(Schema schema) {
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        SanitizedColumnInfo info = SanitizationTransformerUtils.getSanitizedColumnInfo(fieldSpec);
        if (info != null) {
          _columnToColumnInfoMap.put(fieldSpec.getName(), info);
        }
      }
    }
  }

  @Override
  public boolean isNoOp() {
    return _columnToColumnInfoMap.isEmpty();
  }

  @Override
  public void transform(GenericRow record) {
    for (Map.Entry<String, SanitizedColumnInfo> entry : _columnToColumnInfoMap.entrySet()) {
      String columnName = entry.getKey();
      SanitizedColumnInfo info = entry.getValue();
      Object value = record.getValue(columnName);
      SanitizationResult result = SanitizationTransformerUtils.sanitizeValue(info, value);
      if (result != null) {
        record.putValue(columnName, result.getValue());
        if (result.isSanitized()) {
          record.markSanitized();
        }
      }
    }
  }
}
