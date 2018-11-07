/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.recordtransformer;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.data.GenericRow;
import java.util.HashMap;
import java.util.Map;


/**
 * The {@code SanitationTransformer} class will sanitize the values to follow certain rules including:
 * <ul>
 *   <li>No {@code null} characters in string values</li>
 *   <li>Values are within the length limit</li>
 *   TODO: add length limit to BYTES values if necessary
 * </ul>
 * <p>NOTE: should put this after the {@link DataTypeTransformer} so that all values follow the data types in
 * {@link FieldSpec}.
 */
public class SanitationTransformer implements RecordTransformer {
  private final Map<String, Integer> _stringColumnMaxLengthMap = new HashMap<>();

  public SanitationTransformer(Schema schema) {
    for (Map.Entry<String, FieldSpec> entry : schema.getFieldSpecMap().entrySet()) {
      FieldSpec fieldSpec = entry.getValue();
      if (fieldSpec.getDataType() == FieldSpec.DataType.STRING) {
        _stringColumnMaxLengthMap.put(entry.getKey(), fieldSpec.getMaxLength());
      }
    }
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (Map.Entry<String, Integer> entry : _stringColumnMaxLengthMap.entrySet()) {
      String stringColumn = entry.getKey();
      int maxLength = entry.getValue();
      Object value = record.getValue(stringColumn);
      if (value instanceof String) {
        // Single-valued column
        String stringValue = (String) value;
        String sanitizedValue = StringUtil.sanitizeStringValue(stringValue, maxLength);
        // NOTE: reference comparison
        //noinspection StringEquality
        if (sanitizedValue != stringValue) {
          record.putField(stringColumn, sanitizedValue);
        }
      } else {
        // Multi-valued column
        Object[] values = (Object[]) value;
        int numValues = values.length;
        for (int i = 0; i < numValues; i++) {
          values[i] = StringUtil.sanitizeStringValue((String) values[i], maxLength);
        }
      }
    }
    return record;
  }
}
