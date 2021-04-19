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
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * The {@code SanitizationTransformer} class will sanitize the values to follow certain rules including:
 * <ul>
 *   <li>No {@code null} characters in string values</li>
 *   <li>String values are within the length limit</li>
 *   TODO: add length limit to BYTES values if necessary
 * </ul>
 * <p>NOTE: should put this after the {@link DataTypeTransformer} so that all values follow the data types in
 * {@link FieldSpec}.
 */
public class SanitizationTransformer implements RecordTransformer {
  private final Map<String, Integer> _stringColumnMaxLengthMap = new HashMap<>();

  public SanitizationTransformer(Schema schema) {
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn() && fieldSpec.getDataType() == DataType.STRING) {
        _stringColumnMaxLengthMap.put(fieldSpec.getName(), fieldSpec.getMaxLength());
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
          record.putValue(stringColumn, sanitizedValue);
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
