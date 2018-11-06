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
import com.linkedin.pinot.common.utils.primitive.ByteArray;
import com.linkedin.pinot.core.data.GenericRow;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * The {@code SanitationTransformer} class will sanitize the values to follow certain rules including:
 * <ul>
 *   <li>No {@code null} characters in string values</li>
 *   <li>Bytes values are stored as {@link ByteArray}</li>
 * </ul>
 * <p>NOTE: should put this after the {@link DataTypeTransformer} so that all values follow the data types in
 * {@link FieldSpec}.
 */
public class SanitationTransformer implements RecordTransformer {
  List<String> _stringColumns = new ArrayList<>();
  List<String> _bytesColumns = new ArrayList<>();

  public SanitationTransformer(Schema schema) {
    for (Map.Entry<String, FieldSpec> entry : schema.getFieldSpecMap().entrySet()) {
      FieldSpec.DataType dataType = entry.getValue().getDataType();
      if (dataType == FieldSpec.DataType.STRING) {
        _stringColumns.add(entry.getKey());
      } else if (dataType == FieldSpec.DataType.BYTES) {
        _bytesColumns.add(entry.getKey());
      }
    }
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (String stringColumn : _stringColumns) {
      Object value = record.getValue(stringColumn);
      if (value instanceof String) {
        // Single-valued column
        String stringValue = (String) value;
        if (StringUtil.containsNullCharacter(stringValue)) {
          record.putField(stringColumn, StringUtil.removeNullCharacters(stringValue));
        }
      } else {
        // Multi-valued column
        Object[] values = (Object[]) value;
        int numValues = values.length;
        for (int i = 0; i < numValues; i++) {
          String stringValue = (String) values[i];
          if (StringUtil.containsNullCharacter(stringValue)) {
            values[i] = StringUtil.removeNullCharacters(stringValue);
          }
        }
      }
    }

    for (String bytesColumn : _bytesColumns) {
      Object value = record.getValue(bytesColumn);
      if (value instanceof byte[]) {
        record.putField(bytesColumn, new ByteArray((byte[]) value));
      }
    }

    return record;
  }
}
