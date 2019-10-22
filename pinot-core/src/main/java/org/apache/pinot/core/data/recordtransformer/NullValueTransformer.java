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
package org.apache.pinot.core.data.recordtransformer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.FieldSpec.FieldType;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.data.GenericRow;


public class NullValueTransformer implements RecordTransformer {
  private final Map<String, Object> _defaultNullValues = new HashMap<>();
  private final boolean _nullHandlingEnabled;

  public NullValueTransformer(Schema schema) {
    this(schema, false);
  }

  public NullValueTransformer(Schema schema, boolean nullHandlingEnabled) {
    _nullHandlingEnabled = nullHandlingEnabled;
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn() && fieldSpec.getFieldType() != FieldType.TIME) {
        String fieldName = fieldSpec.getName();
        Object defaultNullValue = fieldSpec.getDefaultNullValue();
        if (fieldSpec.isSingleValueField()) {
          _defaultNullValues.put(fieldName, defaultNullValue);
        } else {
          _defaultNullValues.put(fieldName, new Object[]{defaultNullValue});
        }
      }
    }
  }

  @Override
  public GenericRow transform(GenericRow record) {
    Set<String> nullColumnsSet = null;
    if (_nullHandlingEnabled) {
      // Try and reuse the null columns set
      nullColumnsSet = (Set<String>) record.getValue(CommonConstants.Segment.NULL_FIELDS);
      if (nullColumnsSet != null) {
        nullColumnsSet.clear();
      }
    }

    for (Map.Entry<String, Object> entry : _defaultNullValues.entrySet()) {
      String fieldName = entry.getKey();
      Object value = record.getValue(fieldName);
      if (value == null || (value instanceof Object[] && ((Object[]) value).length == 0) || (value instanceof List
          && ((List) value).isEmpty())) {
        record.putDefaultNullValue(fieldName, entry.getValue());
        if (_nullHandlingEnabled) {
          if (nullColumnsSet == null) {
            nullColumnsSet = new HashSet<>();
          }
          nullColumnsSet.add(fieldName);
        }
      }
    }

    if (_nullHandlingEnabled) {
      record.putValue(CommonConstants.Segment.NULL_FIELDS, nullColumnsSet);
    }
    return record;
  }
}
