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
import org.apache.pinot.segment.local.utils.NullValueTransformerUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;

public class NullValueTransformer implements RecordTransformer {

  private final Map<String, Object> _defaultNullValues = new HashMap<>();

  public NullValueTransformer(TableConfig tableConfig, Schema schema) {
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        String fieldName = fieldSpec.getName();
        Object defaultNullValue = NullValueTransformerUtils.getDefaultNullValue(fieldSpec, tableConfig, schema);
        _defaultNullValues.put(fieldName, defaultNullValue);
      }
    }
  }

  @Override
  public void transform(GenericRow record) {
    for (Map.Entry<String, Object> entry : _defaultNullValues.entrySet()) {
      String fieldName = entry.getKey();
      Object value = record.getValue(fieldName);
      Object transformedValue = NullValueTransformerUtils.transformValue(value, entry.getValue());
      if (transformedValue != value) {
        record.putDefaultNullValue(fieldName, transformedValue);
      }
    }
  }
}
