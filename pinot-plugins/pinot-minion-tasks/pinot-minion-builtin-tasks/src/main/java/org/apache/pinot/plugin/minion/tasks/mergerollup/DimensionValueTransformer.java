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
package org.apache.pinot.plugin.minion.tasks.mergerollup;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code DimensionValueTransformer} class will transform certain dimension values by substituting the
 * existing value for that dimension with the 'defaultNullValue' from its 'fieldSpec'.
 */
public class DimensionValueTransformer implements RecordTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(DimensionValueTransformer.class);

  private final Map<String, Object> _defaultNullValues = new HashMap<>();
  private final Set<String> _dimensionsToErase;

  public DimensionValueTransformer(Schema schema, Set<String> dimensionsToErase) {
    _dimensionsToErase = dimensionsToErase;
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      String fieldName = fieldSpec.getName();
      Object defaultNullValue = fieldSpec.getDefaultNullValue();
      if (fieldSpec.isSingleValueField()) {
        _defaultNullValues.put(fieldName, defaultNullValue);
      } else {
        _defaultNullValues.put(fieldName, new Object[]{defaultNullValue});
      }
    }

    for (String key : dimensionsToErase) {
      if (!_defaultNullValues.containsKey(key)) {
        LOGGER.warn("Dimension name: {} does not exist in schema and will be ignored.", key);
      }
    }
  }

  @Override
  public boolean isNoOp() {
    return _dimensionsToErase.isEmpty();
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (String dimensionName : _dimensionsToErase) {
      Object defaultNullValue = _defaultNullValues.get(dimensionName);
      record.putDefaultNullValue(dimensionName, defaultNullValue);
    }
    return record;
  }
}
