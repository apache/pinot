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

import java.util.HashSet;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * The {@code SpecialValueTransformer} class will transform special values according to the following rules:
 * <ul>
 *   <li>Negative zero (-0.0) should be converted to 0.0</li>
 *   <li>NaN should be converted to default null</li>
 * </ul>
 * <p>NOTE: should put this after the {@link DataTypeTransformer} so that all values follow the data types in
 * {@link FieldSpec}.
 */
public class SpecialValueTransformer implements RecordTransformer {
  private final HashSet<String> _specialValuesKeySet = new HashSet<>();

  public SpecialValueTransformer(Schema schema) {
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn() && (fieldSpec.getDataType() == DataType.FLOAT
          || fieldSpec.getDataType() == DataType.DOUBLE)) {
        _specialValuesKeySet.add(fieldSpec.getName());
      }
    }
  }

  private Object transformNegativeZero(Object value) {
    if ((value instanceof Float) && (Float.floatToRawIntBits((float) value) == Float.floatToRawIntBits(-0.0f))) {
      value = 0.0f;
    } else if ((value instanceof Double) && (Double.doubleToLongBits((double) value) == Double.doubleToLongBits(
        -0.0d))) {
      value = 0.0d;
    }
    return value;
  }

  private Object transformNaN(Object value) {
    if ((value instanceof Float) && ((Float) value).isNaN()) {
      value = FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT;
    } else if ((value instanceof Double) && ((Double) value).isNaN()) {
      value = FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE;
    }
    return value;
  }

  @Override
  public boolean isNoOp() {
    return _specialValuesKeySet.isEmpty();
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (String element : _specialValuesKeySet) {
      Object value = record.getValue(element);
      if (value instanceof Float || value instanceof Double) {
        // Single-valued column.
        Object zeroTransformedValue = transformNegativeZero(value);
        Object nanTransformedValue = transformNaN(zeroTransformedValue);
        if (nanTransformedValue != value) {
          record.putValue(element, nanTransformedValue);
        }
      } else if (value instanceof Object[]) {
        // Multi-valued column.
        Object[] values = (Object[]) value;
        int numValues = values.length;
        for (int i = 0; i < numValues; i++) {
          values[i] = transformNegativeZero(values[i]);
          values[i] = transformNaN(values[i]);
        }
      }
    }
    return record;
  }
}
