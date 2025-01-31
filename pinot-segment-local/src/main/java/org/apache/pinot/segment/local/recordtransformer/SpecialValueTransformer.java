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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;


/**
 * The {@code SpecialValueTransformer} class will transform special values according to the following rules:
 * <ul>
 *   <li>
 *     For FLOAT and DOUBLE:
 *     <ul>
 *       <li>Negative zero (-0.0) should be converted to 0.0</li>
 *       <li>NaN should be converted to default null</li>
 *     </ul>
 *   </li>
 *   <li>
 *     For BIG_DECIMAL:
 *     <ul>
 *       <li>Remove trailing zeros</li>
 *     </ul>
 *   </li>
 * </ul>
 * <p>NOTE: should put this after the {@link DataTypeTransformer} so that we already have the values complying
 * with the schema before handling special values and before {@link NullValueTransformer} so that it transforms
 * all the null values properly.
 */
public class SpecialValueTransformer implements RecordTransformer {
  private final static int NEGATIVE_ZERO_FLOAT_BITS = Float.floatToRawIntBits(-0.0f);
  private final static long NEGATIVE_ZERO_DOUBLE_BITS = Double.doubleToLongBits(-0.0d);

  private final Set<String> _columnsToCheck = new HashSet<>();

  public SpecialValueTransformer(Schema schema) {
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        FieldSpec.DataType dataType = fieldSpec.getDataType();
        if (dataType == FieldSpec.DataType.FLOAT || dataType == FieldSpec.DataType.DOUBLE
            || dataType == FieldSpec.DataType.BIG_DECIMAL) {
          _columnsToCheck.add(fieldSpec.getName());
        }
      }
    }
  }

  @Override
  public boolean isNoOp() {
    return _columnsToCheck.isEmpty();
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (String column : _columnsToCheck) {
      Object value = record.getValue(column);
      if (value instanceof Object[]) {
        // Multi-valued column.
        Object[] values = (Object[]) value;
        List<Object> transformedValues = new ArrayList<>(values.length);
        boolean transformed = false;
        for (Object v : values) {
          Object transformedValue = transformValue(v);
          if (transformedValue != v) {
            transformed = true;
          }
          if (transformedValue != null) {
            transformedValues.add(transformedValue);
          }
          if (transformed) {
            record.putValue(column, !transformedValues.isEmpty() ? transformedValues.toArray() : null);
          }
        }
      } else if (value != null) {
        // Single-valued column.
        Object transformedValue = transformValue(value);
        if (transformedValue != value) {
          record.putValue(column, transformedValue);
        }
      }
    }
    return record;
  }

  @Nullable
  private Object transformValue(Object value) {
    if (value instanceof Float) {
      Float floatValue = (Float) value;
      if (floatValue.isNaN()) {
        return null;
      }
      if (Float.floatToRawIntBits(floatValue) == NEGATIVE_ZERO_FLOAT_BITS) {
        return 0.0f;
      }
    } else if (value instanceof Double) {
      Double doubleValue = (Double) value;
      if (doubleValue.isNaN()) {
        return null;
      }
      if (Double.doubleToRawLongBits(doubleValue) == NEGATIVE_ZERO_DOUBLE_BITS) {
        return 0.0d;
      }
    } else if (value instanceof BigDecimal) {
      BigDecimal bigDecimalValue = (BigDecimal) value;
      BigDecimal stripped = bigDecimalValue.stripTrailingZeros();
      if (!stripped.equals(bigDecimalValue)) {
        return stripped;
      }
    }
    return value;
  }
}
