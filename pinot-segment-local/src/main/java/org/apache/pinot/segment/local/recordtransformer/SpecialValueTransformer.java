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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code SpecialValueTransformer} class will transform special values according to the following rules:
 * <ul>
 *   <li>Negative zero (-0.0) should be converted to 0.0</li>
 *   <li>NaN should be converted to default null</li>
 * </ul>
 * <p>NOTE: should put this after the {@link DataTypeTransformer} so that we already have the values complying
 * with the schema before handling special values and before {@link NullValueTransformer} so that it transforms
 * all the null values properly.
 */
public class SpecialValueTransformer implements RecordTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpecialValueTransformer.class);
  private final HashSet<String> _specialValuesKeySet = new HashSet<>();
  private int _negativeZeroConversionCount = 0;
  private int _nanConversionCount = 0;

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
      _negativeZeroConversionCount++;
    } else if ((value instanceof Double) && (Double.doubleToLongBits((double) value) == Double.doubleToLongBits(
        -0.0d))) {
      value = 0.0d;
      _negativeZeroConversionCount++;
    }
    return value;
  }

  private Object transformNaN(Object value) {
    if ((value instanceof Float) && ((Float) value).isNaN()) {
      value = null;
      _nanConversionCount++;
    } else if ((value instanceof Double) && ((Double) value).isNaN()) {
      _nanConversionCount++;
      value = null;
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
      if (value instanceof Object[]) {
        // Multi-valued column.
        Object[] values = (Object[]) value;
        int numValues = values.length;
        List<Object> negativeZeroNanSanitizedValues = new ArrayList<>(numValues);
        for (Object o : values) {
          Object zeroTransformedValue = transformNegativeZero(o);
          Object nanTransformedValue = transformNaN(zeroTransformedValue);
          if (nanTransformedValue != null) {
            negativeZeroNanSanitizedValues.add(nanTransformedValue);
          }
        }
        record.putValue(element, negativeZeroNanSanitizedValues.toArray());
      } else {
        // Single-valued column.
        Object zeroTransformedValue = transformNegativeZero(value);
        Object nanTransformedValue = transformNaN(zeroTransformedValue);
        if (nanTransformedValue != value) {
          record.putValue(element, nanTransformedValue);
        }
      }
    }
    if (_negativeZeroConversionCount > 0 || _nanConversionCount > 0) {
      LOGGER.info("Converted {} -0.0s to 0.0 and {} NaNs to null", _negativeZeroConversionCount, _nanConversionCount);
    }
    return record;
  }

  @VisibleForTesting
  int getNegativeZeroConversionCount() {
    return _negativeZeroConversionCount;
  }

  @VisibleForTesting
  int getNanConversionCount() {
    return _nanConversionCount;
  }
}
