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
package org.apache.pinot.segment.local.segment.index.loader.defaultcolumn;

import com.google.common.base.Utf8;
import java.math.BigDecimal;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;


/// Column statistics for a default-value column.
@SuppressWarnings("rawtypes")
public class DefaultColumnStatistics implements ColumnStatistics {
  private final FieldSpec _fieldSpec;
  private final int _numDocs;
  private final Comparable _value;
  private final Object _valueSet;
  private final int _valueLength;
  private final boolean _isAscii;

  public DefaultColumnStatistics(FieldSpec fieldSpec, int numDocs) {
    _fieldSpec = fieldSpec;
    _numDocs = numDocs;

    DataType valueType = fieldSpec.getDataType().getStoredType();
    Object value = fieldSpec.getDefaultNullValue();
    int length = 0;
    boolean isAscii = false;
    switch (valueType) {
      case INT:
        _valueSet = new int[]{(Integer) value};
        break;
      case LONG:
        _valueSet = new long[]{(Long) value};
        break;
      case FLOAT:
        _valueSet = new float[]{(Float) value};
        break;
      case DOUBLE:
        _valueSet = new double[]{(Double) value};
        break;
      case BIG_DECIMAL:
        _valueSet = new BigDecimal[]{(BigDecimal) value};
        length = BigDecimalUtils.byteSize((BigDecimal) value);
        break;
      case STRING:
        String stringValue = (String) value;
        _valueSet = new String[]{stringValue};
        length = Utf8.encodedLength(stringValue);
        isAscii = length == stringValue.length();
        break;
      case BYTES:
        ByteArray byteArrayValue = new ByteArray((byte[]) value);
        value = byteArrayValue;
        _valueSet = new ByteArray[]{byteArrayValue};
        length = byteArrayValue.length();
        break;
      // TODO: Support MAP
      default:
        throw new IllegalArgumentException(
            "Unsupported value type: " + valueType + " for column: " + fieldSpec.getName());
    }
    _value = (Comparable) value;
    _valueLength = valueType.isFixedWidth() ? valueType.size() : length;
    _isAscii = isAscii;
  }

  @Override
  public FieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  @Override
  public Comparable getMinValue() {
    return _value;
  }

  @Override
  public Comparable getMaxValue() {
    return _value;
  }

  @Override
  public Object getUniqueValuesSet() {
    return _valueSet;
  }

  @Override
  public int getCardinality() {
    return 1;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _valueLength;
  }

  @Override
  public int getLengthOfLongestElement() {
    return _valueLength;
  }

  @Override
  public boolean isAscii() {
    return _isAscii;
  }

  @Override
  public boolean isSorted() {
    return isSingleValue();
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _numDocs;
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return isSingleValue() ? 0 : 1;
  }

  @Override
  public int getMaxRowLengthInBytes() {
    return _valueLength;
  }

  @Nullable
  @Override
  public PartitionFunction getPartitionFunction() {
    return null;
  }

  @Nullable
  @Override
  public Set<Integer> getPartitions() {
    return null;
  }
}
