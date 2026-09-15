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
package org.apache.pinot.core.query.aggregation.utils.exprminmax;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.roaringbitmap.RoaringBitmap;


/// Wrapper class for projection block value set for exprmin/max aggregation function.
/// Used to get the value from val set of different data types.
public class ExprMinMaxProjectionValSetWrapper extends ExprMinMaxWrapperValSet {
  private final boolean _nullHandlingEnabled;
  @Nullable
  private RoaringBitmap _nullBitmap;

  public ExprMinMaxProjectionValSetWrapper(BlockValSet blockValSet) {
    this(blockValSet, ColumnDataType.fromDataType(blockValSet.getValueType().getStoredType(),
        blockValSet.isSingleValue()), false);
  }

  /// Preserves the bound projection type and nulls while reading each physical block through its conversion getters.
  public ExprMinMaxProjectionValSetWrapper(BlockValSet blockValSet, ColumnDataType type, boolean nullHandlingEnabled) {
    super(type.getStoredType());
    Preconditions.checkState(type.isArray() != blockValSet.isSingleValue(),
        "ExprMinMax projection cardinality does not match its bound type: %s", type);
    _nullHandlingEnabled = nullHandlingEnabled;
    setNewBlock(blockValSet);
  }

  @Override
  public void setNewBlock(BlockValSet blockValSet) {
    super.setNewBlock(blockValSet);
    _nullBitmap = _nullHandlingEnabled ? blockValSet.getNullBitmap() : null;
  }

  public Object getValue(int i) {
    if (_nullBitmap != null && _nullBitmap.contains(i)) {
      return null;
    }
    switch (_storedType) {
      case INT:
        return _intValues[i];
      case LONG:
        return _longValues[i];
      case FLOAT:
        return _floatValues[i];
      case DOUBLE:
        return _doublesValues[i];
      case BIG_DECIMAL:
      case STRING:
      case BYTES:
        return _objectsValues[i];
      case INT_ARRAY:
        return _intValuesMV[i].length == 0 ? null : _intValuesMV[i];
      case LONG_ARRAY:
        return _longValuesMV[i].length == 0 ? null : _longValuesMV[i];
      case FLOAT_ARRAY:
        return _floatValuesMV[i].length == 0 ? null : _floatValuesMV[i];
      case DOUBLE_ARRAY:
        return _doublesValuesMV[i].length == 0 ? null : _doublesValuesMV[i];
      case BIG_DECIMAL_ARRAY:
      case STRING_ARRAY:
      case BYTES_ARRAY:
        return _objectsValuesMV[i].length == 0 ? null : _objectsValuesMV[i];
      default:
        throw new IllegalStateException("Unsupported stored type: " + _storedType);
    }
  }
}
