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

package com.linkedin.pinot.core.startree.v2;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.quantile.QDigest;
import com.clearspring.analytics.stream.quantile.TDigest;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.datatable.ObjectCustomSerDe;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;


public class AggregationFunctionColumnPairBuffer {

  int _totalBytes;
  private final Object[] _values;
  private final List<AggregationFunctionColumnPair> _aggFunColumnPairs;

  public AggregationFunctionColumnPairBuffer(Object[] values, List<AggregationFunctionColumnPair> aggFunColumnPairs) {
    _values = values;
    _aggFunColumnPairs = aggFunColumnPairs;

    calculateBytesRequired();
  }

  /**
   * NOTE: pass in byte size for performance. Byte size can be calculated by adding up field size for all metrics.
   */
  public byte[] toBytes() {
    byte[] bytes = new byte[_totalBytes];
    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(PinotDataBuffer.NATIVE_ORDER);

    for (int i = 0; i < _aggFunColumnPairs.size(); i++) {
      AggregationFunctionColumnPair pair = _aggFunColumnPairs.get(i);
      AggregationFunction factory = AggregationFunctionFactory.getAggregationFunction(pair.getFunctionType().getName());

      switch (factory.getDataType()) {
        case INT:
          buffer.putInt((Integer) _values[i]);
          break;
        case LONG:
          buffer.putLong((Long) _values[i]);
          break;
        case FLOAT:
          buffer.putFloat((Float) _values[i]);
          break;
        case DOUBLE:
          buffer.putDouble((Double) _values[i]);
          break;
        case BYTES:
          int size = getObjectSize(factory, _values[i]);
          buffer.putInt(size);
          try {
            buffer.put(ObjectCustomSerDe.serialize(_values[i]));
          } catch (IOException e) {
            e.printStackTrace();
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }

    return bytes;
  }

  private void calculateBytesRequired() {
    for (int i = 0; i < _aggFunColumnPairs.size(); i++) {
      AggregationFunctionColumnPair pair = _aggFunColumnPairs.get(i);
      AggregationFunction factory = AggregationFunctionFactory.getAggregationFunction(pair.getFunctionType().getName());
      int size = getObjectSize(factory, _values[i]);
      _totalBytes += factory.getDataType().equals(FieldSpec.DataType.BYTES) ? size + Integer.BYTES : size;
    }
  }

  private int getObjectSize(AggregationFunction factory, Object obj) {
    switch (factory.getDataType()) {
      case INT:
        return Integer.BYTES;
      case LONG:
        return Long.BYTES;
      case FLOAT:
        return Float.BYTES;
      case DOUBLE:
        return Double.BYTES;
      case BYTES:
        if (obj instanceof HyperLogLog)
          return ((HyperLogLog) obj).sizeof();
        else if (obj instanceof TDigest)
          return ((TDigest) obj).byteSize();
        else if (obj instanceof QDigest)
          return ((HyperLogLog) obj).sizeof();
      default:
        throw new IllegalStateException();
    }
  }
}
