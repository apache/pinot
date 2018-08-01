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

import java.util.List;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.linkedin.pinot.common.data.FieldSpec;
import com.clearspring.analytics.stream.quantile.QDigest;
import com.clearspring.analytics.stream.quantile.TDigest;
import com.linkedin.pinot.core.common.datatable.ObjectType;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.core.common.datatable.ObjectCustomSerDe;


public class AggregationFunctionColumnPairBuffer {

  int _totalBytes;
  protected final Object[] _values;
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
          buffer.putInt(new Integer(_values[i].toString()));
          break;
        case LONG:
          buffer.putLong(new Long(_values[i].toString()));
          break;
        case FLOAT:
          buffer.putFloat(new Float(_values[i].toString()));
          break;
        case DOUBLE:
          buffer.putDouble(new Double(_values[i].toString()));
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

  /**
   * NOTE: pass in byte size for performance. Byte size can be calculated by adding up field size for all metrics.
   */
  public static AggregationFunctionColumnPairBuffer fromBytes(byte[] bytes, List<AggregationFunctionColumnPair> aggFunColumnPairs) {

    int numMetrics = aggFunColumnPairs.size();
    Object[] values = new Object[numMetrics];

    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(PinotDataBuffer.NATIVE_ORDER);
    for (int i = 0; i < numMetrics; i++) {
      AggregationFunctionColumnPair pair = aggFunColumnPairs.get(i);
      AggregationFunction factory = AggregationFunctionFactory.getAggregationFunction(pair.getFunctionType().getName());

      switch (factory.getDataType()) {
        case INT:
          values[i] = buffer.getInt();
          break;
        case LONG:
          values[i] = buffer.getLong();
          break;
        case FLOAT:
          values[i] = buffer.getFloat();
          break;
        case DOUBLE:
          values[i] = buffer.getDouble();
          break;
        case BYTES:
          int objLength = buffer.getInt();
          byte[] objBytes = new byte[objLength];
          buffer.get(objBytes);
          try {
            if (pair.getFunctionType().getName().equals(StarTreeV2Constant.AggregateFunctions.DISTINCTCOUNTHLL)) {
              values[i] = ObjectCustomSerDe.deserialize(objBytes, ObjectType.HyperLogLog);
            } else if (pair.getFunctionType().getName().equals(StarTreeV2Constant.AggregateFunctions.PERCENTILEEST)) {
              values[i] = ObjectCustomSerDe.deserialize(objBytes, ObjectType.QuantileDigest);
            } else if (pair.getFunctionType().getName().equals(StarTreeV2Constant.AggregateFunctions.PERCENTILETDIGEST)) {
              values[i] = ObjectCustomSerDe.deserialize(objBytes, ObjectType.TDigest);
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }

    return new AggregationFunctionColumnPairBuffer(values, aggFunColumnPairs);
  }

  /**
   * NOTE: pass in byte size for performance. Byte size can be calculated by adding up field size for all metrics.
   */
  public void aggregate(AggregationFunctionColumnPairBuffer buffer) {
    int numValues = _values.length;
    for (int i = 0; i < numValues; i++) {
      AggregationFunctionColumnPair pair = _aggFunColumnPairs.get(i);
      AggregationFunction factory = AggregationFunctionFactory.getAggregationFunction(pair.getFunctionType().getName());
      _values[i] = factory.aggregate(_values[i], buffer._values[i]);
    }
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
