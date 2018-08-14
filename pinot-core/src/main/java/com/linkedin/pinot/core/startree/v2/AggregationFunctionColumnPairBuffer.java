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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.datatable.ObjectCustomSerDe;
import com.linkedin.pinot.core.common.datatable.ObjectType;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;


public class AggregationFunctionColumnPairBuffer {

  int _totalBytes;
  final Object[] _values;
  private final List<AggregationFunction> _aggFunColumnPairFunctions;

  AggregationFunctionColumnPairBuffer(Object[] values, List<AggregationFunction> aggFunColumnPairFunctions) {
    _values = values;
    _aggFunColumnPairFunctions = aggFunColumnPairFunctions;
  }

  /**
   * NOTE: convert to byte array from objects array.
   */
  public byte[] toBytes() throws IOException {
    calculateBytesRequired();
    byte[] bytes = new byte[_totalBytes];
    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(PinotDataBuffer.NATIVE_ORDER);

    for (int i = 0; i < _aggFunColumnPairFunctions.size(); i++) {
      AggregationFunction factory = _aggFunColumnPairFunctions.get(i);

      switch (factory.getResultDataType()) {
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
          byte[] serializedObject = ObjectCustomSerDe.serialize(_values[i]);
          buffer.putInt(serializedObject.length);
          buffer.put(serializedObject);
          break;
        default:
          throw new IllegalStateException();
      }
    }

    return bytes;
  }

  /**
   * NOTE: convert from byte to  objects array
   */
  public static AggregationFunctionColumnPairBuffer fromBytes(byte[] bytes,
      List<AggregationFunction> aggFunColumnPairFunctions) throws IOException {

    int numMetrics = aggFunColumnPairFunctions.size();
    Object[] values = new Object[numMetrics];
    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(PinotDataBuffer.NATIVE_ORDER);

    for (int i = 0; i < numMetrics; i++) {
      AggregationFunction factory = aggFunColumnPairFunctions.get(i);

      switch (factory.getResultDataType()) {
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
          if (factory.getType().getName().equals(AggregationFunctionType.DISTINCTCOUNTHLL.getName())) {
            values[i] = ObjectCustomSerDe.deserialize(objBytes, ObjectType.HyperLogLog);
          } else if (factory.getType().getName().equals(AggregationFunctionType.PERCENTILEEST.getName())) {
            values[i] = ObjectCustomSerDe.deserialize(objBytes, ObjectType.QuantileDigest);
          } else if (factory.getType().getName().equals(AggregationFunctionType.PERCENTILETDIGEST.getName())) {
            values[i] = ObjectCustomSerDe.deserialize(objBytes, ObjectType.TDigest);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }

    return new AggregationFunctionColumnPairBuffer(values, aggFunColumnPairFunctions);
  }

  /**
   * NOTE: aggregate the metric values.
   */
  public void aggregate(AggregationFunctionColumnPairBuffer buffer) {
    int numValues = _values.length;

    int aggregatedByteSize = 0;
    for (int i = 0; i < numValues; i++) {
      AggregationFunction factory = _aggFunColumnPairFunctions.get(i);
      _values[i] = factory.aggregate(_values[i], buffer._values[i]);
      try {
        aggregatedByteSize += getObjectSize(factory, _values[i]);
        if (factory.getResultDataType().equals(FieldSpec.DataType.BYTES)) {
          aggregatedByteSize += Integer.BYTES;
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    _totalBytes = aggregatedByteSize;
  }

  /**
   * NOTE: calculate size of buffer required to store metric values.
   */
  private void calculateBytesRequired() {
    for (int i = 0; i < _aggFunColumnPairFunctions.size(); i++) {
      AggregationFunction factory = _aggFunColumnPairFunctions.get(i);
      int size = 0;
      try {
        size = getObjectSize(factory, _values[i]);
      } catch (IOException e) {
        e.printStackTrace();
      }
      _totalBytes += factory.getResultDataType().equals(FieldSpec.DataType.BYTES) ? (size + Integer.BYTES) : size;
    }
  }

  /**
   * NOTE: get the size of the objects.
   */
  private int getObjectSize(AggregationFunction factory, Object obj) throws IOException {

    switch (factory.getResultDataType()) {
      case INT:
        return Integer.BYTES;
      case LONG:
        return Long.BYTES;
      case FLOAT:
        return Float.BYTES;
      case DOUBLE:
        return Double.BYTES;
      case BYTES:
        return ObjectCustomSerDe.serialize(obj).length;
      default:
        throw new IllegalStateException();
    }
  }
}
