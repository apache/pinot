/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.linkedin.pinot.common.data.FieldSpec.DataType;


public class MetricBuffer {
  Number[] numbers;

  public MetricBuffer(Number[] numbers) {
    this.numbers = numbers;
  }

  public static MetricBuffer fromBytes(byte[] bytes, List<DataType> metricTypes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    Number[] numbers = new Number[metricTypes.size()];
    for (int i = 0; i < metricTypes.size(); i++) {
      switch (metricTypes.get(i)) {
        case SHORT:
          numbers[i] = buffer.getShort();
          break;
        case INT:
          numbers[i] = buffer.getInt();
          break;
        case LONG:
          numbers[i] = buffer.getLong();
          break;
        case FLOAT:
          numbers[i] = buffer.getFloat();
          break;
        case DOUBLE:
          numbers[i] = buffer.getDouble();
          break;
        default:
          throw new IllegalArgumentException("Unsupported metric type " + metricTypes.get(i));
      }
    }
    return new MetricBuffer(numbers);
  }

  public Number get(int index) {
    return numbers[index];
  }

  public byte[] toBytes(int numBytes, List<DataType> metricTypes) {
    byte[] bytes = new byte[numBytes];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    for (int i = 0; i < metricTypes.size(); i++) {
      switch (metricTypes.get(i)) {
        case SHORT:
          buffer.putShort(numbers[i].shortValue());
          break;
        case INT:
          buffer.putInt(numbers[i].intValue());
          break;
        case LONG:
          buffer.putLong(numbers[i].longValue());
          break;
        case FLOAT:
          buffer.putFloat(numbers[i].floatValue());
          break;
        case DOUBLE:
          buffer.putDouble(numbers[i].doubleValue());
          break;
        default:
          throw new IllegalArgumentException("Unsupported metric type " + metricTypes.get(i));
      }
    }
    return bytes;
  }

  public void aggregate(MetricBuffer metrics, List<DataType> metricTypes) {
    for (int i = 0; i < metricTypes.size(); i++) {
      switch (metricTypes.get(i)) {
        case SHORT:
          numbers[i] = numbers[i].shortValue() + metrics.get(i).shortValue();
          break;
        case INT:
          numbers[i] = numbers[i].intValue() + metrics.get(i).intValue();
          break;
        case LONG:
          numbers[i] = numbers[i].longValue() + metrics.get(i).longValue();
          break;
        case FLOAT:
          numbers[i] = numbers[i].floatValue() + metrics.get(i).floatValue();
          break;
        case DOUBLE:
          numbers[i] = numbers[i].doubleValue() + metrics.get(i).doubleValue();
          break;
        default:
          throw new IllegalArgumentException("Unsupported metric type " + metricTypes.get(i));
      }
    }
  }

  @Override
  public String toString() {
    return Arrays.toString(numbers);
  }
}
