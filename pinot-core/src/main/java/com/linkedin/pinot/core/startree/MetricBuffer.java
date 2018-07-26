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
package com.linkedin.pinot.core.startree;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec.DerivedMetricType;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.startree.hll.HllUtil;
import java.nio.ByteBuffer;
import java.util.List;


public class MetricBuffer {
  private final Object[] _values;
  private final List<MetricFieldSpec> _metricFieldSpecs;

  public static MetricBuffer fromBytes(byte[] bytes, List<MetricFieldSpec> metricFieldSpecs) {
    int numMetrics = metricFieldSpecs.size();
    Object[] values = new Object[numMetrics];

    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(PinotDataBuffer.NATIVE_ORDER);
    for (int i = 0; i < numMetrics; i++) {
      MetricFieldSpec metricFieldSpec = metricFieldSpecs.get(i);
      switch (metricFieldSpec.getDataType()) {
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
        case STRING:
          assert metricFieldSpec.getDerivedMetricType() == DerivedMetricType.HLL;
          byte[] hllBytes = new byte[metricFieldSpec.getFieldSize()];
          buffer.get(hllBytes);
          values[i] = HllUtil.buildHllFromBytes(hllBytes);
          break;
        default:
          throw new IllegalStateException();
      }
    }

    return new MetricBuffer(values, metricFieldSpecs);
  }

  public MetricBuffer(Object[] values, List<MetricFieldSpec> metricFieldSpecs) {
    _values = values;
    _metricFieldSpecs = metricFieldSpecs;
  }

  /**
   * NOTE: pass in byte size for performance. Byte size can be calculated by adding up field size for all metrics.
   */
  public byte[] toBytes(int byteSize) {
    byte[] bytes = new byte[byteSize];
    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(PinotDataBuffer.NATIVE_ORDER);

    int numValues = _values.length;
    for (int i = 0; i < numValues; i++) {
      MetricFieldSpec metricFieldSpec = _metricFieldSpecs.get(i);
      switch (metricFieldSpec.getDataType()) {
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
        case STRING:
          assert metricFieldSpec.getDerivedMetricType() == DerivedMetricType.HLL;
          buffer.put(HllUtil.toBytes((HyperLogLog) _values[i]));
          break;
        default:
          throw new IllegalStateException();
      }
    }

    return bytes;
  }

  public void aggregate(MetricBuffer buffer) {
    int numValues = _values.length;
    for (int i = 0; i < numValues; i++) {
      MetricFieldSpec metricFieldSpec = _metricFieldSpecs.get(i);
      switch (metricFieldSpec.getDataType()) {
        case INT:
          _values[i] = (Integer) _values[i] + (Integer) buffer._values[i];
          break;
        case LONG:
          _values[i] = (Long) _values[i] + (Long) buffer._values[i];
          break;
        case FLOAT:
          _values[i] = (Float) _values[i] + (Float) buffer._values[i];
          break;
        case DOUBLE:
          _values[i] = (Double) _values[i] + (Double) buffer._values[i];
          break;
        case STRING:
          assert metricFieldSpec.getDerivedMetricType() == DerivedMetricType.HLL;
          try {
            ((HyperLogLog) _values[i]).addAll((HyperLogLog) buffer._values[i]);
          } catch (CardinalityMergeException e) {
            throw new RuntimeException(e);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  public Object getValueConformToDataType(int index) {
    MetricFieldSpec metricFieldSpec = _metricFieldSpecs.get(index);
    if (metricFieldSpec.getDataType() == FieldSpec.DataType.STRING) {
      assert metricFieldSpec.getDerivedMetricType() == DerivedMetricType.HLL;
      return HllUtil.convertHllToString((HyperLogLog) _values[index]);
    } else {
      return _values[index];
    }
  }
}
