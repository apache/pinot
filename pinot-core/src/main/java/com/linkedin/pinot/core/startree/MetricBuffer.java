/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec.DerivedMetricType;
import com.linkedin.pinot.core.startree.hll.HllUtil;
import com.linkedin.pinot.startree.hll.HllSizeUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * fromBytes and toBytes methods are used only in {@link OffHeapStarTreeBuilder}, as read and write to temp files.
 * Thus no serialization of hll type to string is necessary at these steps.
 */
public class MetricBuffer {

  /**
   * stored as number or hyperLogLog, but serialized out as number or string
   */
  private final Object[] values;
  private final List<MetricFieldSpec> metricFieldSpecs;

  public MetricBuffer(Object[] values, List<MetricFieldSpec> metricFieldSpecs) {
    this.values = values;
    this.metricFieldSpecs = metricFieldSpecs;
  }

  public MetricBuffer(MetricBuffer copy) {
    this.values = new Object[copy.values.length];
    for (int i = 0; i < this.values.length; i++) {
      Object copyValue = copy.values[i];
      if (copyValue instanceof HyperLogLog) {
        // deep copy of hll field
        this.values[i] = HllUtil.clone((HyperLogLog)copyValue,
            HllSizeUtils.getLog2mFromHllFieldSize(copy.metricFieldSpecs.get(i).getFieldSize()));
      } else if (copyValue instanceof Number) {
        // number field is immutable
        this.values[i] = copyValue;
      } else {
        throw new IllegalArgumentException("Unsupported metric type: " + copyValue.getClass());
      }
    }
    this.metricFieldSpecs = copy.metricFieldSpecs;
  }

  public static MetricBuffer fromBytes(byte[] bytes, List<MetricFieldSpec> metricFieldSpecs) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    Object[] values = new Object[metricFieldSpecs.size()];

    for (int i = 0; i < metricFieldSpecs.size(); i++) {
      MetricFieldSpec metric = metricFieldSpecs.get(i);
      if (metric.getDerivedMetricType() == DerivedMetricType.HLL) {
        byte[] hllBytes = new byte[metric.getFieldSize()]; // TODO: buffer reuse
        buffer.get(hllBytes);
        values[i] = HllUtil.buildHllFromBytes(hllBytes);
      } else {
        switch (metric.getDataType()) {
          case SHORT:
            values[i] = buffer.getShort();
            break;
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
          default:
            throw new IllegalArgumentException("Unsupported metric type " + metric.getDataType());
        }
      }
    }
    return new MetricBuffer(values, metricFieldSpecs);
  }

  public byte[] toBytes(int numBytes) throws IOException {
    byte[] bytes = new byte[numBytes];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    for (int i = 0; i < metricFieldSpecs.size(); i++) {
      MetricFieldSpec metric = metricFieldSpecs.get(i);
      if (metric.getDerivedMetricType() == DerivedMetricType.HLL) {
        buffer.put(((HyperLogLog)values[i]).getBytes());
      } else {
        switch (metric.getDataType()) {
          case SHORT:
            buffer.putShort(((Number) values[i]).shortValue());
            break;
          case INT:
            buffer.putInt(((Number) values[i]).intValue());
            break;
          case LONG:
            buffer.putLong(((Number) values[i]).longValue());
            break;
          case FLOAT:
            buffer.putFloat(((Number) values[i]).floatValue());
            break;
          case DOUBLE:
            buffer.putDouble(((Number) values[i]).doubleValue());
            break;
          default:
            throw new IllegalArgumentException("Unsupported metric type " + metric.getDataType());
        }
      }
    }
    return bytes;
  }

  public void aggregate(MetricBuffer metrics) {
    for (int i = 0; i < metricFieldSpecs.size(); i++) {
      MetricFieldSpec metric = metricFieldSpecs.get(i);
      if (metric.getDerivedMetricType() == DerivedMetricType.HLL) {
        try {
          ((HyperLogLog) values[i]).addAll((HyperLogLog) metrics.values[i]);
        } catch (CardinalityMergeException e) {
          throw new RuntimeException(e);
        }
      } else {
        switch (metric.getDataType()) {
          case SHORT:
            values[i] = ((Number) values[i]).shortValue() + ((Number) metrics.values[i]).shortValue();
            break;
          case INT:
            values[i] = ((Number) values[i]).intValue() + ((Number) metrics.values[i]).intValue();
            break;
          case LONG:
            values[i] = ((Number) values[i]).longValue() + ((Number) metrics.values[i]).longValue();
            break;
          case FLOAT:
            values[i] = ((Number) values[i]).floatValue() + ((Number) metrics.values[i]).floatValue();
            break;
          case DOUBLE:
            values[i] = ((Number) values[i]).doubleValue() + ((Number) metrics.values[i]).doubleValue();
            break;
          default:
            throw new IllegalArgumentException("Unsupported metric type " + metric.getDataType());
        }
      }
    }
  }

  /**
   * this method should return correct value conformed to datatype to iterators
   * @param index
   * @return
   */
  public Object getValueConformToDataType(int index) {
    if (metricFieldSpecs.get(index).getDerivedMetricType() == DerivedMetricType.HLL) {
      return HllUtil.convertHllToString((HyperLogLog) values[index]);
    } else {
      return values[index];
    }
  }

  @Override
  public String toString() {
    return Arrays.toString(values);
  }
}