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
package com.linkedin.pinot.core.realtime.utils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.impl.fwdindex.ByteBufferUtils;


public class RealtimeMetricsSerDe {

  private final Schema schema;
  private final Map<String, Integer> metricsOffsetsMap;
  private final int metricBuffSizeInBytes;

  public RealtimeMetricsSerDe(Schema schema) {
    this.schema = schema;
    this.metricsOffsetsMap = new HashMap<String, Integer>();
    metricBuffSizeInBytes = ByteBufferUtils.computeMetricsBuffAllocationSize(schema);
    init();
  }

  public void init() {
    createMetricsOffsetsMap();
  }

  public void createMetricsOffsetsMap() {
    int offset = 0;
    for (String metric : schema.getMetricNames()) {
      metricsOffsetsMap.put(metric, offset);
      switch (schema.getFieldSpecFor(metric).getDataType()) {
        case INT:
          offset += Integer.SIZE / Byte.SIZE;
          break;
        case FLOAT:
          offset += Float.SIZE / Byte.SIZE;
          break;
        case LONG:
          offset += Long.SIZE / Byte.SIZE;
          break;
        case DOUBLE:
          offset += Double.SIZE / Byte.SIZE;
          break;
        default:
          break;
      }
    }
  }

  public ByteBuffer serialize(GenericRow row) {
    ByteBuffer metricBuff = ByteBuffer.allocate(metricBuffSizeInBytes);
    for (String metric : schema.getMetricNames()) {
      Object entry = row.getValue(metric);
      FieldSpec spec = schema.getFieldSpecFor(metric);
      switch (spec.getDataType()) {
        case INT:
          metricBuff.putInt((Integer) entry);
          break;
        case LONG:
          metricBuff.putLong((Long) entry);
          break;
        case FLOAT:
          metricBuff.putFloat((Float) entry);
          break;
        case DOUBLE:
          metricBuff.putDouble((Double) entry);
          break;
      }
    }
    return metricBuff;
  }

  public Object getRawValueFor(String metric, ByteBuffer metBuff) {
    Object ret = null;

    switch (schema.getFieldSpecFor(metric).getDataType()) {
      case INT:
        ret = new Integer(metBuff.getInt(metricsOffsetsMap.get(metric)));
        break;
      case FLOAT:
        ret = new Float(metBuff.getFloat(metricsOffsetsMap.get(metric)));
        break;
      case LONG:
        ret = new Long(metBuff.getLong(metricsOffsetsMap.get(metric)));
        break;
      case DOUBLE:
        ret = new Double(metBuff.getDouble(metricsOffsetsMap.get(metric)));
        break;
      default:
        return null;
    }
    return ret;
  }

  public int getIntVal(String metric, ByteBuffer metBuff) {
    return metBuff.getInt(metricsOffsetsMap.get(metric));
  }

  public float getFloatVal(String metric, ByteBuffer metBuff) {
    return metBuff.getFloat(metricsOffsetsMap.get(metric));
  }

  public double getDoubleVal(String metric, ByteBuffer metBuff) {
    return metBuff.getDouble(metricsOffsetsMap.get(metric));
  }

  public long getLongVal(String metric, ByteBuffer metBuff) {
    return metBuff.getLong(metricsOffsetsMap.get(metric));
  }

}
