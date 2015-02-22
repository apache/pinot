package com.linkedin.pinot.core.realtime.utils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;


public class RealtimeMetricsSerDe {

  private final Schema schema;
  private final Map<String, Integer> metricsOffsetsMap;
  private int metricBuffSizeInBytes;

  public RealtimeMetricsSerDe(Schema schema) {
    this.schema = schema;
    this.metricsOffsetsMap = new HashMap<String, Integer>();
    init();
  }

  public void init() {
    createMetricsOffsetsMap();
    computeMetricsBuffAllocationSize();
  }

  public void computeMetricsBuffAllocationSize() {
    metricBuffSizeInBytes = 0;
    for (String metricName : schema.getMetricNames()) {
      switch (schema.getFieldSpecFor(metricName).getDataType()) {
        case INT:
          metricBuffSizeInBytes += Integer.SIZE / Byte.SIZE;
          break;
        case FLOAT:
          metricBuffSizeInBytes += Float.SIZE / Byte.SIZE;
          break;
        case LONG:
          metricBuffSizeInBytes += Long.SIZE / Byte.SIZE;
          break;
        case DOUBLE:
          metricBuffSizeInBytes += Double.SIZE / Byte.SIZE;
          break;
        default:
          break;
      }
    }
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
