package com.linkedin.pinot.core.realtime.impl.fwdindex;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;


public class ByteBufferUtils {

  /**
  *
  * @param metric
  * @param metBuff
  */

  public static Object extractMetricValueFrom(String metric, ByteBuffer metBuff, Schema dataSchema,
      Map<String, Integer> offsetsMap) {
    Object ret = null;

    switch (dataSchema.getFieldSpecFor(metric).getDataType()) {
      case INT:
        ret = new Integer(metBuff.getInt(offsetsMap.get(metric)));
        break;
      case FLOAT:
        ret = new Float(metBuff.getFloat(offsetsMap.get(metric)));
        break;
      case LONG:
        ret = new Long(metBuff.getLong(offsetsMap.get(metric)));
        break;
      case DOUBLE:
        ret = new Double(metBuff.getDouble(offsetsMap.get(metric)));
        break;
    }
    return ret;
  }

  /**
  *
  * @param dimension
  * @param dimBuff
  * @return
  */

  public static int[] extractDicIdFromDimByteBuffFor(String dimension, IntBuffer dimBuff, Schema dataSchema) {

    int ret[] = null;
    int dimIndex = dataSchema.getDimensionNames().indexOf(dimension);
    int start = dimBuff.get(dimIndex);
    int end = dimBuff.get((dimIndex + 1));

    ret = new int[end - start];

    int counter = 0;
    for (int i = start; i < end; i++) {
      ret[counter] = dimBuff.get(i);
    }
    return ret;
  }

  public static ByteBuffer append(Object entry, FieldSpec spec, ByteBuffer buff) {
    switch (spec.getDataType()) {
      case INT:
        buff.putInt((Integer) entry);
        break;
      case LONG:
        buff.putLong((Long) entry);
        break;
      case FLOAT:
        buff.putFloat((Float) entry);
        break;
      case DOUBLE:
        buff.putDouble((Double) entry);
        break;
    }
    return buff;
  }

  public static ByteBuffer addTwoMetricBuffs(Schema schema, ByteBuffer one, ByteBuffer two) {
    ByteBuffer oneCopy = one.duplicate();
    ByteBuffer twoCopy = two.duplicate();
    oneCopy.rewind();
    twoCopy.rewind();
    ByteBuffer ret = ByteBuffer.allocate(computeMetricsBuffAllocationSize(schema));
    for (String metricName : schema.getMetricNames()) {
      switch (schema.getFieldSpecFor(metricName).getDataType()) {
        case INT:
          ret.putInt(oneCopy.getInt() + twoCopy.getInt());
          break;
        case FLOAT:
          ret.putFloat(oneCopy.getFloat() + twoCopy.getFloat());
          break;
        case LONG:
          ret.putLong(oneCopy.getLong() + twoCopy.getLong());
          break;
        case DOUBLE:
          ret.putDouble(oneCopy.getDouble() + twoCopy.getDouble());
          break;
      }
    }
    ret.rewind();
    return ret;
  }

  public static int computeMetricsBuffAllocationSize(Schema schema) {
    int ret = 0;
    for (String metricName : schema.getMetricNames()) {
      switch (schema.getFieldSpecFor(metricName).getDataType()) {
        case INT:
          ret += Integer.SIZE / Byte.SIZE;
          break;
        case FLOAT:
          ret += Float.SIZE / Byte.SIZE;
          break;
        case LONG:
          ret += Long.SIZE / Byte.SIZE;
          break;
        case DOUBLE:
          ret += Double.SIZE / Byte.SIZE;
          break;
        default:
          break;
      }
    }
    return ret;
  }

  private static int computeMetricReadOffsetFor(String metric, Schema schema) {
    int offset = 0;

    for (String metricName : schema.getMetricNames()) {
      if (metricName.equals(metric)) {
        return offset;
      }
      switch (schema.getFieldSpecFor(metricName).getDataType()) {
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
    return -1;
  }

  @SuppressWarnings("incomplete-switch")
  public static Object readMetricValueFor(String metric, Schema schema, ByteBuffer metBuff) {
    ByteBuffer shallowCopiedMetricBuffer = metBuff.duplicate();
    int offset = computeMetricReadOffsetFor(metric, schema);
    Object ret = null;

    switch (schema.getFieldSpecFor(metric).getDataType()) {
      case INT:
        ret = new Integer(shallowCopiedMetricBuffer.getInt(offset));
        break;
      case FLOAT:
        ret = new Float(shallowCopiedMetricBuffer.getFloat(offset));
        break;
      case LONG:
        ret = new Long(shallowCopiedMetricBuffer.getLong(offset));
        break;
      case DOUBLE:
        ret = new Double(shallowCopiedMetricBuffer.getDouble(offset));
        break;
    }
    return ret;
  }

  public Object extractMetricValueFrom(Schema dataSchema, String metric, ByteBuffer metBuff,
      Map<String, Integer> metricOffsetMap) {
    Object ret = null;

    switch (dataSchema.getFieldSpecFor(metric).getDataType()) {
      case INT:
        ret = new Integer(metBuff.getInt(metricOffsetMap.get(metric)));
        break;
      case FLOAT:
        ret = new Float(metBuff.getFloat(metricOffsetMap.get(metric)));
        break;
      case LONG:
        ret = new Long(metBuff.getLong(metricOffsetMap.get(metric)));
        break;
      case DOUBLE:
        ret = new Double(metBuff.getDouble(metricOffsetMap.get(metric)));
        break;
    }
    return ret;
  }
}
