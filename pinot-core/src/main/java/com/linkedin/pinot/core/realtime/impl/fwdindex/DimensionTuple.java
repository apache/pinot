package com.linkedin.pinot.core.realtime.impl.fwdindex;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.HashUtil;


public class DimensionTuple {

  private final IntBuffer dimesionIntBuffer;
  private final long hash64;
  private final Map<Long, ByteBuffer> timeToMetricsBuffMap;

  public DimensionTuple(IntBuffer buff) {
    this.dimesionIntBuffer = buff;
    hash64 = HashUtil.compute(buff);
    this.timeToMetricsBuffMap = new HashMap<Long, ByteBuffer>();
  }

  public DimensionTuple(IntBuffer buff, long hash64) {
    this.dimesionIntBuffer = buff;
    this.hash64 = hash64;
    this.timeToMetricsBuffMap = new HashMap<Long, ByteBuffer>();
  }

  public long getHashValue() {
    return hash64;
  }

  public boolean containsTime(Long time) {
    return timeToMetricsBuffMap.containsKey(time);
  }

  public IntBuffer getDimBuff() {
    return dimesionIntBuffer;
  }

  public void addMetricsbuffFor(Long time, ByteBuffer metricsBuff, Schema schema) {
    if (timeToMetricsBuffMap.containsKey(time)) {
      ByteBuffer addedMetricsBuff =
          ByteBufferUtils.addTwoMetricBuffs(schema, timeToMetricsBuffMap.get(time), metricsBuff);
      timeToMetricsBuffMap.put(time, addedMetricsBuff);
      return;
    }
    metricsBuff.rewind();
    timeToMetricsBuffMap.put(time, metricsBuff);
  }

  public ByteBuffer getMetricsBuffForTime(Long time) {
    return timeToMetricsBuffMap.get(time);
  }

  public Object getMetricValueFor(Long time, Schema schema, String metricName) {
    if (!timeToMetricsBuffMap.containsKey(time))
      return null;

    return ByteBufferUtils.readMetricValueFor(metricName, schema, timeToMetricsBuffMap.get(time));

  }

}
