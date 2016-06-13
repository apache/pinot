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
package com.linkedin.pinot.core.realtime.impl.fwdindex;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.HashUtil;


/**
 *
 * This class holds a unique dimension set entry (d1....dn) as a ByteBuffer
 * it also has a map of time value to metric set (m1....mn) as a ByteBuffer
 *
 */
public class DimensionTuple {

  private final ByteBuffer dimesionIntBuffer;
  private final long hash64;
  private final Map<Object, ByteBuffer> timeToMetricsBuffMap;

  public DimensionTuple(ByteBuffer buff) {
    this.dimesionIntBuffer = buff;
    hash64 = HashUtil.compute(buff);
    this.timeToMetricsBuffMap = new HashMap<Object, ByteBuffer>();
  }

  public DimensionTuple(ByteBuffer buff, long hash64) {
    this.dimesionIntBuffer = buff;
    this.hash64 = hash64;
    this.timeToMetricsBuffMap = new HashMap<Object, ByteBuffer>();
  }

  public long getHashValue() {
    return hash64;
  }

  public boolean containsTime(Object time) {
    return timeToMetricsBuffMap.containsKey(time);
  }

  public ByteBuffer getDimBuff() {
    return dimesionIntBuffer;
  }

  public void addMetricsbuffFor(Object time, ByteBuffer metricsBuff, Schema schema) {
    if (timeToMetricsBuffMap.containsKey(time)) {
      ByteBuffer addedMetricsBuff =
          ByteBufferUtils.addTwoMetricBuffs(schema, timeToMetricsBuffMap.get(time), metricsBuff);
      timeToMetricsBuffMap.put(time, addedMetricsBuff);
      return;
    }
    metricsBuff.rewind();
    timeToMetricsBuffMap.put(time, metricsBuff);
  }

  public ByteBuffer getMetricsBuffForTime(Object time) {
    return timeToMetricsBuffMap.get(time);
  }
}
