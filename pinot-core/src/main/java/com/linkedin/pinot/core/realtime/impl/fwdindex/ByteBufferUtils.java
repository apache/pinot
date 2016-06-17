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

import com.linkedin.pinot.common.data.Schema;


public class ByteBufferUtils {

  /**
   *
   * @param schema
   * @return
   */
  public static int computeMetricsBuffAllocationSize(Schema schema) {
    int metricBuffSizeInBytes = 0;
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
    return metricBuffSizeInBytes;
  }

  /**
   *
   * @param schema
   * @param one
   * @param two
   * @return
   */
  public static ByteBuffer addTwoMetricBuffs(Schema schema, ByteBuffer one, ByteBuffer two) {
    ByteBuffer oneCopy = one.duplicate();
    ByteBuffer twoCopy = two.duplicate();
    oneCopy.rewind();
    twoCopy.rewind();
    ByteBuffer ret = ByteBuffer.allocate(oneCopy.array().length);
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
}
