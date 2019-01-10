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
package com.linkedin.thirdeye.hadoop.config;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Represents the various data types supported for a metric<br/>
 * Currently we support INT, SHORT, LONG, FLOAT, DOUBLE
 */
public enum MetricType {

  INT {
    public Number toNumber(String s) {
      return Integer.parseInt(s);
    }

    public int byteSize() {
      return 4;
    }

    @Override
    public Number getDefaultNullValue() {
      return ThirdEyeConstants.EMPTY_INT;
    }

  },
  SHORT {
    public Number toNumber(String s) {
      return Short.parseShort(s);
    }

    public int byteSize() {
      return 2;

    }

    @Override
    public Number getDefaultNullValue() {
      return ThirdEyeConstants.EMPTY_SHORT;
    }

  },
  LONG {
    public Number toNumber(String s) {
      return Long.parseLong(s);
    }

    public int byteSize() {
      return 8;

    }

    @Override
    public Number getDefaultNullValue() {
      return ThirdEyeConstants.EMPTY_LONG;
    }

  },
  FLOAT {
    public Number toNumber(String s) {
      return Float.parseFloat(s);
    }

    public int byteSize() {
      return 4;

    }

    @Override
    public Number getDefaultNullValue() {
      return ThirdEyeConstants.EMPTY_FLOAT;
    }

  },
  DOUBLE {
    public Number toNumber(String s) {
      return Double.parseDouble(s);
    }

    public int byteSize() {
      return 8;
    }

    @Override
    public Number getDefaultNullValue() {
      return ThirdEyeConstants.EMPTY_DOUBLE;
    }
  };

  public Number toNumber(String s) {
    throw new AbstractMethodError();
  }

  public int byteSize() {
    throw new AbstractMethodError();
  }

  public abstract Number getDefaultNullValue();

  /**
   * Writes a metric value to a data output stream
   * @param dos
   * @param number
   * @param metricType
   * @throws IOException
   */
  public static void writeMetricValueToDataOutputStream(DataOutputStream dos, Number number, MetricType metricType) throws IOException {
    switch (metricType) {
    case SHORT:
      dos.writeShort(number.intValue());
      break;
    case LONG:
      dos.writeLong(number.longValue());
      break;
    case INT:
      dos.writeInt(number.intValue());
      break;
    case FLOAT:
      dos.writeFloat(number.floatValue());
      break;
    case DOUBLE:
      dos.writeDouble(number.doubleValue());
      break;
    default:
      throw new IllegalArgumentException("Unsupported metricType " + metricType);
    }
  }

  /**
   * Reads a metric value from a data input stream
   * @param dis
   * @param metricType
   * @return
   * @throws IOException
   */
  public static Number readMetricValueFromDataInputStream(DataInputStream dis, MetricType metricType) throws IOException {
    Number metricValue = null;
    switch (metricType) {
    case SHORT:
      metricValue = dis.readShort();
      break;
    case LONG:
      metricValue = dis.readLong();
      break;
    case INT:
      metricValue = dis.readInt();
      break;
    case FLOAT:
      metricValue = dis.readFloat();
      break;
    case DOUBLE:
      metricValue = dis.readDouble();
      break;
    default:
      throw new IllegalArgumentException("Unsupported metricType " + metricType);
    }
    return metricValue;
  }
}
