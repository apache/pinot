/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.util;

import java.math.BigDecimal;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * The class <code>ArrayCopyUtils</code> provides methods to copy values across arrays of different types.
 */
public class ArrayCopyUtils {
  private ArrayCopyUtils() {
  }

  public static void copy(int[] src, int[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = src[i];
    }
  }

  public static void copy(int[] src, long[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = src[i];
    }
  }

  public static void copy(int[] src, float[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = src[i];
    }
  }

  public static void copy(int[] src, double[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = src[i];
    }
  }

  public static void copy(int[] src, String[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = Integer.toString(src[i]);
    }
  }

  public static void copy(long[] src, int[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = (int) src[i];
    }
  }

  public static void copy(long[] src, float[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = src[i];
    }
  }

  public static void copy(long[] src, double[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = src[i];
    }
  }

  public static void copy(long[] src, String[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = Long.toString(src[i]);
    }
  }

  public static void copy(float[] src, int[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = (int) src[i];
    }
  }

  public static void copy(float[] src, long[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = (long) src[i];
    }
  }

  public static void copy(float[] src, double[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = src[i];
    }
  }

  public static void copy(float[] src, String[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = Float.toString(src[i]);
    }
  }

  public static void copy(double[] src, int[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = (int) src[i];
    }
  }

  public static void copy(double[] src, long[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = (long) src[i];
    }
  }

  public static void copy(double[] src, float[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = (float) src[i];
    }
  }

  public static void copy(double[] src, String[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = Double.toString(src[i]);
    }
  }

  public static void copy(String[] src, int[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = Double.valueOf(src[i]).intValue();
    }
  }

  public static void copy(String[] src, long[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = new BigDecimal(src[i]).longValue();
    }
  }

  public static void copy(String[] src, float[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = Float.parseFloat(src[i]);
    }
  }

  public static void copy(String[] src, double[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = Double.parseDouble(src[i]);
    }
  }

  public static void copy(String[] src, byte[][] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = BytesUtils.toBytes(src[i]);
    }
  }

  public static void copy(byte[][] src, String[] dest, int length) {
    for (int i = 0; i < length; i++) {
      dest[i] = BytesUtils.toHexString(src[i]);
    }
  }
}
