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
package org.apache.pinot.segment.local.utils;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;


public class ArraySerDeUtils {
  private ArraySerDeUtils() {
  }

  public static byte[] serializeIntArrayWithLength(int[] values) {
    byte[] bytes = new byte[Integer.BYTES + values.length * Integer.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putInt(values.length);
    writeValues(byteBuffer, values);
    return bytes;
  }

  public static int[] deserializeIntArrayWithLength(byte[] bytes) {
    return deserializeIntArrayWithLength(ByteBuffer.wrap(bytes));
  }

  public static int[] deserializeIntArrayWithLength(ByteBuffer byteBuffer) {
    int length = byteBuffer.getInt();
    int[] values = new int[length];
    readValues(byteBuffer, values, length);
    return values;
  }

  public static int deserializeIntArrayWithLength(byte[] bytes, int[] values) {
    return deserializeIntArrayWithLength(ByteBuffer.wrap(bytes), values);
  }

  public static int deserializeIntArrayWithLength(ByteBuffer byteBuffer, int[] values) {
    int length = byteBuffer.getInt();
    readValues(byteBuffer, values, length);
    return length;
  }

  public static byte[] serializeIntArrayWithoutLength(int[] values) {
    byte[] bytes = new byte[values.length * Integer.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    writeValues(byteBuffer, values);
    return bytes;
  }

  public static int[] deserializeIntArrayWithoutLength(byte[] bytes) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int length = bytes.length / Integer.BYTES;
    int[] values = new int[length];
    readValues(byteBuffer, values, length);
    return values;
  }

  public static int deserializeIntArrayWithoutLength(byte[] bytes, int[] values) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int length = bytes.length / Integer.BYTES;
    readValues(byteBuffer, values, length);
    return length;
  }

  private static void writeValues(ByteBuffer byteBuffer, int[] values) {
    for (int value : values) {
      byteBuffer.putInt(value);
    }
  }

  private static void readValues(ByteBuffer byteBuffer, int[] values, int length) {
    for (int i = 0; i < length; i++) {
      values[i] = byteBuffer.getInt();
    }
  }

  public static byte[] serializeLongArrayWithLength(long[] values) {
    byte[] bytes = new byte[Integer.BYTES + values.length * Long.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putInt(values.length);
    writeValues(byteBuffer, values);
    return bytes;
  }

  public static long[] deserializeLongArrayWithLength(byte[] bytes) {
    return deserializeLongArrayWithLength(ByteBuffer.wrap(bytes));
  }

  public static long[] deserializeLongArrayWithLength(ByteBuffer byteBuffer) {
    int length = byteBuffer.getInt();
    long[] values = new long[length];
    readValues(byteBuffer, values, length);
    return values;
  }

  public static int deserializeLongArrayWithLength(byte[] bytes, long[] values) {
    return deserializeLongArrayWithLength(ByteBuffer.wrap(bytes), values);
  }

  public static int deserializeLongArrayWithLength(ByteBuffer byteBuffer, long[] values) {
    int length = byteBuffer.getInt();
    readValues(byteBuffer, values, length);
    return length;
  }

  public static byte[] serializeLongArrayWithoutLength(long[] values) {
    byte[] bytes = new byte[values.length * Long.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    writeValues(byteBuffer, values);
    return bytes;
  }

  public static long[] deserializeLongArrayWithoutLength(byte[] bytes) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int length = bytes.length / Long.BYTES;
    long[] values = new long[length];
    readValues(byteBuffer, values, length);
    return values;
  }

  public static int deserializeLongArrayWithoutLength(byte[] bytes, long[] values) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int length = bytes.length / Long.BYTES;
    readValues(byteBuffer, values, length);
    return length;
  }

  private static void writeValues(ByteBuffer byteBuffer, long[] values) {
    for (long value : values) {
      byteBuffer.putLong(value);
    }
  }

  private static void readValues(ByteBuffer byteBuffer, long[] values, int length) {
    for (int i = 0; i < length; i++) {
      values[i] = byteBuffer.getLong();
    }
  }

  public static byte[] serializeFloatArrayWithLength(float[] values) {
    byte[] bytes = new byte[Integer.BYTES + values.length * Float.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putInt(values.length);
    writeValues(byteBuffer, values);
    return bytes;
  }

  public static float[] deserializeFloatArrayWithLength(byte[] bytes) {
    return deserializeFloatArrayWithLength(ByteBuffer.wrap(bytes));
  }

  public static float[] deserializeFloatArrayWithLength(ByteBuffer byteBuffer) {
    int length = byteBuffer.getInt();
    float[] values = new float[length];
    readValues(byteBuffer, values, length);
    return values;
  }

  public static int deserializeFloatArrayWithLength(byte[] bytes, float[] values) {
    return deserializeFloatArrayWithLength(ByteBuffer.wrap(bytes), values);
  }

  public static int deserializeFloatArrayWithLength(ByteBuffer byteBuffer, float[] values) {
    int length = byteBuffer.getInt();
    readValues(byteBuffer, values, length);
    return length;
  }

  public static byte[] serializeFloatArrayWithoutLength(float[] values) {
    byte[] bytes = new byte[values.length * Float.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    writeValues(byteBuffer, values);
    return bytes;
  }

  public static float[] deserializeFloatArrayWithoutLength(byte[] bytes) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int length = bytes.length / Float.BYTES;
    float[] values = new float[length];
    readValues(byteBuffer, values, length);
    return values;
  }

  public static int deserializeFloatArrayWithoutLength(byte[] bytes, float[] values) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int length = bytes.length / Float.BYTES;
    readValues(byteBuffer, values, length);
    return length;
  }

  private static void writeValues(ByteBuffer byteBuffer, float[] values) {
    for (float value : values) {
      byteBuffer.putFloat(value);
    }
  }

  private static void readValues(ByteBuffer byteBuffer, float[] values, int length) {
    for (int i = 0; i < length; i++) {
      values[i] = byteBuffer.getFloat();
    }
  }

  public static byte[] serializeDoubleArrayWithLength(double[] values) {
    byte[] bytes = new byte[Integer.BYTES + values.length * Double.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putInt(values.length);
    writeValues(byteBuffer, values);
    return bytes;
  }

  public static double[] deserializeDoubleArrayWithLength(byte[] bytes) {
    return deserializeDoubleArrayWithLength(ByteBuffer.wrap(bytes));
  }

  public static double[] deserializeDoubleArrayWithLength(ByteBuffer byteBuffer) {
    int length = byteBuffer.getInt();
    double[] values = new double[length];
    readValues(byteBuffer, values, length);
    return values;
  }

  public static int deserializeDoubleArrayWithLength(byte[] bytes, double[] values) {
    return deserializeDoubleArrayWithLength(ByteBuffer.wrap(bytes), values);
  }

  public static int deserializeDoubleArrayWithLength(ByteBuffer byteBuffer, double[] values) {
    int length = byteBuffer.getInt();
    readValues(byteBuffer, values, length);
    return length;
  }

  public static byte[] serializeDoubleArrayWithoutLength(double[] values) {
    byte[] bytes = new byte[values.length * Double.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    writeValues(byteBuffer, values);
    return bytes;
  }

  public static double[] deserializeDoubleArrayWithoutLength(byte[] bytes) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int length = bytes.length / Double.BYTES;
    double[] values = new double[length];
    readValues(byteBuffer, values, length);
    return values;
  }

  public static int deserializeDoubleArrayWithoutLength(byte[] bytes, double[] values) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int length = bytes.length / Double.BYTES;
    readValues(byteBuffer, values, length);
    return length;
  }

  private static void writeValues(ByteBuffer byteBuffer, double[] values) {
    for (double value : values) {
      byteBuffer.putDouble(value);
    }
  }

  private static void readValues(ByteBuffer byteBuffer, double[] values, int length) {
    for (int i = 0; i < length; i++) {
      values[i] = byteBuffer.getDouble();
    }
  }

  public static byte[] serializeStringArray(String[] values) {
    // Format: [numValues][length1][length2]...[lengthN][value1][value2]...[valueN]
    int headerSize = Integer.BYTES + Integer.BYTES * values.length;
    int size = headerSize;
    byte[][] stringBytes = new byte[values.length][];
    for (int i = 0; i < values.length; i++) {
      stringBytes[i] = values[i].getBytes(UTF_8);
      size += stringBytes[i].length;
    }
    return writeValues(stringBytes, size, headerSize);
  }

  public static String[] deserializeStringArray(byte[] bytes) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int numValues = byteBuffer.getInt();
    String[] values = new String[numValues];
    readValues(bytes, byteBuffer, values, numValues);
    return values;
  }

  public static int deserializeStringArray(byte[] bytes, String[] values) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int numValues = byteBuffer.getInt();
    readValues(bytes, byteBuffer, values, numValues);
    return numValues;
  }

  public static byte[] serializeBytesArray(byte[][] values) {
    // Format: [numValues][length1][length2]...[lengthN][value1][value2]...[valueN]
    int headerSize = Integer.BYTES + Integer.BYTES * values.length;
    int size = headerSize;
    for (byte[] value : values) {
      size += value.length;
    }
    return writeValues(values, size, headerSize);
  }

  public static byte[][] deserializeBytesArray(byte[] bytes) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int numValues = byteBuffer.getInt();
    byte[][] values = new byte[numValues][];
    readValues(byteBuffer, values, numValues);
    return values;
  }

  public static int deserializeBytesArray(byte[] bytes, byte[][] values) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int numValues = byteBuffer.getInt();
    readValues(byteBuffer, values, numValues);
    return numValues;
  }

  private static byte[] writeValues(byte[][] values, int size, int headerSize) {
    byte[] bytes = new byte[size];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putInt(values.length);
    byteBuffer.position(headerSize);
    for (int i = 0; i < values.length; i++) {
      byteBuffer.putInt((i + 1) * Integer.BYTES, values[i].length);
      byteBuffer.put(values[i]);
    }
    return bytes;
  }

  private static void readValues(byte[] bytes, ByteBuffer byteBuffer, String[] values, int numValues) {
    int offset = (numValues + 1) * Integer.BYTES;
    for (int i = 0; i < numValues; i++) {
      int length = byteBuffer.getInt();
      values[i] = new String(bytes, offset, length, UTF_8);
      offset += length;
    }
  }

  private static void readValues(ByteBuffer byteBuffer, byte[][] values, int numValues) {
    byteBuffer.position((numValues + 1) * Integer.BYTES);
    for (int i = 0; i < numValues; i++) {
      int length = byteBuffer.getInt((i + 1) * Integer.BYTES);
      values[i] = new byte[length];
      byteBuffer.get(values[i]);
    }
  }
}
