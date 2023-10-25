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
package org.apache.pinot.spi.utils;

/**
 * Utility class for vector operations.
 */
public class VectorUtils {

  private VectorUtils() {
  }

  public static float[] fromBytes(byte[] bytes) {
    float[] vector = new float[bytes.length / Float.BYTES];
    for (int i = 0; i < vector.length; i++) {
      vector[i] = Float.intBitsToFloat(
          (bytes[i * Float.BYTES] & 0xFF) << 24 | (bytes[i * Float.BYTES + 1] & 0xFF) << 16
              | (bytes[i * Float.BYTES + 2] & 0xFF) << 8 | (bytes[i * Float.BYTES + 3] & 0xFF));
    }
    return vector;
  }

  public static byte[] toBytes(float[] vector) {
    byte[] bytes = new byte[vector.length * Float.BYTES];
    for (int i = 0; i < vector.length; i++) {
      int intBits = Float.floatToIntBits(vector[i]);
      bytes[i * Float.BYTES] = (byte) (intBits >> 24);
      bytes[i * Float.BYTES + 1] = (byte) (intBits >> 16);
      bytes[i * Float.BYTES + 2] = (byte) (intBits >> 8);
      bytes[i * Float.BYTES + 3] = (byte) intBits;
    }
    return bytes;
  }
}
