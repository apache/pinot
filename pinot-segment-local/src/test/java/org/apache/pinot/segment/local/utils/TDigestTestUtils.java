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
import java.util.Arrays;
import org.apache.pinot.segment.local.customobject.tdigest.TDigest;
import org.apache.pinot.segment.local.customobject.tdigest.TDigestCodec;


/// Builds legacy t-digest test fixtures without exposing compact writing in the production API.
/// This utility has no shared state; callers must exclusively own the digest being serialized.
public final class TDigestTestUtils {
  private TDigestTestUtils() {
  }

  public static byte[] verbose(double[] means, double[] weights) {
    return verbose(means, weights, 100.0);
  }

  public static byte[] verbose(double[] means, double[] weights, double compression) {
    return verbose(compression, means[0], means[means.length - 1], means, weights);
  }

  public static byte[] verbose(double compression, double min, double max, double[] means, double[] weights) {
    return legacyBytes(false, compression, min, max, means, weights, 0, 0);
  }

  public static byte[] small(double[] means, double[] weights) {
    return legacyBytes(true, 100, means[0], means[means.length - 1], means, weights, 210, 1050);
  }

  public static byte[] unitCentroids(boolean compact, int count, double compression, int mainCapacity,
      int bufferCapacity) {
    double[] means = new double[count];
    double[] weights = new double[count];
    Arrays.setAll(means, i -> i);
    Arrays.fill(weights, 1.0);
    return legacyBytes(compact, compression, 0, count - 1.0, means, weights, mainCapacity, bufferCapacity);
  }

  // Write the wire fields directly, preserving unusual means, masses, and explicitly supplied capacities.
  private static byte[] legacyBytes(boolean compact, double compression, double min, double max, double[] means,
      double[] weights, int mainCapacity, int bufferCapacity) {
    ByteBuffer bytes = ByteBuffer.allocate((compact ? 30 : 32) + means.length * (compact ? 8 : 16));
    bytes.putInt(compact ? 2 : 1).putDouble(min).putDouble(max);
    if (compact) {
      bytes.putFloat((float) compression).putShort((short) mainCapacity).putShort((short) bufferCapacity)
          .putShort((short) means.length);
    } else {
      bytes.putDouble(compression).putInt(means.length);
    }
    for (int i = 0; i < means.length; i++) {
      if (compact) {
        bytes.putFloat((float) weights[i]).putFloat((float) means[i]);
      } else {
        bytes.putDouble(weights[i]).putDouble(means[i]);
      }
    }
    return bytes.array();
  }

  public static byte[] compactFixture(TDigest digest) {
    byte[] serialized = digest.serialize();
    ByteBuffer input = ByteBuffer.wrap(serialized);
    if (input.getInt() == TDigestCodec.SMALL_ENCODING) {
      return serialized;
    }
    double min = input.getDouble();
    double max = input.getDouble();
    double compression = input.getDouble();
    int count = input.getInt();
    double normalizedCompression = Math.max(compression, 10.0);
    int mainCapacity = Math.max((int) Math.min(Integer.MAX_VALUE,
        Math.ceil(2.0 * normalizedCompression + (normalizedCompression < 30.0 ? 30 : 10))), count);
    long bufferCapacity = 5L * mainCapacity;
    if ((double) (float) compression != compression || count > Short.MAX_VALUE || bufferCapacity > Short.MAX_VALUE) {
      return serialized;
    }
    ByteBuffer output = ByteBuffer.allocate(30 + 8 * count);
    output.putInt(TDigestCodec.SMALL_ENCODING).putDouble(min).putDouble(max).putFloat((float) compression);
    output.putShort((short) mainCapacity).putShort((short) bufferCapacity).putShort((short) count);
    for (int i = 0; i < count; i++) {
      float weight = (float) input.getDouble();
      float mean = (float) input.getDouble();
      if (!(weight > 0.0f) || !Float.isFinite(weight) || !Float.isFinite(mean)) {
        return serialized;
      }
      output.putFloat(weight).putFloat(mean);
    }
    return output.array();
  }
}
