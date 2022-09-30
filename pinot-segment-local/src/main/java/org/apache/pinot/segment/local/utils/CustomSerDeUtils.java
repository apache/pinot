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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.primitives.Longs;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.pinot.common.ObjectSerDeUtils;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.segment.local.customobject.QuantileDigest;

import static java.nio.charset.StandardCharsets.UTF_8;


public class CustomSerDeUtils {

  private CustomSerDeUtils() {
  }

  public static final ObjectSerDeUtils.ObjectSerDe<String> STRING_SER_DE = new ObjectSerDeUtils.ObjectSerDe<String>() {

    @Override
    public byte[] serialize(String value) {
      return value.getBytes(UTF_8);
    }

    @Override
    public String deserialize(byte[] bytes) {
      return new String(bytes);
    }

    @Override
    public String deserialize(ByteBuffer byteBuffer) {
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return new String(bytes, UTF_8);
    }
  };

  public static final ObjectSerDeUtils.ObjectSerDe<Long> LONG_SER_DE = new ObjectSerDeUtils.ObjectSerDe<Long>() {

    @Override
    public byte[] serialize(Long value) {
      return Longs.toByteArray(value);
    }

    @Override
    public Long deserialize(byte[] bytes) {
      return Longs.fromByteArray(bytes);
    }

    @Override
    public Long deserialize(ByteBuffer byteBuffer) {
      return byteBuffer.getLong();
    }
  };

  public static final ObjectSerDeUtils.ObjectSerDe<Double> DOUBLE_SER_DE = new ObjectSerDeUtils.ObjectSerDe<Double>() {

    @Override
    public byte[] serialize(Double value) {
      return Longs.toByteArray(Double.doubleToRawLongBits(value));
    }

    @Override
    public Double deserialize(byte[] bytes) {
      return Double.longBitsToDouble(Longs.fromByteArray(bytes));
    }

    @Override
    public Double deserialize(ByteBuffer byteBuffer) {
      return byteBuffer.getDouble();
    }
  };

  public static final ObjectSerDeUtils.ObjectSerDe<AvgPair> AVG_PAIR_SER_DE =
      new ObjectSerDeUtils.ObjectSerDe<AvgPair>() {

        @Override
        public byte[] serialize(AvgPair avgPair) {
          return avgPair.toBytes();
        }

        @Override
        public AvgPair deserialize(byte[] bytes) {
          return AvgPair.fromBytes(bytes);
        }

        @Override
        public AvgPair deserialize(ByteBuffer byteBuffer) {
          return AvgPair.fromByteBuffer(byteBuffer);
        }
      };

  public static final ObjectSerDeUtils.ObjectSerDe<MinMaxRangePair> MIN_MAX_RANGE_PAIR_SER_DE =
      new ObjectSerDeUtils.ObjectSerDe<MinMaxRangePair>() {

        @Override
        public byte[] serialize(MinMaxRangePair minMaxRangePair) {
          return minMaxRangePair.toBytes();
        }

        @Override
        public MinMaxRangePair deserialize(byte[] bytes) {
          return MinMaxRangePair.fromBytes(bytes);
        }

        @Override
        public MinMaxRangePair deserialize(ByteBuffer byteBuffer) {
          return MinMaxRangePair.fromByteBuffer(byteBuffer);
        }
      };

  public static final ObjectSerDeUtils.ObjectSerDe<HyperLogLog> HYPER_LOG_LOG_SER_DE =
      new ObjectSerDeUtils.ObjectSerDe<HyperLogLog>() {

        @Override
        public byte[] serialize(HyperLogLog hyperLogLog) {
          try {
            return hyperLogLog.getBytes();
          } catch (IOException e) {
            throw new RuntimeException("Caught exception while serializing HyperLogLog", e);
          }
        }

        @Override
        public HyperLogLog deserialize(byte[] bytes) {
          try {
            return HyperLogLog.Builder.build(bytes);
          } catch (IOException e) {
            throw new RuntimeException("Caught exception while de-serializing HyperLogLog", e);
          }
        }

        @Override
        public HyperLogLog deserialize(ByteBuffer byteBuffer) {
          byte[] bytes = new byte[byteBuffer.remaining()];
          byteBuffer.get(bytes);
          try {
            return HyperLogLog.Builder.build(bytes);
          } catch (IOException e) {
            throw new RuntimeException("Caught exception while de-serializing HyperLogLog", e);
          }
        }
      };

  public static final ObjectSerDeUtils.ObjectSerDe<TDigest> TDIGEST_SER_DE =
      new ObjectSerDeUtils.ObjectSerDe<TDigest>() {

        @Override
        public byte[] serialize(TDigest tDigest) {
          byte[] bytes = new byte[tDigest.byteSize()];
          tDigest.asBytes(ByteBuffer.wrap(bytes));
          return bytes;
        }

        @Override
        public TDigest deserialize(byte[] bytes) {
          return MergingDigest.fromBytes(ByteBuffer.wrap(bytes));
        }

        @Override
        public TDigest deserialize(ByteBuffer byteBuffer) {
          return MergingDigest.fromBytes(byteBuffer);
        }
      };

  public static final ObjectSerDeUtils.ObjectSerDe<Sketch> DATA_SKETCH_SER_DE =
      new ObjectSerDeUtils.ObjectSerDe<Sketch>() {

        @Override
        public byte[] serialize(Sketch value) {
          // NOTE: Compact the sketch in unsorted, on-heap fashion for performance concern.
          //       See https://datasketches.apache.org/docs/Theta/ThetaSize.html for more details.
          return value.compact(false, null).toByteArray();
        }

        @Override
        public Sketch deserialize(byte[] bytes) {
          return Sketch.wrap(Memory.wrap(bytes));
        }

        @Override
        public Sketch deserialize(ByteBuffer byteBuffer) {
          byte[] bytes = new byte[byteBuffer.remaining()];
          byteBuffer.get(bytes);
          return Sketch.wrap(Memory.wrap(bytes));
        }
      };

  public static final ObjectSerDeUtils.ObjectSerDe<QuantileDigest> QUANTILE_DIGEST_SER_DE =
      new ObjectSerDeUtils.ObjectSerDe<QuantileDigest>() {

        @Override
        public byte[] serialize(QuantileDigest quantileDigest) {
          return quantileDigest.toBytes();
        }

        @Override
        public QuantileDigest deserialize(byte[] bytes) {
          return QuantileDigest.fromBytes(bytes);
        }

        @Override
        public QuantileDigest deserialize(ByteBuffer byteBuffer) {
          return QuantileDigest.fromByteBuffer(byteBuffer);
        }
      };
}
