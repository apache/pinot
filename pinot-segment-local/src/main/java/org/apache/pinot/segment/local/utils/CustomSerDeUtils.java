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
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import com.google.common.primitives.Longs;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummaryDeserializer;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.segment.local.customobject.QuantileDigest;
import org.roaringbitmap.RoaringBitmap;

import static java.nio.charset.StandardCharsets.UTF_8;


public class CustomSerDeUtils {

  private CustomSerDeUtils() {
  }

  /**
   * Serializer/De-serializer for a specific type of object.
   *
   * @param <T> Type of the object
   */
  public interface ObjectSerDe<T> {

    /**
     * Serializes a value into a byte array.
     */
    byte[] serialize(T value);

    /**
     * De-serializes a value from a byte array.
     */
    T deserialize(byte[] bytes);

    /**
     * De-serializes a value from a byte buffer.
     */
    T deserialize(ByteBuffer byteBuffer);
  }

  public static final ObjectSerDe<String> STRING_SER_DE = new ObjectSerDe<String>() {

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

  public static final ObjectSerDe<Long> LONG_SER_DE = new ObjectSerDe<Long>() {

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

  public static final ObjectSerDe<Double> DOUBLE_SER_DE = new ObjectSerDe<Double>() {

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

  public static final ObjectSerDe<AvgPair> AVG_PAIR_SER_DE = new ObjectSerDe<AvgPair>() {

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

  public static final ObjectSerDe<MinMaxRangePair> MIN_MAX_RANGE_PAIR_SER_DE = new ObjectSerDe<MinMaxRangePair>() {

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

  public static final ObjectSerDe<HyperLogLog> HYPER_LOG_LOG_SER_DE = new ObjectSerDe<HyperLogLog>() {

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

  public static final ObjectSerDe<HyperLogLogPlus> HYPER_LOG_LOG_PLUS_SER_DE = new ObjectSerDe<HyperLogLogPlus>() {

    @Override
    public byte[] serialize(HyperLogLogPlus hyperLogLogPlus) {
      try {
        return hyperLogLogPlus.getBytes();
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while serializing HyperLogLogPlus", e);
      }
    }

    @Override
    public HyperLogLogPlus deserialize(byte[] bytes) {
      try {
        return HyperLogLogPlus.Builder.build(bytes);
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while de-serializing HyperLogLogPlus", e);
      }
    }

    @Override
    public HyperLogLogPlus deserialize(ByteBuffer byteBuffer) {
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return deserialize(bytes);
    }
  };

  public static final ObjectSerDe<TDigest> TDIGEST_SER_DE = new ObjectSerDe<TDigest>() {

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

  public static final ObjectSerDe<Sketch> DATA_SKETCH_THETA_SER_DE = new ObjectSerDe<Sketch>() {

    @Override
    public byte[] serialize(Sketch value) {
      // The serializer should respect existing ordering to enable "early stop"
      // optimisations on unions.
      if (!value.isCompact()) {
        return value.compact(value.isOrdered(), null).toByteArray();
      }
      return value.toByteArray();
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

  public static final ObjectSerDe<org.apache.datasketches.tuple.Sketch<IntegerSummary>> DATA_SKETCH_INT_TUPLE_SER_DE =
      new ObjectSerDe<org.apache.datasketches.tuple.Sketch<IntegerSummary>>() {
        @Override
        public byte[] serialize(org.apache.datasketches.tuple.Sketch<IntegerSummary> value) {
          return value.compact().toByteArray();
        }

        @Override
        public org.apache.datasketches.tuple.Sketch<IntegerSummary> deserialize(byte[] bytes) {
          return org.apache.datasketches.tuple.Sketches.heapifySketch(Memory.wrap(bytes),
              new IntegerSummaryDeserializer());
        }

        @Override
        public org.apache.datasketches.tuple.Sketch<IntegerSummary> deserialize(ByteBuffer byteBuffer) {
          byte[] bytes = new byte[byteBuffer.remaining()];
          byteBuffer.get(bytes);
          return org.apache.datasketches.tuple.Sketches.heapifySketch(Memory.wrap(bytes),
              new IntegerSummaryDeserializer());
        }
      };

  public static final ObjectSerDe<CpcSketch> DATA_SKETCH_CPC_SER_DE = new ObjectSerDe<CpcSketch>() {

    @Override
    public byte[] serialize(CpcSketch value) {
      return value.toByteArray();
    }

    @Override
    public CpcSketch deserialize(byte[] bytes) {
      return CpcSketch.heapify(Memory.wrap(bytes));
    }

    @Override
    public CpcSketch deserialize(ByteBuffer byteBuffer) {
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return CpcSketch.heapify(Memory.wrap(bytes));
    }
  };

  public static final ObjectSerDe<RoaringBitmap> ROARING_BITMAP_SER_DE = new ObjectSerDe<RoaringBitmap>() {

    @Override
    public byte[] serialize(RoaringBitmap bitmap) {
      byte[] bytes = new byte[bitmap.serializedSizeInBytes()];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      bitmap.serialize(byteBuffer);
      return bytes;
    }

    @Override
    public RoaringBitmap deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public RoaringBitmap deserialize(ByteBuffer byteBuffer) {
      RoaringBitmap bitmap = new RoaringBitmap();
      try {
        bitmap.deserialize(byteBuffer);
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while deserializing RoaringBitmap", e);
      }
      return bitmap;
    }
  };

  public static final ObjectSerDe<QuantileDigest> QUANTILE_DIGEST_SER_DE = new ObjectSerDe<QuantileDigest>() {

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

  public static final ObjectSerDe<UltraLogLog> ULTRA_LOG_LOG_OBJECT_SER_DE = new ObjectSerDe<UltraLogLog>() {

    @Override
    public byte[] serialize(UltraLogLog value) {
      ByteBuffer buff = ByteBuffer.wrap(new byte[(1 << value.getP()) + 1]);
      buff.put((byte) value.getP());
      buff.put(value.getState());
      return buff.array();
    }

    @Override
    public UltraLogLog deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public UltraLogLog deserialize(ByteBuffer byteBuffer) {
      byte p = byteBuffer.get();
      byte[] state = new byte[1 << p];
      byteBuffer.get(state);
      return UltraLogLog.wrap(state);
    }
  };
}
