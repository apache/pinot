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
package org.apache.pinot.core.common;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.primitives.Longs;
import com.google.zetasketch.HyperLogLogPlusPlus;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import it.unimi.dsi.fastutil.doubles.Double2LongMap;
import it.unimi.dsi.fastutil.doubles.Double2LongOpenHashMap;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.Float2LongMap;
import it.unimi.dsi.fastutil.floats.Float2LongOpenHashMap;
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.segment.local.customobject.QuantileDigest;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.StringUtils;
import org.locationtech.jts.geom.Geometry;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code ObjectSerDeUtils} class provides the utility methods to serialize/de-serialize objects.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ObjectSerDeUtils {
  private ObjectSerDeUtils() {
  }

  public enum ObjectType {
    // NOTE: DO NOT change the value, we rely on the value to indicate the object type
    String(0),
    Long(1),
    Double(2),
    DoubleArrayList(3),
    AvgPair(4),
    MinMaxRangePair(5),
    HyperLogLog(6),
    QuantileDigest(7),
    Map(8),
    IntSet(9),
    TDigest(10),
    DistinctTable(11),
    DataSketch(12),
    Geometry(13),
    RoaringBitmap(14),
    LongSet(15),
    FloatSet(16),
    DoubleSet(17),
    StringSet(18),
    BytesSet(19),
    IdSet(20),
    List(21),
    BigDecimal(22),
    Int2LongMap(23),
    Long2LongMap(24),
    Float2LongMap(25),
    Double2LongMap(26),
    HllSketch(27),
    HllPlusPlus(28);
    private final int _value;

    ObjectType(int value) {
      _value = value;
    }

    public int getValue() {
      return _value;
    }

    public static ObjectType getObjectType(Object value) {
      if (value instanceof String) {
        return ObjectType.String;
      } else if (value instanceof Long) {
        return ObjectType.Long;
      } else if (value instanceof Double) {
        return ObjectType.Double;
      } else if (value instanceof BigDecimal) {
        return ObjectType.BigDecimal;
      } else if (value instanceof DoubleArrayList) {
        return ObjectType.DoubleArrayList;
      } else if (value instanceof AvgPair) {
        return ObjectType.AvgPair;
      } else if (value instanceof MinMaxRangePair) {
        return ObjectType.MinMaxRangePair;
      } else if (value instanceof HyperLogLog) {
        return ObjectType.HyperLogLog;
      } else if (value instanceof HllSketch) {
        return ObjectType.HllSketch;
      } else if (value instanceof HyperLogLogPlusPlus) {
        return ObjectType.HllPlusPlus;
      } else if (value instanceof QuantileDigest) {
        return ObjectType.QuantileDigest;
      } else if (value instanceof Int2LongMap) {
        return ObjectType.Int2LongMap;
      } else if (value instanceof Long2LongMap) {
        return ObjectType.Long2LongMap;
      } else if (value instanceof Float2LongMap) {
        return ObjectType.Float2LongMap;
      } else if (value instanceof Double2LongMap) {
        return ObjectType.Double2LongMap;
      } else if (value instanceof Map) {
        return ObjectType.Map;
      } else if (value instanceof IntSet) {
        return ObjectType.IntSet;
      } else if (value instanceof TDigest) {
        return ObjectType.TDigest;
      } else if (value instanceof DistinctTable) {
        return ObjectType.DistinctTable;
      } else if (value instanceof Sketch) {
        return ObjectType.DataSketch;
      } else if (value instanceof Geometry) {
        return ObjectType.Geometry;
      } else if (value instanceof RoaringBitmap) {
        return ObjectType.RoaringBitmap;
      } else if (value instanceof LongSet) {
        return ObjectType.LongSet;
      } else if (value instanceof FloatSet) {
        return ObjectType.FloatSet;
      } else if (value instanceof DoubleSet) {
        return ObjectType.DoubleSet;
      } else if (value instanceof ObjectSet) {
        ObjectSet objectSet = (ObjectSet) value;
        if (objectSet.isEmpty() || objectSet.iterator().next() instanceof String) {
          return ObjectType.StringSet;
        } else {
          return ObjectType.BytesSet;
        }
      } else if (value instanceof IdSet) {
        return ObjectType.IdSet;
      } else if (value instanceof List) {
        return ObjectType.List;
      } else {
        throw new IllegalArgumentException("Unsupported type of value: " + value.getClass().getSimpleName());
      }
    }
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
      return StringUtil.encodeUtf8(value);
    }

    @Override
    public String deserialize(byte[] bytes) {
      return StringUtil.decodeUtf8(bytes);
    }

    @Override
    public String deserialize(ByteBuffer byteBuffer) {
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return StringUtil.decodeUtf8(bytes);
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

  public static final ObjectSerDe<DoubleArrayList> DOUBLE_ARRAY_LIST_SER_DE = new ObjectSerDe<DoubleArrayList>() {

    @Override
    public byte[] serialize(DoubleArrayList doubleArrayList) {
      int size = doubleArrayList.size();
      byte[] bytes = new byte[Integer.BYTES + size * Double.BYTES];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      double[] values = doubleArrayList.elements();
      for (int i = 0; i < size; i++) {
        byteBuffer.putDouble(values[i]);
      }
      return bytes;
    }

    @Override
    public DoubleArrayList deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public DoubleArrayList deserialize(ByteBuffer byteBuffer) {
      int numValues = byteBuffer.getInt();
      DoubleArrayList doubleArrayList = new DoubleArrayList(numValues);
      for (int i = 0; i < numValues; i++) {
        doubleArrayList.add(byteBuffer.getDouble());
      }
      return doubleArrayList;
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

  public static final ObjectSerDe<HyperLogLogPlusPlus> HYPER_LOG_LOG_PLUS_PLUS_SER_DE =
      new ObjectSerDe<HyperLogLogPlusPlus>() {

        @Override
        public byte[] serialize(HyperLogLogPlusPlus hyperLogLogPlusPlus) {
          return hyperLogLogPlusPlus.serializeToByteArray();
        }

        @Override
        public HyperLogLogPlusPlus deserialize(byte[] bytes) {
          return HyperLogLogPlusPlus.forProto(bytes);
        }

        @Override
        public HyperLogLogPlusPlus deserialize(ByteBuffer byteBuffer) {
          byte[] bytes = new byte[byteBuffer.remaining()];
          byteBuffer.get(bytes);
          return HyperLogLogPlusPlus.forProto(bytes);
        }
      };

  public static final ObjectSerDe<HllSketch> HYPER_LOG_LOG_SKETCH_SER_DE = new ObjectSerDe<>() {

    @Override
    public byte[] serialize(HllSketch hyperLogLogPlusPlus) {
      return hyperLogLogPlusPlus.toUpdatableByteArray();
    }

    @Override
    public HllSketch deserialize(byte[] bytes) {
      return HllSketch.heapify(bytes);
    }

    @Override
    public HllSketch deserialize(ByteBuffer byteBuffer) {
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return HllSketch.heapify(bytes);
    }
  };

  public static final ObjectSerDe<DistinctTable> DISTINCT_TABLE_SER_DE = new ObjectSerDe<DistinctTable>() {

    @Override
    public byte[] serialize(DistinctTable distinctTable) {
      try {
        return distinctTable.toBytes();
      } catch (IOException e) {
        throw new IllegalStateException("Caught exception while serializing DistinctTable", e);
      }
    }

    @Override
    public DistinctTable deserialize(byte[] bytes) {
      try {
        return DistinctTable.fromByteBuffer(ByteBuffer.wrap(bytes));
      } catch (IOException e) {
        throw new IllegalStateException("Caught exception while de-serializing DistinctTable", e);
      }
    }

    @Override
    public DistinctTable deserialize(ByteBuffer byteBuffer) {
      try {
        return DistinctTable.fromByteBuffer(byteBuffer);
      } catch (IOException e) {
        throw new IllegalStateException("Caught exception while de-serializing DistinctTable", e);
      }
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

  public static final ObjectSerDe<Map<Object, Object>> MAP_SER_DE = new ObjectSerDe<Map<Object, Object>>() {

    @Override
    public byte[] serialize(Map<Object, Object> map) {
      int size = map.size();

      // Directly return the size (0) for empty map
      if (size == 0) {
        return new byte[Integer.BYTES];
      }

      // No need to close these 2 streams
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

      try {
        // Write the size of the map
        dataOutputStream.writeInt(size);

        // Write the serialized key-value pairs
        Iterator<Map.Entry<Object, Object>> iterator = map.entrySet().iterator();
        // First write the key type and value type
        Map.Entry<Object, Object> firstEntry = iterator.next();
        Object firstKey = firstEntry.getKey();
        Object firstValue = firstEntry.getValue();
        int keyTypeValue = ObjectType.getObjectType(firstKey).getValue();
        int valueTypeValue = ObjectType.getObjectType(firstValue).getValue();
        dataOutputStream.writeInt(keyTypeValue);
        dataOutputStream.writeInt(valueTypeValue);
        // Then write each key-value pair
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          byte[] keyBytes = ObjectSerDeUtils.serialize(entry.getKey(), keyTypeValue);
          dataOutputStream.writeInt(keyBytes.length);
          dataOutputStream.write(keyBytes);

          byte[] valueBytes = ObjectSerDeUtils.serialize(entry.getValue(), valueTypeValue);
          dataOutputStream.writeInt(valueBytes.length);
          dataOutputStream.write(valueBytes);
        }
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while serializing Map", e);
      }

      return byteArrayOutputStream.toByteArray();
    }

    @Override
    public HashMap<Object, Object> deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public HashMap<Object, Object> deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      HashMap<Object, Object> map = new HashMap<>(size);
      if (size == 0) {
        return map;
      }

      // De-serialize each key-value pair
      int keyTypeValue = byteBuffer.getInt();
      int valueTypeValue = byteBuffer.getInt();
      for (int i = 0; i < size; i++) {
        Object key = ObjectSerDeUtils.deserialize(sliceByteBuffer(byteBuffer, byteBuffer.getInt()), keyTypeValue);
        Object value = ObjectSerDeUtils.deserialize(sliceByteBuffer(byteBuffer, byteBuffer.getInt()), valueTypeValue);
        map.put(key, value);
      }
      return map;
    }

    private ByteBuffer sliceByteBuffer(ByteBuffer byteBuffer, int size) {
      ByteBuffer slice = byteBuffer.slice();
      slice.limit(size);
      byteBuffer.position(byteBuffer.position() + size);
      return slice;
    }
  };

  public static final ObjectSerDe<IntSet> INT_SET_SER_DE = new ObjectSerDe<IntSet>() {

    @Override
    public byte[] serialize(IntSet intSet) {
      int size = intSet.size();
      byte[] bytes = new byte[Integer.BYTES + size * Integer.BYTES];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      IntIterator iterator = intSet.iterator();
      while (iterator.hasNext()) {
        byteBuffer.putInt(iterator.nextInt());
      }
      return bytes;
    }

    @Override
    public IntOpenHashSet deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public IntOpenHashSet deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      IntOpenHashSet intSet = new IntOpenHashSet(size);
      for (int i = 0; i < size; i++) {
        intSet.add(byteBuffer.getInt());
      }
      return intSet;
    }
  };

  public static final ObjectSerDe<LongSet> LONG_SET_SER_DE = new ObjectSerDe<LongSet>() {

    @Override
    public byte[] serialize(LongSet longSet) {
      int size = longSet.size();
      byte[] bytes = new byte[Integer.BYTES + size * Long.BYTES];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      LongIterator iterator = longSet.iterator();
      while (iterator.hasNext()) {
        byteBuffer.putLong(iterator.nextLong());
      }
      return bytes;
    }

    @Override
    public LongOpenHashSet deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public LongOpenHashSet deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      LongOpenHashSet longSet = new LongOpenHashSet(size);
      for (int i = 0; i < size; i++) {
        longSet.add(byteBuffer.getLong());
      }
      return longSet;
    }
  };

  public static final ObjectSerDe<FloatSet> FLOAT_SET_SER_DE = new ObjectSerDe<FloatSet>() {

    @Override
    public byte[] serialize(FloatSet floatSet) {
      int size = floatSet.size();
      byte[] bytes = new byte[Integer.BYTES + size * Float.BYTES];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      FloatIterator iterator = floatSet.iterator();
      while (iterator.hasNext()) {
        byteBuffer.putFloat(iterator.nextFloat());
      }
      return bytes;
    }

    @Override
    public FloatOpenHashSet deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public FloatOpenHashSet deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      FloatOpenHashSet floatSet = new FloatOpenHashSet(size);
      for (int i = 0; i < size; i++) {
        floatSet.add(byteBuffer.getFloat());
      }
      return floatSet;
    }
  };

  public static final ObjectSerDe<DoubleSet> DOUBLE_SET_SER_DE = new ObjectSerDe<DoubleSet>() {

    @Override
    public byte[] serialize(DoubleSet doubleSet) {
      int size = doubleSet.size();
      byte[] bytes = new byte[Integer.BYTES + size * Double.BYTES];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      DoubleIterator iterator = doubleSet.iterator();
      while (iterator.hasNext()) {
        byteBuffer.putDouble(iterator.nextDouble());
      }
      return bytes;
    }

    @Override
    public DoubleOpenHashSet deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public DoubleOpenHashSet deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      DoubleOpenHashSet doubleSet = new DoubleOpenHashSet(size);
      for (int i = 0; i < size; i++) {
        doubleSet.add(byteBuffer.getDouble());
      }
      return doubleSet;
    }
  };

  public static final ObjectSerDe<Set<String>> STRING_SET_SER_DE = new ObjectSerDe<Set<String>>() {

    @Override
    public byte[] serialize(Set<String> stringSet) {
      int size = stringSet.size();
      // NOTE: No need to close the ByteArrayOutputStream.
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
      try {
        dataOutputStream.writeInt(size);
        for (String value : stringSet) {
          byte[] bytes = StringUtils.encodeUtf8(value);
          dataOutputStream.writeInt(bytes.length);
          dataOutputStream.write(bytes);
        }
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while serializing Set<String>", e);
      }
      return byteArrayOutputStream.toByteArray();
    }

    @Override
    public ObjectOpenHashSet<String> deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public ObjectOpenHashSet<String> deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      ObjectOpenHashSet<String> stringSet = new ObjectOpenHashSet<>(size);
      for (int i = 0; i < size; i++) {
        int length = byteBuffer.getInt();
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        stringSet.add(StringUtils.decodeUtf8(bytes));
      }
      return stringSet;
    }
  };

  public static final ObjectSerDe<Set<ByteArray>> BYTES_SET_SER_DE = new ObjectSerDe<Set<ByteArray>>() {

    @Override
    public byte[] serialize(Set<ByteArray> bytesSet) {
      int size = bytesSet.size();
      // NOTE: No need to close the ByteArrayOutputStream.
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
      try {
        dataOutputStream.writeInt(size);
        for (ByteArray value : bytesSet) {
          byte[] bytes = value.getBytes();
          dataOutputStream.writeInt(bytes.length);
          dataOutputStream.write(bytes);
        }
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while serializing Set<ByteArray>", e);
      }
      return byteArrayOutputStream.toByteArray();
    }

    @Override
    public ObjectOpenHashSet<ByteArray> deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public ObjectOpenHashSet<ByteArray> deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      ObjectOpenHashSet<ByteArray> bytesSet = new ObjectOpenHashSet<>(size);
      for (int i = 0; i < size; i++) {
        int length = byteBuffer.getInt();
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        bytesSet.add(new ByteArray(bytes));
      }
      return bytesSet;
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

  public static final ObjectSerDe<Sketch> DATA_SKETCH_SER_DE = new ObjectSerDe<Sketch>() {

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

  public static final ObjectSerDe<Geometry> GEOMETRY_SER_DE = new ObjectSerDe<Geometry>() {

    @Override
    public byte[] serialize(Geometry value) {
      return GeometrySerializer.serialize(value);
    }

    @Override
    public Geometry deserialize(byte[] bytes) {
      return GeometrySerializer.deserialize(bytes);
    }

    @Override
    public Geometry deserialize(ByteBuffer byteBuffer) {
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return GeometrySerializer.deserialize(bytes);
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

  public static final ObjectSerDe<IdSet> ID_SET_SER_DE = new ObjectSerDe<IdSet>() {

    @Override
    public byte[] serialize(IdSet idSet) {
      try {
        return idSet.toBytes();
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while serializing IdSet", e);
      }
    }

    @Override
    public IdSet deserialize(byte[] bytes) {
      try {
        return IdSets.fromBytes(bytes);
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while deserializing IdSet", e);
      }
    }

    @Override
    public IdSet deserialize(ByteBuffer byteBuffer) {
      try {
        return IdSets.fromByteBuffer(byteBuffer);
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while deserializing IdSet", e);
      }
    }
  };

  public static final ObjectSerDe<List<Object>> LIST_SER_DE = new ObjectSerDe<List<Object>>() {

    @Override
    public byte[] serialize(List<Object> list) {
      int size = list.size();

      // Directly return the size (0) for empty list
      if (size == 0) {
        return new byte[Integer.BYTES];
      }

      // No need to close these 2 streams (close() is no-op)
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

      try {
        // Write the size of the list
        dataOutputStream.writeInt(size);

        // Write the value type
        Object firstValue = list.get(0);
        int valueType = ObjectType.getObjectType(firstValue).getValue();
        dataOutputStream.writeInt(valueType);

        // Write the serialized values
        for (Object value : list) {
          byte[] bytes = ObjectSerDeUtils.serialize(value, valueType);
          dataOutputStream.writeInt(bytes.length);
          dataOutputStream.write(bytes);
        }
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while serializing List", e);
      }

      return byteArrayOutputStream.toByteArray();
    }

    @Override
    public ArrayList<Object> deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public ArrayList<Object> deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      ArrayList<Object> list = new ArrayList<>(size);

      // De-serialize the values
      if (size != 0) {
        int valueType = byteBuffer.getInt();
        for (int i = 0; i < size; i++) {
          int numBytes = byteBuffer.getInt();
          ByteBuffer slice = byteBuffer.slice();
          slice.limit(numBytes);
          list.add(ObjectSerDeUtils.deserialize(slice, valueType));
          byteBuffer.position(byteBuffer.position() + numBytes);
        }
      }

      return list;
    }
  };

  public static final ObjectSerDe<BigDecimal> BIGDECIMAL_SER_DE = new ObjectSerDe<BigDecimal>() {

    @Override
    public byte[] serialize(BigDecimal value) {
      return BigDecimalUtils.serialize(value);
    }

    @Override
    public BigDecimal deserialize(byte[] bytes) {
      return BigDecimalUtils.deserialize(bytes);
    }

    @Override
    public BigDecimal deserialize(ByteBuffer byteBuffer) {
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return BigDecimalUtils.deserialize(bytes);
    }
  };

  public static final ObjectSerDe<Int2LongMap> INT_2_LONG_MAP_SER_DE = new ObjectSerDe<Int2LongMap>() {

    @Override
    public byte[] serialize(Int2LongMap map) {
      int size = map.size();
      byte[] bytes = new byte[Integer.BYTES + size * (Integer.BYTES + Long.BYTES)];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      for (Int2LongMap.Entry entry : map.int2LongEntrySet()) {
        byteBuffer.putInt(entry.getIntKey());
        byteBuffer.putLong(entry.getLongValue());
      }
      return bytes;
    }

    @Override
    public Int2LongOpenHashMap deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public Int2LongOpenHashMap deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      Int2LongOpenHashMap map = new Int2LongOpenHashMap(size);
      for (int i = 0; i < size; i++) {
        map.put(byteBuffer.getInt(), byteBuffer.getLong());
      }
      return map;
    }
  };

  public static final ObjectSerDe<Long2LongMap> LONG_2_LONG_MAP_SER_DE = new ObjectSerDe<Long2LongMap>() {

    @Override
    public byte[] serialize(Long2LongMap map) {
      int size = map.size();
      byte[] bytes = new byte[Integer.BYTES + size * (Long.BYTES + Long.BYTES)];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      for (Long2LongMap.Entry entry : map.long2LongEntrySet()) {
        byteBuffer.putLong(entry.getLongKey());
        byteBuffer.putLong(entry.getLongValue());
      }
      return bytes;
    }

    @Override
    public Long2LongOpenHashMap deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public Long2LongOpenHashMap deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      Long2LongOpenHashMap map = new Long2LongOpenHashMap(size);
      for (int i = 0; i < size; i++) {
        map.put(byteBuffer.getLong(), byteBuffer.getLong());
      }
      return map;
    }
  };

  public static final ObjectSerDe<Float2LongMap> FLOAT_2_LONG_MAP_SER_DE = new ObjectSerDe<Float2LongMap>() {

    @Override
    public byte[] serialize(Float2LongMap map) {
      int size = map.size();
      byte[] bytes = new byte[Integer.BYTES + size * (Float.BYTES + Long.BYTES)];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      for (Float2LongMap.Entry entry : map.float2LongEntrySet()) {
        byteBuffer.putFloat(entry.getFloatKey());
        byteBuffer.putLong(entry.getLongValue());
      }
      return bytes;
    }

    @Override
    public Float2LongOpenHashMap deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public Float2LongOpenHashMap deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      Float2LongOpenHashMap map = new Float2LongOpenHashMap(size);
      for (int i = 0; i < size; i++) {
        map.put(byteBuffer.getFloat(), byteBuffer.getLong());
      }
      return map;
    }
  };

  public static final ObjectSerDe<Double2LongMap> DOUBLE_2_LONG_MAP_SER_DE = new ObjectSerDe<Double2LongMap>() {

    @Override
    public byte[] serialize(Double2LongMap map) {
      int size = map.size();
      byte[] bytes = new byte[Integer.BYTES + size * (Double.BYTES + Long.BYTES)];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      for (Double2LongMap.Entry entry : map.double2LongEntrySet()) {
        byteBuffer.putDouble(entry.getDoubleKey());
        byteBuffer.putLong(entry.getLongValue());
      }
      return bytes;
    }

    @Override
    public Double2LongOpenHashMap deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public Double2LongOpenHashMap deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      Double2LongOpenHashMap map = new Double2LongOpenHashMap(size);
      for (int i = 0; i < size; i++) {
        map.put(byteBuffer.getDouble(), byteBuffer.getLong());
      }
      return map;
    }
  };

  // NOTE: DO NOT change the order, it has to be the same order as the ObjectType
  //@formatter:off
  private static final ObjectSerDe[] SER_DES = {
      STRING_SER_DE,
      LONG_SER_DE,
      DOUBLE_SER_DE,
      DOUBLE_ARRAY_LIST_SER_DE,
      AVG_PAIR_SER_DE,
      MIN_MAX_RANGE_PAIR_SER_DE,
      HYPER_LOG_LOG_SER_DE,
      QUANTILE_DIGEST_SER_DE,
      MAP_SER_DE,
      INT_SET_SER_DE,
      TDIGEST_SER_DE,
      DISTINCT_TABLE_SER_DE,
      DATA_SKETCH_SER_DE,
      GEOMETRY_SER_DE,
      ROARING_BITMAP_SER_DE,
      LONG_SET_SER_DE,
      FLOAT_SET_SER_DE,
      DOUBLE_SET_SER_DE,
      STRING_SET_SER_DE,
      BYTES_SET_SER_DE,
      ID_SET_SER_DE,
      LIST_SER_DE,
      BIGDECIMAL_SER_DE,
      INT_2_LONG_MAP_SER_DE,
      LONG_2_LONG_MAP_SER_DE,
      FLOAT_2_LONG_MAP_SER_DE,
      DOUBLE_2_LONG_MAP_SER_DE,
      HYPER_LOG_LOG_SKETCH_SER_DE,
      HYPER_LOG_LOG_PLUS_PLUS_SER_DE
  };
  //@formatter:on

  public static byte[] serialize(Object value) {
    return serialize(value, ObjectType.getObjectType(value)._value);
  }

  public static byte[] serialize(Object value, ObjectType objectType) {
    return serialize(value, objectType._value);
  }

  public static byte[] serialize(Object value, int objectTypeValue) {
    return SER_DES[objectTypeValue].serialize(value);
  }

  public static <T> T deserialize(byte[] bytes, ObjectType objectType) {
    return deserialize(bytes, objectType._value);
  }

  public static <T> T deserialize(byte[] bytes, int objectTypeValue) {
    return (T) SER_DES[objectTypeValue].deserialize(bytes);
  }

  public static <T> T deserialize(ByteBuffer byteBuffer, ObjectType objectType) {
    return deserialize(byteBuffer, objectType._value);
  }

  public static <T> T deserialize(ByteBuffer byteBuffer, int objectTypeValue) {
    return (T) SER_DES[objectTypeValue].deserialize(byteBuffer);
  }
}
