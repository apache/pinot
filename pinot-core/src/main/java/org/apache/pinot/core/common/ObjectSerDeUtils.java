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
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.geospatial.serde.GeometrySerializer;
import org.apache.pinot.core.query.aggregation.function.customobject.AvgPair;
import org.apache.pinot.core.query.aggregation.function.customobject.DistinctTable;
import org.apache.pinot.core.query.aggregation.function.customobject.MinMaxRangePair;
import org.apache.pinot.core.query.aggregation.function.customobject.QuantileDigest;
import org.locationtech.jts.geom.Geometry;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code ObjectSerDeUtils} class provides the utility methods to serialize/de-serialize objects.
 */
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
    RoaringBitmap(14);

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
      } else if (value instanceof DoubleArrayList) {
        return ObjectType.DoubleArrayList;
      } else if (value instanceof AvgPair) {
        return ObjectType.AvgPair;
      } else if (value instanceof MinMaxRangePair) {
        return ObjectType.MinMaxRangePair;
      } else if (value instanceof HyperLogLog) {
        return ObjectType.HyperLogLog;
      } else if (value instanceof QuantileDigest) {
        return ObjectType.QuantileDigest;
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
        return new DistinctTable(ByteBuffer.wrap(bytes));
      } catch (IOException e) {
        throw new IllegalStateException("Caught exception while de-serializing DistinctTable", e);
      }
    }

    @Override
    public DistinctTable deserialize(ByteBuffer byteBuffer) {
      try {
        return new DistinctTable(byteBuffer);
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
    public Map<Object, Object> deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public Map<Object, Object> deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      Map<Object, Object> map = new HashMap<>(size);
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
    public IntSet deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public IntSet deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      IntSet intSet = new IntOpenHashSet(size);
      for (int i = 0; i < size; i++) {
        intSet.add(byteBuffer.getInt());
      }
      return intSet;
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
      return value.compact().toByteArray();
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
      ROARING_BITMAP_SER_DE
  };
  //@formatter:on

  public static byte[] serialize(Object value) {
    return serialize(value, ObjectType.getObjectType(value)._value);
  }

  public static byte[] serialize(Object value, ObjectType objectType) {
    return serialize(value, objectType._value);
  }

  @SuppressWarnings("unchecked")
  public static byte[] serialize(Object value, int objectTypeValue) {
    return SER_DES[objectTypeValue].serialize(value);
  }

  public static <T> T deserialize(byte[] bytes, ObjectType objectType) {
    return deserialize(bytes, objectType._value);
  }

  @SuppressWarnings("unchecked")
  public static <T> T deserialize(byte[] bytes, int objectTypeValue) {
    return (T) SER_DES[objectTypeValue].deserialize(bytes);
  }

  public static <T> T deserialize(ByteBuffer byteBuffer, ObjectType objectType) {
    return deserialize(byteBuffer, objectType._value);
  }

  @SuppressWarnings("unchecked")
  public static <T> T deserialize(ByteBuffer byteBuffer, int objectTypeValue) {
    return (T) SER_DES[objectTypeValue].deserialize(byteBuffer);
  }
}
