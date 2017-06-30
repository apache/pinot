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
package com.linkedin.pinot.core.common.datatable;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.core.query.aggregation.function.customobject.AvgPair;
import com.linkedin.pinot.core.query.aggregation.function.customobject.MinMaxRangePair;
import com.linkedin.pinot.core.query.aggregation.function.customobject.QuantileDigest;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * The <code>ObjectCustomSerDe</code> class provides utility methods to serialize/de-serialize objects.
 */
public class ObjectCustomSerDe {
  private ObjectCustomSerDe() {
  }

  private static final Charset UTF_8 = Charset.forName("UTF-8");

  /**
   * Given an object, serialize it into a byte array.
   */
  @SuppressWarnings("unchecked")
  @Nonnull
  public static byte[] serialize(@Nonnull Object object)
      throws IOException {
    if (object instanceof String) {
      return ((String) object).getBytes("UTF-8");
    } else if (object instanceof Long) {
      return serializeLong((Long) object);
    } else if (object instanceof Double) {
      return serializeDouble((Double) object);
    } else if (object instanceof DoubleArrayList) {
      return serializeDoubleArrayList((DoubleArrayList) object);
    } else if (object instanceof AvgPair) {
      return ((AvgPair) object).toBytes();
    } else if (object instanceof MinMaxRangePair) {
      return ((MinMaxRangePair) object).toBytes();
    } else if (object instanceof HyperLogLog) {
      return ((HyperLogLog) object).getBytes();
    } else if (object instanceof QuantileDigest) {
      return serializeQuantileDigest((QuantileDigest) object);
    } else if (object instanceof HashMap) {
      return serializeHashMap((HashMap<Object, Object>) object);
    } else if (object instanceof IntOpenHashSet) {
      return serializeIntOpenHashSet((IntOpenHashSet) object);
    } else {
      throw new IllegalArgumentException("Illegal class for serialization: " + object.getClass().getName());
    }
  }

  /**
   * Given a byte array and the object type, de-serialize the byte array into an object.
   */
  @SuppressWarnings("unchecked")
  @Nonnull
  public static <T> T deserialize(@Nonnull byte[] bytes, @Nonnull ObjectType objectType)
      throws IOException {
    switch (objectType) {
      case String:
        return (T) new String(bytes, UTF_8);
      case Long:
        return (T) new Long(ByteBuffer.wrap(bytes).getLong());
      case Double:
        return (T) new Double(ByteBuffer.wrap(bytes).getDouble());
      case DoubleArrayList:
        return (T) deserializeDoubleArray(bytes);
      case AvgPair:
        return (T) AvgPair.fromBytes(bytes);
      case MinMaxRangePair:
        return (T) MinMaxRangePair.fromBytes(bytes);
      case HyperLogLog:
        return (T) HyperLogLog.Builder.build(bytes);
      case QuantileDigest:
        return (T) deserializeQuantileDigest(bytes);
      case HashMap:
        return (T) deserializeHashMap(bytes);
      case IntOpenHashSet:
        return (T) deserializeIntOpenHashSet(bytes);
      default:
        throw new IllegalArgumentException("Illegal object type for de-serialization: " + objectType);
    }
  }

  /**
   * Given a ByteBuffer and the object type, de-serialize the ByteBuffer into an object.
   */
  @SuppressWarnings("unchecked")
  @Nonnull
  public static <T> T deserialize(@Nonnull ByteBuffer byteBuffer, @Nonnull ObjectType objectType)
      throws IOException {
    switch (objectType) {
      case String:
        byte[] bytes = getBytesFromByteBuffer(byteBuffer);
        return (T) new String(bytes, UTF_8);
      case Long:
        return (T) new Long(byteBuffer.getLong());
      case Double:
        return (T) new Double(byteBuffer.getDouble());
      case DoubleArrayList:
        return (T) deserializeDoubleArray(byteBuffer);
      case AvgPair:
        return (T) AvgPair.fromByteBuffer(byteBuffer);
      case MinMaxRangePair:
        return (T) MinMaxRangePair.fromByteBuffer(byteBuffer);
      case HyperLogLog:
        bytes = getBytesFromByteBuffer(byteBuffer);
        return (T) HyperLogLog.Builder.build(bytes);
      case QuantileDigest:
        bytes = getBytesFromByteBuffer(byteBuffer);
        return (T) deserializeQuantileDigest(bytes);
      case HashMap:
        return (T) deserializeHashMap(byteBuffer);
      case IntOpenHashSet:
        return (T) deserializeIntOpenHashSet(byteBuffer);
      default:
        throw new IllegalArgumentException("Illegal object type for de-serialization: " + objectType);
    }
  }

  /**
   * Helper method to get a new byte[] from the given ByteBuffer.
   *
   * @param byteBuffer ByteBuffer for which to get the byte[] from.
   * @return byte[] containing the contents of ByteBuffer.
   */
  private static byte[] getBytesFromByteBuffer(@Nonnull ByteBuffer byteBuffer) {
    byte[] bytes = new byte[byteBuffer.limit()];
    byteBuffer.get(bytes);
    return bytes;
  }

  /**
   * Given an object, get its {@link ObjectType}.
   */
  @Nonnull
  public static ObjectType getObjectType(Object object) {
    if (object instanceof String) {
      return ObjectType.String;
    } else if (object instanceof Long) {
      return ObjectType.Long;
    } else if (object instanceof Double) {
      return ObjectType.Double;
    } else if (object instanceof DoubleArrayList) {
      return ObjectType.DoubleArrayList;
    } else if (object instanceof AvgPair) {
      return ObjectType.AvgPair;
    } else if (object instanceof MinMaxRangePair) {
      return ObjectType.MinMaxRangePair;
    } else if (object instanceof HyperLogLog) {
      return ObjectType.HyperLogLog;
    } else if (object instanceof QuantileDigest) {
      return ObjectType.QuantileDigest;
    } else if (object instanceof HashMap) {
      return ObjectType.HashMap;
    } else if (object instanceof IntOpenHashSet) {
      return ObjectType.IntOpenHashSet;
    } else {
      throw new IllegalArgumentException("No object type matches class: " + object.getClass().getName());
    }
  }

  /**
   * Helper method to serialize a {@link Long}.
   */
  private static byte[] serializeLong(Long value) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(V1Constants.Numbers.LONG_SIZE);
    byteBuffer.putLong(value);
    return byteBuffer.array();
  }

  /**
   * Helper method to serialize a {@link Double}.
   */
  private static byte[] serializeDouble(Double value) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(V1Constants.Numbers.DOUBLE_SIZE);
    byteBuffer.putDouble(value);
    return byteBuffer.array();
  }

  /**
   * Helper method to serialize a {@link DoubleArrayList}.
   */
  private static byte[] serializeDoubleArrayList(DoubleArrayList doubleArray) {
    int size = doubleArray.size();
    int byteBufferSize = V1Constants.Numbers.INTEGER_SIZE + (size * V1Constants.Numbers.DOUBLE_SIZE);

    ByteBuffer byteBuffer = ByteBuffer.allocate(byteBufferSize);
    byteBuffer.putInt(size);
    double[] elements = doubleArray.elements();
    for (int i = 0; i < size; i++) {
      byteBuffer.putDouble(elements[i]);
    }

    return byteBuffer.array();
  }

  /**
   * Helper method to de-serialize a {@link DoubleArrayList}.
   */
  private static DoubleArrayList deserializeDoubleArray(byte[] bytes) {
    return deserializeDoubleArray(ByteBuffer.wrap(bytes));
  }

  private static DoubleArrayList deserializeDoubleArray(ByteBuffer byteBuffer) {
    int size = byteBuffer.getInt();
    DoubleArrayList doubleArray = new DoubleArrayList(size);
    for (int i = 0; i < size; i++) {
      doubleArray.add(byteBuffer.getDouble());
    }

    return doubleArray;
  }

  /**
   * Helper method to serialize a {@link HashMap}.
   */
  private static byte[] serializeHashMap(HashMap<Object, Object> map)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    // Write the size of the map.
    dataOutputStream.writeInt(map.size());

    // Write the serialized key-value pairs.
    boolean first = true;
    for (Map.Entry<Object, Object> entry : map.entrySet()) {

      // Write the key and value type before writing the first key-value pair.
      if (first) {
        dataOutputStream.writeInt(getObjectType(entry.getKey()).getValue());
        dataOutputStream.writeInt(getObjectType(entry.getValue()).getValue());
        first = false;
      }

      byte[] keyBytes = serialize(entry.getKey());
      dataOutputStream.writeInt(keyBytes.length);
      dataOutputStream.write(keyBytes);

      byte[] valueBytes = serialize(entry.getValue());
      dataOutputStream.writeInt(valueBytes.length);
      dataOutputStream.write(valueBytes);
    }

    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Helper method to de-serialize a {@link HashMap} from a byte buffer.
   */
  private static HashMap<Object, Object> deserializeHashMap(ByteBuffer byteBuffer)
      throws IOException {
    int size = byteBuffer.getInt();
    HashMap<Object, Object> hashMap = new HashMap<>(size);
    if (size == 0) {
      return hashMap;
    }

    ObjectType keyType = ObjectType.getObjectType(byteBuffer.getInt());
    ObjectType valueType = ObjectType.getObjectType(byteBuffer.getInt());

    for (int i = 0; i < size; i++) {
      int keyNumBytes = byteBuffer.getInt();
      ByteBuffer slice = getByteBufferSlice(byteBuffer, keyNumBytes);
      Object key = deserialize(slice, keyType);
      byteBuffer.position(byteBuffer.position() + keyNumBytes);

      int valueNumBytes = byteBuffer.getInt();
      slice = getByteBufferSlice(byteBuffer, valueNumBytes);
      Object value = deserialize(slice, valueType);
      byteBuffer.position(byteBuffer.position() + valueNumBytes);

      hashMap.put(key, value);
    }
    return hashMap;
  }

  /**
   * Helper method to slice out a ByteBuffer of given size from the original ByteBuffer.
   *
   * @param byteBuffer Original ByteBuffer.
   * @param numBytes Number of bytes to slice out.
   * @return New ByteBuffer containing a slice of the original ByteBuffer.
   */
  private static ByteBuffer getByteBufferSlice(ByteBuffer byteBuffer, int numBytes) {
    ByteBuffer slice = byteBuffer.slice();
    slice.limit(numBytes);
    return slice;
  }

  /**
   * Helper method to de-serialize a {@link HashMap} from a byte[].
   */
  private static HashMap<Object, Object> deserializeHashMap(byte[] bytes)
      throws IOException {
    return deserializeHashMap(ByteBuffer.wrap(bytes));
  }

  /**
   * Helper method to serialize a {@link QuantileDigest}.
   */
  private static byte[] serializeQuantileDigest(QuantileDigest quantileDigest)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    quantileDigest.serialize(new DataOutputStream(byteArrayOutputStream));
    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Helper method to de-serialize a {@link QuantileDigest}.
   */
  private static QuantileDigest deserializeQuantileDigest(byte[] bytes)
      throws IOException {
    return QuantileDigest.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  /**
   * Helper method to serialize an {@link IntOpenHashSet}.
   */
  private static byte[] serializeIntOpenHashSet(IntOpenHashSet intOpenHashSet)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    // Write the size of the set.
    dataOutputStream.writeInt(intOpenHashSet.size());

    IntIterator intIterator = intOpenHashSet.iterator();
    while (intIterator.hasNext()) {
      dataOutputStream.writeInt(intIterator.nextInt());
    }

    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Helper method to de-serialize an {@link IntOpenHashSet} from a ByteBuffer.
   */
  private static IntOpenHashSet deserializeIntOpenHashSet(ByteBuffer byteBuffer) {

    int size = byteBuffer.getInt();
    IntOpenHashSet intOpenHashSet = new IntOpenHashSet(size);
    for (int i = 0; i < size; i++) {
      intOpenHashSet.add(byteBuffer.getInt());
    }

    return intOpenHashSet;
  }

  /**
   * Helper method to de-serialize an {@link IntOpenHashSet} from a byte[].
   */
  private static IntOpenHashSet deserializeIntOpenHashSet(byte[] bytes) {
    return deserializeIntOpenHashSet(ByteBuffer.wrap(bytes));
  }
}
