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
import com.clearspring.analytics.stream.cardinality.RegisterSet;
import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import it.unimi.dsi.fastutil.doubles.Double2LongMap;
import it.unimi.dsi.fastutil.doubles.Double2LongOpenHashMap;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.Float2LongMap;
import it.unimi.dsi.fastutil.floats.Float2LongOpenHashMap;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Set;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.pinot.segment.local.customobject.tdigest.SerializedTDigestInput;
import org.apache.pinot.segment.local.customobject.tdigest.TDigest;
import org.apache.pinot.segment.local.customobject.tdigest.TDigestAccumulator;
import org.apache.pinot.segment.spi.memory.PinotOutputStream;
import org.apache.pinot.segment.spi.memory.PrimitiveArrays;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.Utf8Utils;


/// The codecs of the intermediate results that aggregation functions carry in `OBJECT` columns, and of the
/// pre-aggregated values that value aggregators store in `BYTES` columns, one per type defined in this module or below
/// it. An aggregation function owns the serialization of its intermediate result: it serializes and deserializes the
/// result, with the codec of its type when there is one, and declares as its own constant the wire type value that
/// precedes the payload. A value identifies one intermediate result type for good: a new type takes a value above
/// every one declared so far, and a value never changes or gets reused once its type is retired.
public class ObjectSerDeUtils {
  private ObjectSerDeUtils() {
  }

  /// Serializer/De-serializer for a specific type of object.
  ///
  /// @param <T> Type of the object
  public interface ObjectSerDe<T> {

    /// Serializes a value into a byte array.
    byte[] serialize(T value);

    /// Writes the serialized form of a value to an output stream, which takes values straight into its pages. The
    /// default writes the byte array; a codec that writes its value in parts overrides this to skip the full-size
    /// array.
    default void serialize(T value, PinotOutputStream output)
        throws IOException {
      output.write(serialize(value));
    }

    /// De-serializes a value from the remaining bytes of a byte buffer, which end where the payload ends. Where the
    /// position is left afterwards depends on the codec.
    T deserialize(ByteBuffer buffer);

    /// De-serializes a value from a byte array. The default wraps the array, which costs nothing. A codec whose native
    /// input is an array overrides this one too, so that an array caller skips the copy its buffer overload makes.
    default T deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }
  }

  /// Serializer/De-serializer that knows the payload size of a value without serializing it, so the value can be
  /// written straight into a buffer reserved for it.
  ///
  /// @param <T> Type of the object
  public interface SizedObjectSerDe<T> extends ObjectSerDe<T> {

    /// Returns the exact payload size of a value. Must not serialize the value or allocate the payload. The value must
    /// stay unchanged between sizing and serialization.
    int getSerializedSize(T value);

    /// Writes the payload at the current position of the big-endian `buffer`, which has at least
    /// [#getSerializedSize(Object)] bytes remaining, and advances the position by the payload size. Only the position
    /// changes: the buffer is not retained and the value is not mutated. Supports heap and direct buffers.
    void serialize(T value, ByteBuffer buffer);

    @Override
    default byte[] serialize(T value) {
      byte[] bytes = new byte[getSerializedSize(value)];
      serialize(value, ByteBuffer.wrap(bytes));
      return bytes;
    }
  }

  public static final SizedObjectSerDe<IntArrayList> INT_ARRAY_LIST_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(IntArrayList intArrayList) {
      return Math.addExact(Integer.BYTES, Math.multiplyExact(intArrayList.size(), Integer.BYTES));
    }

    @Override
    public void serialize(IntArrayList intArrayList, ByteBuffer buffer) {
      int size = intArrayList.size();
      buffer.putInt(size);
      int[] values = intArrayList.elements();
      for (int i = 0; i < size; i++) {
        buffer.putInt(values[i]);
      }
    }

    @Override
    public void serialize(IntArrayList intArrayList, PinotOutputStream output)
        throws IOException {
      int size = intArrayList.size();
      output.writeInt(size);
      output.writeInts(intArrayList.elements(), 0, size);
    }

    @Override
    public IntArrayList deserialize(ByteBuffer buffer) {
      int numValues = buffer.getInt();
      int[] values = new int[numValues];
      PrimitiveArrays.readInts(buffer, values, 0, numValues);
      return IntArrayList.wrap(values);
    }
  };

  public static final SizedObjectSerDe<LongArrayList> LONG_ARRAY_LIST_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(LongArrayList longArrayList) {
      return Math.addExact(Integer.BYTES, Math.multiplyExact(longArrayList.size(), Long.BYTES));
    }

    @Override
    public void serialize(LongArrayList longArrayList, ByteBuffer buffer) {
      int size = longArrayList.size();
      buffer.putInt(size);
      long[] values = longArrayList.elements();
      for (int i = 0; i < size; i++) {
        buffer.putLong(values[i]);
      }
    }

    @Override
    public void serialize(LongArrayList longArrayList, PinotOutputStream output)
        throws IOException {
      int size = longArrayList.size();
      output.writeInt(size);
      output.writeLongs(longArrayList.elements(), 0, size);
    }

    @Override
    public LongArrayList deserialize(ByteBuffer buffer) {
      int numValues = buffer.getInt();
      long[] values = new long[numValues];
      PrimitiveArrays.readLongs(buffer, values, 0, numValues);
      return LongArrayList.wrap(values);
    }
  };

  public static final SizedObjectSerDe<FloatArrayList> FLOAT_ARRAY_LIST_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(FloatArrayList floatArrayList) {
      return Math.addExact(Integer.BYTES, Math.multiplyExact(floatArrayList.size(), Float.BYTES));
    }

    @Override
    public void serialize(FloatArrayList floatArrayList, ByteBuffer buffer) {
      int size = floatArrayList.size();
      buffer.putInt(size);
      float[] values = floatArrayList.elements();
      for (int i = 0; i < size; i++) {
        buffer.putInt(Float.floatToRawIntBits(values[i]));
      }
    }

    @Override
    public void serialize(FloatArrayList floatArrayList, PinotOutputStream output)
        throws IOException {
      int size = floatArrayList.size();
      output.writeInt(size);
      output.writeFloats(floatArrayList.elements(), 0, size);
    }

    @Override
    public FloatArrayList deserialize(ByteBuffer buffer) {
      int numValues = buffer.getInt();
      float[] values = new float[numValues];
      PrimitiveArrays.readFloats(buffer, values, 0, numValues);
      return FloatArrayList.wrap(values);
    }
  };

  public static final SizedObjectSerDe<DoubleArrayList> DOUBLE_ARRAY_LIST_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(DoubleArrayList doubleArrayList) {
      return Math.addExact(Integer.BYTES, Math.multiplyExact(doubleArrayList.size(), Double.BYTES));
    }

    @Override
    public void serialize(DoubleArrayList doubleArrayList, ByteBuffer buffer) {
      int size = doubleArrayList.size();
      buffer.putInt(size);
      double[] values = doubleArrayList.elements();
      for (int i = 0; i < size; i++) {
        buffer.putLong(Double.doubleToRawLongBits(values[i]));
      }
    }

    @Override
    public void serialize(DoubleArrayList doubleArrayList, PinotOutputStream output)
        throws IOException {
      int size = doubleArrayList.size();
      output.writeInt(size);
      output.writeDoubles(doubleArrayList.elements(), 0, size);
    }

    @Override
    public DoubleArrayList deserialize(ByteBuffer buffer) {
      int numValues = buffer.getInt();
      double[] values = new double[numValues];
      PrimitiveArrays.readDoubles(buffer, values, 0, numValues);
      return DoubleArrayList.wrap(values);
    }
  };

  /// Wraps an `Object[]` as a list of `T`, the array type [ObjectArrayList] backs itself with, so that later stores
  /// and growth skip the component-type checks of a typed array. The cast is erased inside this generic method.
  private static <T> ObjectArrayList<T> wrapObjects(Object[] values) {
    //noinspection unchecked
    return ObjectArrayList.wrap((T[]) values);
  }

  public static final SizedObjectSerDe<ObjectArrayList<BigDecimal>> BIG_DECIMAL_ARRAY_LIST_SER_DE =
      new SizedObjectSerDe<>() {

        @Override
        public int getSerializedSize(ObjectArrayList<BigDecimal> list) {
          // Header: size, then a length-prefix per element, followed by the serialized BigDecimal bytes.
          int numValues = list.size();
          int size = Math.addExact(Integer.BYTES, Math.multiplyExact(numValues, Integer.BYTES));
          Object[] values = list.elements();
          for (int i = 0; i < numValues; i++) {
            size = Math.addExact(size, BigDecimalUtils.byteSize((BigDecimal) values[i]));
          }
          return size;
        }

        @Override
        public void serialize(ObjectArrayList<BigDecimal> list, ByteBuffer buffer) {
          int numValues = list.size();
          buffer.putInt(numValues);
          Object[] values = list.elements();
          for (int i = 0; i < numValues; i++) {
            byte[] valueBytes = BigDecimalUtils.serialize((BigDecimal) values[i]);
            buffer.putInt(valueBytes.length);
            buffer.put(valueBytes);
          }
        }

        @Override
        public void serialize(ObjectArrayList<BigDecimal> list, PinotOutputStream output)
            throws IOException {
          int numValues = list.size();
          output.writeInt(numValues);
          Object[] values = list.elements();
          for (int i = 0; i < numValues; i++) {
            byte[] valueBytes = BigDecimalUtils.serialize((BigDecimal) values[i]);
            output.writeInt(valueBytes.length);
            output.write(valueBytes);
          }
        }

        @Override
        public ObjectArrayList<BigDecimal> deserialize(ByteBuffer buffer) {
          int numValues = buffer.getInt();
          Object[] values = new Object[numValues];
          for (int i = 0; i < numValues; i++) {
            byte[] valueBytes = new byte[buffer.getInt()];
            buffer.get(valueBytes);
            values[i] = BigDecimalUtils.deserialize(valueBytes);
          }
          return wrapObjects(values);
        }
      };

  public static final SizedObjectSerDe<ObjectArrayList<String>> STRING_ARRAY_LIST_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(ObjectArrayList<String> stringArrayList) {
      // Besides the value bytes, we store: size, length for each value
      int numValues = stringArrayList.size();
      int size = Math.addExact(Integer.BYTES, Math.multiplyExact(numValues, Integer.BYTES));
      Object[] values = stringArrayList.elements();
      for (int i = 0; i < numValues; i++) {
        size = Math.addExact(size, Utf8Utils.encodedLength((String) values[i]));
      }
      return size;
    }

    @Override
    public void serialize(ObjectArrayList<String> stringArrayList, ByteBuffer buffer) {
      int numValues = stringArrayList.size();
      buffer.putInt(numValues);
      Object[] values = stringArrayList.elements();
      for (int i = 0; i < numValues; i++) {
        byte[] valueBytes = Utf8Utils.encode((String) values[i]);
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);
      }
    }

    @Override
    public void serialize(ObjectArrayList<String> stringArrayList, PinotOutputStream output)
        throws IOException {
      int numValues = stringArrayList.size();
      output.writeInt(numValues);
      Object[] values = stringArrayList.elements();
      for (int i = 0; i < numValues; i++) {
        byte[] valueBytes = Utf8Utils.encode((String) values[i]);
        output.writeInt(valueBytes.length);
        output.write(valueBytes);
      }
    }

    @Override
    public ObjectArrayList<String> deserialize(ByteBuffer buffer) {
      int numValues = buffer.getInt();
      Object[] values = new Object[numValues];
      for (int i = 0; i < numValues; i++) {
        byte[] valueBytes = new byte[buffer.getInt()];
        buffer.get(valueBytes);
        values[i] = Utf8Utils.decode(valueBytes);
      }
      return wrapObjects(values);
    }
  };

  public static final SizedObjectSerDe<ObjectArrayList<ByteArray>> BYTES_ARRAY_LIST_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(ObjectArrayList<ByteArray> list) {
      int numValues = list.size();
      int size = Math.addExact(Integer.BYTES, Math.multiplyExact(numValues, Integer.BYTES));
      Object[] values = list.elements();
      for (int i = 0; i < numValues; i++) {
        size = Math.addExact(size, ((ByteArray) values[i]).length());
      }
      return size;
    }

    @Override
    public void serialize(ObjectArrayList<ByteArray> list, ByteBuffer buffer) {
      int numValues = list.size();
      buffer.putInt(numValues);
      Object[] values = list.elements();
      for (int i = 0; i < numValues; i++) {
        byte[] valueBytes = ((ByteArray) values[i]).getBytes();
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);
      }
    }

    @Override
    public void serialize(ObjectArrayList<ByteArray> list, PinotOutputStream output)
        throws IOException {
      int numValues = list.size();
      output.writeInt(numValues);
      Object[] values = list.elements();
      for (int i = 0; i < numValues; i++) {
        byte[] valueBytes = ((ByteArray) values[i]).getBytes();
        output.writeInt(valueBytes.length);
        output.write(valueBytes);
      }
    }

    @Override
    public ObjectArrayList<ByteArray> deserialize(ByteBuffer buffer) {
      int numValues = buffer.getInt();
      Object[] values = new Object[numValues];
      for (int i = 0; i < numValues; i++) {
        byte[] valueBytes = new byte[buffer.getInt()];
        buffer.get(valueBytes);
        values[i] = new ByteArray(valueBytes);
      }
      return wrapObjects(values);
    }
  };

  public static final SizedObjectSerDe<IntSet> INT_SET_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(IntSet intSet) {
      return Math.addExact(Integer.BYTES, Math.multiplyExact(intSet.size(), Integer.BYTES));
    }

    @Override
    public void serialize(IntSet intSet, ByteBuffer buffer) {
      buffer.putInt(intSet.size());
      IntIterator iterator = intSet.iterator();
      while (iterator.hasNext()) {
        buffer.putInt(iterator.nextInt());
      }
    }

    @Override
    public void serialize(IntSet intSet, PinotOutputStream output)
        throws IOException {
      output.writeInt(intSet.size());
      IntIterator iterator = intSet.iterator();
      while (iterator.hasNext()) {
        output.writeInt(iterator.nextInt());
      }
    }

    @Override
    public IntOpenHashSet deserialize(ByteBuffer buffer) {
      int size = buffer.getInt();
      IntOpenHashSet intSet = new IntOpenHashSet(size);
      for (int i = 0; i < size; i++) {
        intSet.add(buffer.getInt());
      }
      return intSet;
    }
  };

  public static final SizedObjectSerDe<LongSet> LONG_SET_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(LongSet longSet) {
      return Math.addExact(Integer.BYTES, Math.multiplyExact(longSet.size(), Long.BYTES));
    }

    @Override
    public void serialize(LongSet longSet, ByteBuffer buffer) {
      buffer.putInt(longSet.size());
      LongIterator iterator = longSet.iterator();
      while (iterator.hasNext()) {
        buffer.putLong(iterator.nextLong());
      }
    }

    @Override
    public void serialize(LongSet longSet, PinotOutputStream output)
        throws IOException {
      output.writeInt(longSet.size());
      LongIterator iterator = longSet.iterator();
      while (iterator.hasNext()) {
        output.writeLong(iterator.nextLong());
      }
    }

    @Override
    public LongOpenHashSet deserialize(ByteBuffer buffer) {
      int size = buffer.getInt();
      LongOpenHashSet longSet = new LongOpenHashSet(size);
      for (int i = 0; i < size; i++) {
        longSet.add(buffer.getLong());
      }
      return longSet;
    }
  };

  public static final SizedObjectSerDe<FloatSet> FLOAT_SET_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(FloatSet floatSet) {
      return Math.addExact(Integer.BYTES, Math.multiplyExact(floatSet.size(), Float.BYTES));
    }

    @Override
    public void serialize(FloatSet floatSet, ByteBuffer buffer) {
      buffer.putInt(floatSet.size());
      FloatIterator iterator = floatSet.iterator();
      while (iterator.hasNext()) {
        buffer.putFloat(iterator.nextFloat());
      }
    }

    @Override
    public void serialize(FloatSet floatSet, PinotOutputStream output)
        throws IOException {
      output.writeInt(floatSet.size());
      FloatIterator iterator = floatSet.iterator();
      while (iterator.hasNext()) {
        output.writeInt(Float.floatToRawIntBits(iterator.nextFloat()));
      }
    }

    @Override
    public FloatOpenHashSet deserialize(ByteBuffer buffer) {
      int size = buffer.getInt();
      FloatOpenHashSet floatSet = new FloatOpenHashSet(size);
      for (int i = 0; i < size; i++) {
        floatSet.add(buffer.getFloat());
      }
      return floatSet;
    }
  };

  public static final SizedObjectSerDe<DoubleSet> DOUBLE_SET_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(DoubleSet doubleSet) {
      return Math.addExact(Integer.BYTES, Math.multiplyExact(doubleSet.size(), Double.BYTES));
    }

    @Override
    public void serialize(DoubleSet doubleSet, ByteBuffer buffer) {
      buffer.putInt(doubleSet.size());
      DoubleIterator iterator = doubleSet.iterator();
      while (iterator.hasNext()) {
        buffer.putDouble(iterator.nextDouble());
      }
    }

    @Override
    public void serialize(DoubleSet doubleSet, PinotOutputStream output)
        throws IOException {
      output.writeInt(doubleSet.size());
      DoubleIterator iterator = doubleSet.iterator();
      while (iterator.hasNext()) {
        output.writeLong(Double.doubleToRawLongBits(iterator.nextDouble()));
      }
    }

    @Override
    public DoubleOpenHashSet deserialize(ByteBuffer buffer) {
      int size = buffer.getInt();
      DoubleOpenHashSet doubleSet = new DoubleOpenHashSet(size);
      for (int i = 0; i < size; i++) {
        doubleSet.add(buffer.getDouble());
      }
      return doubleSet;
    }
  };

  public static final SizedObjectSerDe<Set<BigDecimal>> BIG_DECIMAL_SET_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(Set<BigDecimal> set) {
      // Besides the value bytes, we store: size, length for each value
      int size = Math.addExact(Integer.BYTES, Math.multiplyExact(set.size(), Integer.BYTES));
      for (BigDecimal value : set) {
        size = Math.addExact(size, BigDecimalUtils.byteSize(value));
      }
      return size;
    }

    @Override
    public void serialize(Set<BigDecimal> set, ByteBuffer buffer) {
      buffer.putInt(set.size());
      for (BigDecimal value : set) {
        byte[] valueBytes = BigDecimalUtils.serialize(value);
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);
      }
    }

    @Override
    public void serialize(Set<BigDecimal> set, PinotOutputStream output)
        throws IOException {
      output.writeInt(set.size());
      for (BigDecimal value : set) {
        byte[] valueBytes = BigDecimalUtils.serialize(value);
        output.writeInt(valueBytes.length);
        output.write(valueBytes);
      }
    }

    @Override
    public ObjectOpenHashSet<BigDecimal> deserialize(ByteBuffer buffer) {
      int size = buffer.getInt();
      ObjectOpenHashSet<BigDecimal> set = new ObjectOpenHashSet<>(size);
      for (int i = 0; i < size; i++) {
        int length = buffer.getInt();
        byte[] valueBytes = new byte[length];
        buffer.get(valueBytes);
        set.add(BigDecimalUtils.deserialize(valueBytes));
      }
      return set;
    }
  };

  public static final SizedObjectSerDe<Set<String>> STRING_SET_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(Set<String> stringSet) {
      // Besides the value bytes, we store: size, length for each value
      int size = Math.addExact(Integer.BYTES, Math.multiplyExact(stringSet.size(), Integer.BYTES));
      for (String value : stringSet) {
        size = Math.addExact(size, Utf8Utils.encodedLength(value));
      }
      return size;
    }

    @Override
    public void serialize(Set<String> stringSet, ByteBuffer buffer) {
      buffer.putInt(stringSet.size());
      for (String value : stringSet) {
        byte[] valueBytes = Utf8Utils.encode(value);
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);
      }
    }

    @Override
    public void serialize(Set<String> stringSet, PinotOutputStream output)
        throws IOException {
      output.writeInt(stringSet.size());
      for (String value : stringSet) {
        byte[] valueBytes = Utf8Utils.encode(value);
        output.writeInt(valueBytes.length);
        output.write(valueBytes);
      }
    }

    @Override
    public ObjectOpenHashSet<String> deserialize(ByteBuffer buffer) {
      int size = buffer.getInt();
      ObjectOpenHashSet<String> stringSet = new ObjectOpenHashSet<>(size);
      for (int i = 0; i < size; i++) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        stringSet.add(Utf8Utils.decode(bytes));
      }
      return stringSet;
    }
  };

  public static final SizedObjectSerDe<Set<ByteArray>> BYTES_SET_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(Set<ByteArray> bytesSet) {
      // Besides the value bytes, we store: size, length for each value
      int size = Math.addExact(Integer.BYTES, Math.multiplyExact(bytesSet.size(), Integer.BYTES));
      for (ByteArray value : bytesSet) {
        size = Math.addExact(size, value.length());
      }
      return size;
    }

    @Override
    public void serialize(Set<ByteArray> bytesSet, ByteBuffer buffer) {
      buffer.putInt(bytesSet.size());
      for (ByteArray value : bytesSet) {
        byte[] valueBytes = value.getBytes();
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);
      }
    }

    @Override
    public void serialize(Set<ByteArray> bytesSet, PinotOutputStream output)
        throws IOException {
      output.writeInt(bytesSet.size());
      for (ByteArray value : bytesSet) {
        byte[] valueBytes = value.getBytes();
        output.writeInt(valueBytes.length);
        output.write(valueBytes);
      }
    }

    @Override
    public ObjectOpenHashSet<ByteArray> deserialize(ByteBuffer buffer) {
      int size = buffer.getInt();
      ObjectOpenHashSet<ByteArray> bytesSet = new ObjectOpenHashSet<>(size);
      for (int i = 0; i < size; i++) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        bytesSet.add(new ByteArray(bytes));
      }
      return bytesSet;
    }
  };

  public static final SizedObjectSerDe<ObjectLinkedOpenHashSet<String>> ORDERED_STRING_SET_SER_DE =
      new SizedObjectSerDe<>() {

        @Override
        public int getSerializedSize(ObjectLinkedOpenHashSet<String> stringSet) {
          // Besides the value bytes, we store: size, length for each value
          int size = Math.addExact(Integer.BYTES, Math.multiplyExact(stringSet.size(), Integer.BYTES));
          for (String value : stringSet) {
            size = Math.addExact(size, Utf8Utils.encodedLength(value));
          }
          return size;
        }

        @Override
        public void serialize(ObjectLinkedOpenHashSet<String> stringSet, ByteBuffer buffer) {
          buffer.putInt(stringSet.size());
          for (String value : stringSet) {
            byte[] valueBytes = Utf8Utils.encode(value);
            buffer.putInt(valueBytes.length);
            buffer.put(valueBytes);
          }
        }

        @Override
        public void serialize(ObjectLinkedOpenHashSet<String> stringSet, PinotOutputStream output)
            throws IOException {
          output.writeInt(stringSet.size());
          for (String value : stringSet) {
            byte[] valueBytes = Utf8Utils.encode(value);
            output.writeInt(valueBytes.length);
            output.write(valueBytes);
          }
        }

        @Override
        public ObjectLinkedOpenHashSet<String> deserialize(ByteBuffer buffer) {
          int size = buffer.getInt();
          ObjectLinkedOpenHashSet<String> stringSet = new ObjectLinkedOpenHashSet<>(size);
          for (int i = 0; i < size; i++) {
            int length = buffer.getInt();
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            stringSet.add(Utf8Utils.decode(bytes));
          }
          return stringSet;
        }
      };

  public static final SizedObjectSerDe<Int2LongMap> INT_2_LONG_MAP_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(Int2LongMap map) {
      return Math.addExact(Integer.BYTES, Math.multiplyExact(map.size(), Integer.BYTES + Long.BYTES));
    }

    @Override
    public void serialize(Int2LongMap map, ByteBuffer buffer) {
      buffer.putInt(map.size());
      for (Int2LongMap.Entry entry : map.int2LongEntrySet()) {
        buffer.putInt(entry.getIntKey());
        buffer.putLong(entry.getLongValue());
      }
    }

    @Override
    public void serialize(Int2LongMap map, PinotOutputStream output)
        throws IOException {
      output.writeInt(map.size());
      for (Int2LongMap.Entry entry : map.int2LongEntrySet()) {
        output.writeInt(entry.getIntKey());
        output.writeLong(entry.getLongValue());
      }
    }

    @Override
    public Int2LongOpenHashMap deserialize(ByteBuffer buffer) {
      int size = buffer.getInt();
      Int2LongOpenHashMap map = new Int2LongOpenHashMap(size);
      for (int i = 0; i < size; i++) {
        map.put(buffer.getInt(), buffer.getLong());
      }
      return map;
    }
  };

  public static final SizedObjectSerDe<Long2LongMap> LONG_2_LONG_MAP_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(Long2LongMap map) {
      return Math.addExact(Integer.BYTES, Math.multiplyExact(map.size(), Long.BYTES + Long.BYTES));
    }

    @Override
    public void serialize(Long2LongMap map, ByteBuffer buffer) {
      buffer.putInt(map.size());
      for (Long2LongMap.Entry entry : map.long2LongEntrySet()) {
        buffer.putLong(entry.getLongKey());
        buffer.putLong(entry.getLongValue());
      }
    }

    @Override
    public void serialize(Long2LongMap map, PinotOutputStream output)
        throws IOException {
      output.writeInt(map.size());
      for (Long2LongMap.Entry entry : map.long2LongEntrySet()) {
        output.writeLong(entry.getLongKey());
        output.writeLong(entry.getLongValue());
      }
    }

    @Override
    public Long2LongOpenHashMap deserialize(ByteBuffer buffer) {
      int size = buffer.getInt();
      Long2LongOpenHashMap map = new Long2LongOpenHashMap(size);
      for (int i = 0; i < size; i++) {
        map.put(buffer.getLong(), buffer.getLong());
      }
      return map;
    }
  };

  public static final SizedObjectSerDe<Float2LongMap> FLOAT_2_LONG_MAP_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(Float2LongMap map) {
      return Math.addExact(Integer.BYTES, Math.multiplyExact(map.size(), Float.BYTES + Long.BYTES));
    }

    @Override
    public void serialize(Float2LongMap map, ByteBuffer buffer) {
      buffer.putInt(map.size());
      for (Float2LongMap.Entry entry : map.float2LongEntrySet()) {
        buffer.putFloat(entry.getFloatKey());
        buffer.putLong(entry.getLongValue());
      }
    }

    @Override
    public void serialize(Float2LongMap map, PinotOutputStream output)
        throws IOException {
      output.writeInt(map.size());
      for (Float2LongMap.Entry entry : map.float2LongEntrySet()) {
        output.writeInt(Float.floatToRawIntBits(entry.getFloatKey()));
        output.writeLong(entry.getLongValue());
      }
    }

    @Override
    public Float2LongOpenHashMap deserialize(ByteBuffer buffer) {
      int size = buffer.getInt();
      Float2LongOpenHashMap map = new Float2LongOpenHashMap(size);
      for (int i = 0; i < size; i++) {
        map.put(buffer.getFloat(), buffer.getLong());
      }
      return map;
    }
  };

  public static final SizedObjectSerDe<Double2LongMap> DOUBLE_2_LONG_MAP_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(Double2LongMap map) {
      return Math.addExact(Integer.BYTES, Math.multiplyExact(map.size(), Double.BYTES + Long.BYTES));
    }

    @Override
    public void serialize(Double2LongMap map, ByteBuffer buffer) {
      buffer.putInt(map.size());
      for (Double2LongMap.Entry entry : map.double2LongEntrySet()) {
        buffer.putDouble(entry.getDoubleKey());
        buffer.putLong(entry.getLongValue());
      }
    }

    @Override
    public void serialize(Double2LongMap map, PinotOutputStream output)
        throws IOException {
      output.writeInt(map.size());
      for (Double2LongMap.Entry entry : map.double2LongEntrySet()) {
        output.writeLong(Double.doubleToRawLongBits(entry.getDoubleKey()));
        output.writeLong(entry.getLongValue());
      }
    }

    @Override
    public Double2LongOpenHashMap deserialize(ByteBuffer buffer) {
      int size = buffer.getInt();
      Double2LongOpenHashMap map = new Double2LongOpenHashMap(size);
      for (int i = 0; i < size; i++) {
        map.put(buffer.getDouble(), buffer.getLong());
      }
      return map;
    }
  };

  public static final SizedObjectSerDe<Object2LongMap<String>> STRING_2_LONG_MAP_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(Object2LongMap<String> map) {
      // Besides the key bytes, we store: size, length for each key, value for each key
      int size = Math.addExact(Integer.BYTES, Math.multiplyExact(map.size(), Integer.BYTES + Long.BYTES));
      for (String key : map.keySet()) {
        size = Math.addExact(size, Utf8Utils.encodedLength(key));
      }
      return size;
    }

    @Override
    public void serialize(Object2LongMap<String> map, ByteBuffer buffer) {
      buffer.putInt(map.size());
      for (Object2LongMap.Entry<String> entry : map.object2LongEntrySet()) {
        byte[] keyBytes = Utf8Utils.encode(entry.getKey());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putLong(entry.getLongValue());
      }
    }

    @Override
    public void serialize(Object2LongMap<String> map, PinotOutputStream output)
        throws IOException {
      output.writeInt(map.size());
      for (Object2LongMap.Entry<String> entry : map.object2LongEntrySet()) {
        byte[] keyBytes = Utf8Utils.encode(entry.getKey());
        output.writeInt(keyBytes.length);
        output.write(keyBytes);
        output.writeLong(entry.getLongValue());
      }
    }

    @Override
    public Object2LongOpenHashMap<String> deserialize(ByteBuffer buffer) {
      int size = buffer.getInt();
      Object2LongOpenHashMap<String> map = new Object2LongOpenHashMap<>(size);
      for (int i = 0; i < size; i++) {
        byte[] keyBytes = new byte[buffer.getInt()];
        buffer.get(keyBytes);
        map.put(Utf8Utils.decode(keyBytes), buffer.getLong());
      }
      return map;
    }
  };

  public static final SizedObjectSerDe<HyperLogLog> HYPER_LOG_LOG_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(HyperLogLog hyperLogLog) {
      return HyperLogLogUtils.byteSize(hyperLogLog);
    }

    // The registers are not accessible, so the payload goes through the array form
    @Override
    public void serialize(HyperLogLog hyperLogLog, ByteBuffer buffer) {
      buffer.put(serialize(hyperLogLog));
    }

    @Override
    public byte[] serialize(HyperLogLog hyperLogLog) {
      try {
        return hyperLogLog.getBytes();
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while serializing HyperLogLog", e);
      }
    }

    @Override
    public HyperLogLog deserialize(ByteBuffer buffer) {
      // NOTE: The passed in byte buffer is always BIG ENDIAN
      return deserialize(buffer.asIntBuffer());
    }

    private HyperLogLog deserialize(IntBuffer intBuffer) {
      // The first 2 integers are constant headers for the HLL: log2m and size in bytes
      int log2m = intBuffer.get();
      int numBits = intBuffer.get() >>> 2;
      int[] bits = new int[numBits];
      intBuffer.get(bits);
      return new HyperLogLog(log2m, new RegisterSet(1 << log2m, bits));
    }
  };

  public static final ObjectSerDe<HyperLogLogPlus> HYPER_LOG_LOG_PLUS_SER_DE = new ObjectSerDe<>() {

    @Override
    public byte[] serialize(HyperLogLogPlus hyperLogLogPlus) {
      try {
        return hyperLogLogPlus.getBytes();
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while serializing HyperLogLogPlus", e);
      }
    }

    @Override
    public HyperLogLogPlus deserialize(ByteBuffer buffer) {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return deserialize(bytes);
    }

    @Override
    public HyperLogLogPlus deserialize(byte[] bytes) {
      try {
        return HyperLogLogPlus.Builder.build(bytes);
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while deserializing HyperLogLogPlus", e);
      }
    }
  };

  public static final SizedObjectSerDe<UltraLogLog> ULTRA_LOG_LOG_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(UltraLogLog value) {
      return Math.addExact(1, value.getState().length);
    }

    @Override
    public void serialize(UltraLogLog value, ByteBuffer buffer) {
      buffer.put((byte) value.getP());
      buffer.put(value.getState());
    }

    @Override
    public void serialize(UltraLogLog value, PinotOutputStream output)
        throws IOException {
      output.writeByte(value.getP());
      output.write(value.getState());
    }

    @Override
    public UltraLogLog deserialize(ByteBuffer buffer) {
      byte p = buffer.get();
      byte[] state = new byte[1 << p];
      buffer.get(state);
      return UltraLogLog.wrap(state);
    }
  };

  /// Serialized form of every empty theta sketch, as compacting one on the heap produces. Compacting an empty sketch
  /// into memory instead writes the seed hash, and sizes a p-sampled one larger than it writes.
  private static final byte[] EMPTY_THETA_SKETCH_BYTES =
      UpdatableThetaSketch.builder().build().compact(true, null).toByteArray();

  public static final SizedObjectSerDe<ThetaSketch> DATA_SKETCH_THETA_SER_DE = new SizedObjectSerDe<>() {

    @Override
    public int getSerializedSize(ThetaSketch value) {
      if (value.isCompact() && value.hasMemorySegment()) {
        return value.getCurrentBytes();
      }
      return value.isEmpty() ? EMPTY_THETA_SKETCH_BYTES.length : value.getCompactBytes();
    }

    @Override
    public void serialize(ThetaSketch value, ByteBuffer buffer) {
      // A compact sketch wrapped over its serialized bytes is written as it is: compacting it again can rewrite
      // preamble fields that older serialization versions set differently
      if (value.isCompact() && value.hasMemorySegment()) {
        buffer.put(value.toByteArray());
        return;
      }
      if (value.isEmpty()) {
        buffer.put(EMPTY_THETA_SKETCH_BYTES);
        return;
      }
      // The sketch keeps its ordering, which enables the "early stop" optimization of unions
      int size = value.getCompactBytes();
      value.compact(value.isOrdered(), MemorySegment.ofBuffer(buffer));
      buffer.position(buffer.position() + size);
    }

    @Override
    public ThetaSketch deserialize(ByteBuffer buffer) {
      return ThetaSketch.heapify(MemorySegment.ofBuffer(buffer));
    }

    @Override
    public ThetaSketch deserialize(byte[] bytes) {
      return ThetaSketch.wrap(MemorySegment.ofArray(bytes));
    }
  };

  // Intermediate results use reduction compression; stored sketches use the native legacy fromBytes factory.
  public static final ObjectSerDe<TDigest> TDIGEST_SER_DE = new ObjectSerDe<>() {

    @Override
    public byte[] serialize(TDigest tDigest) {
      return tDigest.serialize();
    }

    @Override
    public TDigest deserialize(ByteBuffer buffer) {
      SerializedTDigestInput input = new SerializedTDigestInput();
      input.reset(buffer);
      TDigestAccumulator accumulator = TDigestAccumulator.forReduction(input.getCompression());
      accumulator.addSerializedTDigest(input);
      return accumulator;
    }
  };
}
