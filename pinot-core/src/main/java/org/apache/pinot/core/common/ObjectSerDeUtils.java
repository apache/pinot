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
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.RegisterSet;
import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
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
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.frequencies.LongsSketch;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummaryDeserializer;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.query.aggregation.utils.exprminmax.ExprMinMaxObject;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.local.customobject.CovarianceTuple;
import org.apache.pinot.segment.local.customobject.DoubleLongPair;
import org.apache.pinot.segment.local.customobject.FloatLongPair;
import org.apache.pinot.segment.local.customobject.IntLongPair;
import org.apache.pinot.segment.local.customobject.LongLongPair;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.segment.local.customobject.PinotFourthMoment;
import org.apache.pinot.segment.local.customobject.QuantileDigest;
import org.apache.pinot.segment.local.customobject.StringLongPair;
import org.apache.pinot.segment.local.customobject.ThetaUnionWrap;
import org.apache.pinot.segment.local.customobject.VarianceTuple;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.locationtech.jts.geom.Geometry;
import org.roaringbitmap.RoaringBitmap;

import static java.nio.charset.StandardCharsets.UTF_8;


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
    IntLongPair(27),
    LongLongPair(28),
    FloatLongPair(29),
    DoubleLongPair(30),
    StringLongPair(31),
    CovarianceTuple(32),
    VarianceTuple(33),
    PinotFourthMoment(34),
    ExprMinMaxObject(35),
    KllDataSketch(36),
    IntegerTupleSketch(37),
    FrequentStringsSketch(38),
    FrequentLongsSketch(39),
    HyperLogLogPlus(40),
    CompressedProbabilisticCounting(41),
    IntArrayList(42),
    LongArrayList(43),
    FloatArrayList(44),
    StringArrayList(45),
    UltraLogLog(46),
    ThetaUnionWrap(47);

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
      } else if (value instanceof IntArrayList) {
        return ObjectType.IntArrayList;
      } else if (value instanceof LongArrayList) {
        return ObjectType.LongArrayList;
      } else if (value instanceof FloatArrayList) {
        return ObjectType.FloatArrayList;
      } else if (value instanceof DoubleArrayList) {
        return ObjectType.DoubleArrayList;
      } else if (value instanceof ObjectArrayList) {
        ObjectArrayList objectArrayList = (ObjectArrayList) value;
        if (!objectArrayList.isEmpty()) {
          Object next = objectArrayList.get(0);
          if (next instanceof String) {
            return ObjectType.StringArrayList;
          }
          throw new IllegalArgumentException(
              "Unsupported type of value: " + next.getClass().getSimpleName());
        }
        return ObjectType.StringArrayList;
      } else if (value instanceof AvgPair) {
        return ObjectType.AvgPair;
      } else if (value instanceof MinMaxRangePair) {
        return ObjectType.MinMaxRangePair;
      } else if (value instanceof HyperLogLog) {
        return ObjectType.HyperLogLog;
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
      } else if (value instanceof KllDoublesSketch) {
        return ObjectType.KllDataSketch;
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
      } else if (value instanceof IntLongPair) {
        return ObjectType.IntLongPair;
      } else if (value instanceof LongLongPair) {
        return ObjectType.LongLongPair;
      } else if (value instanceof FloatLongPair) {
        return ObjectType.FloatLongPair;
      } else if (value instanceof DoubleLongPair) {
        return ObjectType.DoubleLongPair;
      } else if (value instanceof StringLongPair) {
        return ObjectType.StringLongPair;
      } else if (value instanceof CovarianceTuple) {
        return ObjectType.CovarianceTuple;
      } else if (value instanceof VarianceTuple) {
        return ObjectType.VarianceTuple;
      } else if (value instanceof PinotFourthMoment) {
        return ObjectType.PinotFourthMoment;
      } else if (value instanceof org.apache.datasketches.tuple.Sketch) {
        return ObjectType.IntegerTupleSketch;
      } else if (value instanceof ExprMinMaxObject) {
        return ObjectType.ExprMinMaxObject;
      } else if (value instanceof ItemsSketch) {
        return ObjectType.FrequentStringsSketch;
      } else if (value instanceof LongsSketch) {
        return ObjectType.FrequentLongsSketch;
      } else if (value instanceof HyperLogLogPlus) {
        return ObjectType.HyperLogLogPlus;
      } else if (value instanceof CpcSketch) {
        return ObjectType.CompressedProbabilisticCounting;
      } else if (value instanceof UltraLogLog) {
        return ObjectType.UltraLogLog;
      } else if (value instanceof ThetaUnionWrap) {
        return ObjectType.ThetaUnionWrap;
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
      return value.getBytes(UTF_8);
    }

    @Override
    public String deserialize(byte[] bytes) {
      return new String(bytes, UTF_8);
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

  public static final ObjectSerDe<IntArrayList> INT_ARRAY_LIST_SER_DE = new ObjectSerDe<IntArrayList>() {

    @Override
    public byte[] serialize(IntArrayList intArrayList) {
      int size = intArrayList.size();
      byte[] bytes = new byte[Integer.BYTES + size * Integer.BYTES];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      int[] values = intArrayList.elements();
      for (int i = 0; i < size; i++) {
        byteBuffer.putInt(values[i]);
      }
      return bytes;
    }

    @Override
    public IntArrayList deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public IntArrayList deserialize(ByteBuffer byteBuffer) {
      int numValues = byteBuffer.getInt();
      IntArrayList intArrayList = new IntArrayList(numValues);
      for (int i = 0; i < numValues; i++) {
        intArrayList.add(byteBuffer.getInt());
      }
      return intArrayList;
    }
  };

  public static final ObjectSerDe<LongArrayList> LONG_ARRAY_LIST_SER_DE = new ObjectSerDe<LongArrayList>() {

    @Override
    public byte[] serialize(LongArrayList longArrayList) {
      int size = longArrayList.size();
      byte[] bytes = new byte[Integer.BYTES + size * Long.BYTES];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      long[] values = longArrayList.elements();
      for (int i = 0; i < size; i++) {
        byteBuffer.putLong(values[i]);
      }
      return bytes;
    }

    @Override
    public LongArrayList deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public LongArrayList deserialize(ByteBuffer byteBuffer) {
      int numValues = byteBuffer.getInt();
      LongArrayList longArrayList = new LongArrayList(numValues);
      for (int i = 0; i < numValues; i++) {
        longArrayList.add(byteBuffer.getLong());
      }
      return longArrayList;
    }
  };

  public static final ObjectSerDe<FloatArrayList> FLOAT_ARRAY_LIST_SER_DE = new ObjectSerDe<FloatArrayList>() {

    @Override
    public byte[] serialize(FloatArrayList floatArrayList) {
      int size = floatArrayList.size();
      byte[] bytes = new byte[Integer.BYTES + size * Float.BYTES];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      float[] values = floatArrayList.elements();
      for (int i = 0; i < size; i++) {
        byteBuffer.putFloat(values[i]);
      }
      return bytes;
    }

    @Override
    public FloatArrayList deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public FloatArrayList deserialize(ByteBuffer byteBuffer) {
      int numValues = byteBuffer.getInt();
      FloatArrayList floatArrayList = new FloatArrayList(numValues);
      for (int i = 0; i < numValues; i++) {
        floatArrayList.add(byteBuffer.getFloat());
      }
      return floatArrayList;
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

  public static final ObjectSerDe<ObjectArrayList> STRING_ARRAY_LIST_SER_DE =
      new ObjectSerDe<ObjectArrayList>() {
        @Override
        public byte[] serialize(ObjectArrayList stringArrayList) {
          int size = stringArrayList.size();
          // Besides the value bytes, we store: size, length for each value
          long bufferSize = (1 + (long) size) * Integer.BYTES;
          byte[][] valueBytesArray = new byte[size][];
          for (int index = 0; index < size; index++) {
            Object value = stringArrayList.get(index);
            byte[] valueBytes = value.toString().getBytes(UTF_8);
            bufferSize += valueBytes.length;
            valueBytesArray[index] = valueBytes;
          }
          Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");
          byte[] bytes = new byte[(int) bufferSize];
          ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
          byteBuffer.putInt(size);
          for (byte[] valueBytes : valueBytesArray) {
            byteBuffer.putInt(valueBytes.length);
            byteBuffer.put(valueBytes);
          }
          return bytes;
        }

        @Override
        public ObjectArrayList deserialize(byte[] bytes) {
          return deserialize(ByteBuffer.wrap(bytes));
        }

        @Override
        public ObjectArrayList deserialize(ByteBuffer byteBuffer) {
          int size = byteBuffer.getInt();
          ObjectArrayList stringArrayList = new ObjectArrayList(size);
          for (int i = 0; i < size; i++) {
            int length = byteBuffer.getInt();
            byte[] valueBytes = new byte[length];
            byteBuffer.get(valueBytes);
            stringArrayList.add(new String(valueBytes, UTF_8));
          }
          return stringArrayList;
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

  public static final ObjectSerDe<IntLongPair> INT_LONG_PAIR_SER_DE = new ObjectSerDe<IntLongPair>() {

    @Override
    public byte[] serialize(IntLongPair intLongPair) {
      return intLongPair.toBytes();
    }

    @Override
    public IntLongPair deserialize(byte[] bytes) {
      return IntLongPair.fromBytes(bytes);
    }

    @Override
    public IntLongPair deserialize(ByteBuffer byteBuffer) {
      return IntLongPair.fromByteBuffer(byteBuffer);
    }
  };

  public static final ObjectSerDe<LongLongPair> LONG_LONG_PAIR_SER_DE = new ObjectSerDe<LongLongPair>() {

    @Override
    public byte[] serialize(LongLongPair longLongPair) {
      return longLongPair.toBytes();
    }

    @Override
    public LongLongPair deserialize(byte[] bytes) {
      return LongLongPair.fromBytes(bytes);
    }

    @Override
    public LongLongPair deserialize(ByteBuffer byteBuffer) {
      return LongLongPair.fromByteBuffer(byteBuffer);
    }
  };

  public static final ObjectSerDe<FloatLongPair> FLOAT_LONG_PAIR_SER_DE = new ObjectSerDe<FloatLongPair>() {

    @Override
    public byte[] serialize(FloatLongPair floatLongPair) {
      return floatLongPair.toBytes();
    }

    @Override
    public FloatLongPair deserialize(byte[] bytes) {
      return FloatLongPair.fromBytes(bytes);
    }

    @Override
    public FloatLongPair deserialize(ByteBuffer byteBuffer) {
      return FloatLongPair.fromByteBuffer(byteBuffer);
    }
  };
  public static final ObjectSerDe<DoubleLongPair> DOUBLE_LONG_PAIR_SER_DE = new ObjectSerDe<DoubleLongPair>() {

    @Override
    public byte[] serialize(DoubleLongPair doubleLongPair) {
      return doubleLongPair.toBytes();
    }

    @Override
    public DoubleLongPair deserialize(byte[] bytes) {
      return DoubleLongPair.fromBytes(bytes);
    }

    @Override
    public DoubleLongPair deserialize(ByteBuffer byteBuffer) {
      return DoubleLongPair.fromByteBuffer(byteBuffer);
    }
  };
  public static final ObjectSerDe<StringLongPair> STRING_LONG_PAIR_SER_DE = new ObjectSerDe<StringLongPair>() {

    @Override
    public byte[] serialize(StringLongPair stringLongPair) {
      return stringLongPair.toBytes();
    }

    @Override
    public StringLongPair deserialize(byte[] bytes) {
      return StringLongPair.fromBytes(bytes);
    }

    @Override
    public StringLongPair deserialize(ByteBuffer byteBuffer) {
      return StringLongPair.fromByteBuffer(byteBuffer);
    }
  };

  public static final ObjectSerDe<CovarianceTuple> COVARIANCE_TUPLE_OBJECT_SER_DE = new ObjectSerDe<CovarianceTuple>() {
    @Override
    public byte[] serialize(CovarianceTuple covarianceTuple) {
      return covarianceTuple.toBytes();
    }

    @Override
    public CovarianceTuple deserialize(byte[] bytes) {
      return CovarianceTuple.fromBytes(bytes);
    }

    @Override
    public CovarianceTuple deserialize(ByteBuffer byteBuffer) {
      return CovarianceTuple.fromByteBuffer(byteBuffer);
    }
  };

  public static final ObjectSerDe<VarianceTuple> VARIANCE_TUPLE_OBJECT_SER_DE = new ObjectSerDe<VarianceTuple>() {
    @Override
    public byte[] serialize(VarianceTuple varianceTuple) {
      return varianceTuple.toBytes();
    }

    @Override
    public VarianceTuple deserialize(byte[] bytes) {
      return VarianceTuple.fromBytes(bytes);
    }

    @Override
    public VarianceTuple deserialize(ByteBuffer byteBuffer) {
      return VarianceTuple.fromByteBuffer(byteBuffer);
    }
  };

  public static final ObjectSerDe<PinotFourthMoment> PINOT_FOURTH_MOMENT_OBJECT_SER_DE
      = new ObjectSerDe<PinotFourthMoment>() {
    @Override
    public byte[] serialize(PinotFourthMoment value) {
      return value.serialize();
    }

    @Override
    public PinotFourthMoment deserialize(byte[] bytes) {
      return PinotFourthMoment.fromBytes(bytes);
    }

    @Override
    public PinotFourthMoment deserialize(ByteBuffer byteBuffer) {
      return PinotFourthMoment.fromBytes(byteBuffer);
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
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public HyperLogLog deserialize(ByteBuffer byteBuffer) {
      // NOTE: The passed in byte buffer is always BIG ENDIAN
      return deserialize(byteBuffer.asIntBuffer());
    }

    private HyperLogLog deserialize(IntBuffer intBuffer) {
      try {
        // The first 2 integers are constant headers for the HLL: log2m and size in bytes
        int log2m = intBuffer.get();
        int numBits = intBuffer.get() >>> 2;
        int[] bits = new int[numBits];
        intBuffer.get(bits);
        return new HyperLogLog(log2m, new RegisterSet(1 << log2m, bits));
      } catch (RuntimeException e) {
        throw new RuntimeException("Caught exception while deserializing HyperLogLog", e);
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
        throw new RuntimeException("Caught exception while serializing HyperLogLogPlus", e);
      }
    }

    @Override
    public HyperLogLogPlus deserialize(ByteBuffer byteBuffer) {
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return deserialize(bytes);
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

      // Besides the value bytes, we store: size, key type, value type, length for each key, length for each value
      long bufferSize = (3 + 2 * (long) size) * Integer.BYTES;
      byte[][] keyBytesArray = new byte[size][];
      byte[][] valueBytesArray = new byte[size][];
      Map.Entry<Object, Object> firstEntry = map.entrySet().iterator().next();
      int keyTypeValue = ObjectType.getObjectType(firstEntry.getKey()).getValue();
      int valueTypeValue = ObjectType.getObjectType(firstEntry.getValue()).getValue();
      ObjectSerDe keySerDe = SER_DES[keyTypeValue];
      ObjectSerDe valueSerDe = SER_DES[valueTypeValue];
      int index = 0;
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        byte[] keyBytes = keySerDe.serialize(entry.getKey());
        bufferSize += keyBytes.length;
        keyBytesArray[index] = keyBytes;
        byte[] valueBytes = valueSerDe.serialize(entry.getValue());
        bufferSize += valueBytes.length;
        valueBytesArray[index++] = valueBytes;
      }
      Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");
      byte[] bytes = new byte[(int) bufferSize];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      byteBuffer.putInt(keyTypeValue);
      byteBuffer.putInt(valueTypeValue);
      for (int i = 0; i < index; i++) {
        byte[] keyBytes = keyBytesArray[i];
        byteBuffer.putInt(keyBytes.length);
        byteBuffer.put(keyBytes);
        byte[] valueBytes = valueBytesArray[i];
        byteBuffer.putInt(valueBytes.length);
        byteBuffer.put(valueBytes);
      }
      return bytes;
    }

    @Override
    public HashMap<Object, Object> deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public HashMap<Object, Object> deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.getInt();
      HashMap<Object, Object> map = new HashMap<>(HashUtil.getHashMapCapacity(size));
      if (size == 0) {
        return map;
      }

      // De-serialize each key-value pair
      ObjectSerDe keySerDe = SER_DES[byteBuffer.getInt()];
      ObjectSerDe valueSerDe = SER_DES[byteBuffer.getInt()];
      for (int i = 0; i < size; i++) {
        Object key = keySerDe.deserialize(sliceByteBuffer(byteBuffer, byteBuffer.getInt()));
        Object value = valueSerDe.deserialize(sliceByteBuffer(byteBuffer, byteBuffer.getInt()));
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
      // Besides the value bytes, we store: size, length for each value
      long bufferSize = (1 + (long) size) * Integer.BYTES;
      byte[][] valueBytesArray = new byte[size][];
      int index = 0;
      for (String value : stringSet) {
        byte[] valueBytes = value.getBytes(UTF_8);
        bufferSize += valueBytes.length;
        valueBytesArray[index++] = valueBytes;
      }
      Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");
      byte[] bytes = new byte[(int) bufferSize];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      for (byte[] valueBytes : valueBytesArray) {
        byteBuffer.putInt(valueBytes.length);
        byteBuffer.put(valueBytes);
      }
      return bytes;
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
        stringSet.add(new String(bytes, UTF_8));
      }
      return stringSet;
    }
  };

  public static final ObjectSerDe<Set<ByteArray>> BYTES_SET_SER_DE = new ObjectSerDe<Set<ByteArray>>() {

    @Override
    public byte[] serialize(Set<ByteArray> bytesSet) {
      int size = bytesSet.size();
      // Besides the value bytes, we store: size, length for each value
      long bufferSize = (1 + (long) size) * Integer.BYTES;
      for (ByteArray value : bytesSet) {
        bufferSize += value.length();
      }
      Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");
      byte[] bytes = new byte[(int) bufferSize];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      for (ByteArray value : bytesSet) {
        byte[] valueBytes = value.getBytes();
        byteBuffer.putInt(valueBytes.length);
        byteBuffer.put(valueBytes);
      }
      return bytes;
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

  public static final ObjectSerDe<Sketch> DATA_SKETCH_THETA_SER_DE = new ObjectSerDe<Sketch>() {

    @Override
    public byte[] serialize(Sketch value) {
      // The serializer should respect existing ordering to enable "early stop"
      // optimisations on unions.
      boolean shouldCompact = !value.isCompact();
      boolean shouldOrder = value.isOrdered();

      if (shouldCompact) {
        return value.compact(shouldOrder, null).toByteArray();
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

  public static final ObjectSerDe<KllDoublesSketch> KLL_SKETCH_SER_DE = new ObjectSerDe<KllDoublesSketch>() {

    @Override
    public byte[] serialize(KllDoublesSketch value) {
      return value.toByteArray();
    }

    @Override
    public KllDoublesSketch deserialize(byte[] bytes) {
      return KllDoublesSketch.wrap(Memory.wrap(bytes));
    }

    @Override
    public KllDoublesSketch deserialize(ByteBuffer byteBuffer) {
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return KllDoublesSketch.wrap(Memory.wrap(bytes));
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

      // Besides the value bytes, we store: size, value type, length for each value
      long bufferSize = (2 + (long) size) * Integer.BYTES;
      byte[][] valueBytesArray = new byte[size][];
      int valueType = ObjectType.getObjectType(list.get(0)).getValue();
      ObjectSerDe serDe = SER_DES[valueType];
      int index = 0;
      for (Object value : list) {
        byte[] valueBytes = serDe.serialize(value);
        bufferSize += valueBytes.length;
        valueBytesArray[index++] = valueBytes;
      }
      Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");
      byte[] bytes = new byte[(int) bufferSize];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      byteBuffer.putInt(valueType);
      for (byte[] valueBytes : valueBytesArray) {
        byteBuffer.putInt(valueBytes.length);
        byteBuffer.put(valueBytes);
      }
      return bytes;
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
        ObjectSerDe serDe = SER_DES[byteBuffer.getInt()];
        for (int i = 0; i < size; i++) {
          int numBytes = byteBuffer.getInt();
          ByteBuffer slice = byteBuffer.slice();
          slice.limit(numBytes);
          list.add(serDe.deserialize(slice));
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

  public static final ObjectSerDe<ExprMinMaxObject> ARG_MIN_MAX_OBJECT_SER_DE =
      new ObjectSerDe<ExprMinMaxObject>() {

        @Override
        public byte[] serialize(ExprMinMaxObject value) {
          try {
            return value.toBytes();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public ExprMinMaxObject deserialize(byte[] bytes) {
          try {
            return ExprMinMaxObject.fromBytes(bytes);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public ExprMinMaxObject deserialize(ByteBuffer byteBuffer) {
          try {
            return ExprMinMaxObject.fromByteBuffer(byteBuffer);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      };

  public static final ObjectSerDe<ItemsSketch<String>> FREQUENT_STRINGS_SKETCH_SER_DE =
      new ObjectSerDe<>() {
        @Override
        public byte[] serialize(ItemsSketch<String> sketch) {
          return sketch.toByteArray(new ArrayOfStringsSerDe());
        }

        @Override
        public ItemsSketch<String> deserialize(byte[] bytes) {
          return ItemsSketch.getInstance(Memory.wrap(bytes), new ArrayOfStringsSerDe());
        }

        @Override
        public ItemsSketch<String> deserialize(ByteBuffer byteBuffer) {
          byte[] arr = new byte[byteBuffer.remaining()];
          byteBuffer.get(arr);
          return ItemsSketch.getInstance(Memory.wrap(arr), new ArrayOfStringsSerDe());
        }
      };

  public static final ObjectSerDe<LongsSketch> FREQUENT_LONGS_SKETCH_SER_DE =
      new ObjectSerDe<>() {
        @Override
        public byte[] serialize(LongsSketch sketch) {
          return sketch.toByteArray();
        }

        @Override
        public LongsSketch deserialize(byte[] bytes) {
          return LongsSketch.getInstance(Memory.wrap(bytes));
        }

        @Override
        public LongsSketch deserialize(ByteBuffer byteBuffer) {
          byte[] arr = new byte[byteBuffer.remaining()];
          byteBuffer.get(arr);
          return LongsSketch.getInstance(Memory.wrap(arr));
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

  public static final ObjectSerDe<ThetaUnionWrap> DATA_SKETCH_THETA_UNION_WRAP_SER_DE =
      new ObjectSerDe<ThetaUnionWrap>() {

        @Override
        public byte[] serialize(ThetaUnionWrap thetaUnionWrap) {
          return thetaUnionWrap.toBytes();
        }

        @Override
        public ThetaUnionWrap deserialize(byte[] bytes) {
          return ThetaUnionWrap.fromBytes(bytes);
        }

        public ThetaUnionWrap deserialize(ByteBuffer byteBuffer) {
          return ThetaUnionWrap.fromByteBuffer(byteBuffer);
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
      DATA_SKETCH_THETA_SER_DE,
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
      INT_LONG_PAIR_SER_DE,
      LONG_LONG_PAIR_SER_DE,
      FLOAT_LONG_PAIR_SER_DE,
      DOUBLE_LONG_PAIR_SER_DE,
      STRING_LONG_PAIR_SER_DE,
      COVARIANCE_TUPLE_OBJECT_SER_DE,
      VARIANCE_TUPLE_OBJECT_SER_DE,
      PINOT_FOURTH_MOMENT_OBJECT_SER_DE,
      ARG_MIN_MAX_OBJECT_SER_DE,
      KLL_SKETCH_SER_DE,
      DATA_SKETCH_INT_TUPLE_SER_DE,
      FREQUENT_STRINGS_SKETCH_SER_DE,
      FREQUENT_LONGS_SKETCH_SER_DE,
      HYPER_LOG_LOG_PLUS_SER_DE,
      DATA_SKETCH_CPC_SER_DE,
      INT_ARRAY_LIST_SER_DE,
      LONG_ARRAY_LIST_SER_DE,
      FLOAT_ARRAY_LIST_SER_DE,
      STRING_ARRAY_LIST_SER_DE,
      ULTRA_LOG_LOG_OBJECT_SER_DE,
      DATA_SKETCH_THETA_UNION_WRAP_SER_DE,
  };
  //@formatter:on

  public static byte[] serialize(Object value, int objectTypeValue) {
    return SER_DES[objectTypeValue].serialize(value);
  }

  public static <T> T deserialize(CustomObject customObject) {
    return (T) SER_DES[customObject.getType()].deserialize(customObject.getBuffer());
  }

  @VisibleForTesting
  public static byte[] serialize(Object value) {
    return serialize(value, ObjectType.getObjectType(value)._value);
  }

  @VisibleForTesting
  public static <T> T deserialize(byte[] bytes, ObjectType objectType) {
    return (T) SER_DES[objectType._value].deserialize(bytes);
  }
}
