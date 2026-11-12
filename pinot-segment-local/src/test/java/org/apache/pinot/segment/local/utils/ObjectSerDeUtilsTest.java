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
import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import it.unimi.dsi.fastutil.doubles.Double2LongMap;
import it.unimi.dsi.fastutil.doubles.Double2LongOpenHashMap;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.Float2LongMap;
import it.unimi.dsi.fastutil.floats.Float2LongOpenHashMap;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.datasketches.theta.CompactThetaSketch;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.pinot.segment.local.customobject.tdigest.SerializedTDigestInput;
import org.apache.pinot.segment.local.customobject.tdigest.TDigest;
import org.apache.pinot.segment.local.customobject.tdigest.TDigest.Centroid;
import org.apache.pinot.segment.local.customobject.tdigest.TDigestAccumulator;
import org.apache.pinot.segment.local.utils.ObjectSerDeUtils.SizedObjectSerDe;
import org.apache.pinot.segment.spi.memory.PagedPinotOutputStream;
import org.apache.pinot.segment.spi.memory.PagedPinotOutputStream.HeapPageAllocator;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class ObjectSerDeUtilsTest {
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;

  private static final int NUM_ITERATIONS = 100;
  // Small, so a large value spans pages
  private static final int PAGE_SIZE = 1000;
  private static final String TDIGEST_3_2_VERBOSE_COMPRESSION_20 =
      "AAAAAT+5mZmZmZmaP+zMzMzMzM1ANAAAAAAAAAAAABtAKgAAAAAAAD+5mZmZmZmaQBgAAAAAAAA/uZmZmZmZmkAcAAAAAAAAP7mZ"
          + "mZmZmZpAHAAAAAAAAD+5mZmZmZmaQBwAAAAAAAA/uZmZmZmZmkAmAAAAAAAAP7mZmZmZmZpAKgAAAAAAAD+5mZmZmZmaQCYAAAAA"
          + "AAA/uZmZmZmZmkAmAAAAAAAAP7mZmZmZmZpAJAAAAAAAAD+5mZmZmZmaQC4AAAAAAAA/uZmZmZmZmkAmAAAAAAAAP7mZmZmZmZpA"
          + "LAAAAAAAAD+5mZmZmZmaQCIAAAAAAAA/uZmZmZmZmkAyAAAAAAAAP8VVVVVVVVZAMQAAAAAAAD/gAAAAAAAAQCwAAAAAAAA/4AAA"
          + "AAAAAEAsAAAAAAAAP+AAAAAAAABAKAAAAAAAAD/gAAAAAAAAQCQAAAAAAAA/564UeuFHrkAUAAAAAAAAP+zMzMzMzM1AFAAAAAAA"
          + "AD/szMzMzMzNQBwAAAAAAAA/7MzMzMzMzUAQAAAAAAAAP+zMzMzMzM1ACAAAAAAAAD/szMzMzMzNP/AAAAAAAAA/7MzMzMzMzT/w"
          + "AAAAAAAAP+zMzMzMzM0=";
  private static final String TDIGEST_3_2_SMALL_COMPRESSION_1000 =
      "AAAAAj+INkk/eVmAQJLlmhJu+WhEegAAB9oTiABAP4AAADxBsko/gAAAPRQvNz+AAAA9fAMgP4AAAD20Elw/gAAAPexqgT+AAAA+"
          + "E5aAP4AAAD4yP88/gAAAPlJFjz+AAAA+c73OP4AAAD6LYDU/gAAAPp2zoz+AAAA+sOdBP4AAAD7FClg/gAAAPtotkD+AAAA+8GMX"
          + "P4AAAD8D32Y/gAAAPxArOT+AAAA/HSDzP4AAAD8qzbU/gAAAPzk/8j+AAAA/SIegP4AAAD9YtmU/gAAAP2nf1D+AAAA/fBmxP4AA"
          + "AD+Hvh4/gAAAP5IRRj+AAAA/nRWBP4AAAD+o294/gAAAP7V3mj+AAAA/wv55P4AAAD/RiSw/gAAAP+Ez2T+AAAA/8h6sP4AAAEAC"
          + "N00/gAAAQAwnIz+AAABAFveKP4AAAEAixUk/gAAAQC+yDj+AAABAPeWGP4AAAEBNjrY/gAAAQF7lsz+AAABAci3iP4AAAECD3G8/"
          + "gAAAQI/1LD+AAABAnZ6aP4AAAECtJUM/gAAAQL7pkj+AAABA02aDP4AAAEDrOzE/gAAAQQOcVz+AAABBFDtQP4AAAEEoOWA/gAAA"
          + "QUChLz+AAABBXvGHP4AAAEGCsOA/gAAAQZutuD+AAABBvS/yP4AAAEHr7EA/gAAAQhhGxT+AAABCTlsKP4AAAEKWasI/gAAAQvfH"
          + "yz+AAABDgpJvP4AAAESXLNE=";

  @DataProvider(name = "directSerializationValues")
  public Object[][] directSerializationValues()
      throws IOException {
    HyperLogLog hyperLogLog = new HyperLogLog(8);
    for (int i = 0; i < 100; i++) {
      hyperLogLog.offer(i);
    }
    IntArrayList largeIntList = new IntArrayList();
    LongArrayList largeLongList = new LongArrayList();
    FloatArrayList largeFloatList = new FloatArrayList();
    DoubleArrayList largeDoubleList = new DoubleArrayList();
    for (int i = 0; i < 3000; i++) {
      largeIntList.add(RANDOM.nextInt());
      largeLongList.add(RANDOM.nextLong());
      largeFloatList.add(RANDOM.nextFloat());
      largeDoubleList.add(RANDOM.nextDouble());
    }
    IntOpenHashSet largeIntSet = new IntOpenHashSet(largeIntList);
    ObjectArrayList<String> largeStringList = new ObjectArrayList<>();
    for (int i = 0; i < 1000; i++) {
      largeStringList.add(RandomStringUtils.secure().next(RANDOM.nextInt(40)));
    }
    Int2LongOpenHashMap int2LongMap = new Int2LongOpenHashMap();
    int2LongMap.put(7, 3L);
    Long2LongOpenHashMap long2LongMap = new Long2LongOpenHashMap();
    long2LongMap.put(7L, 3L);
    Float2LongOpenHashMap float2LongMap = new Float2LongOpenHashMap();
    float2LongMap.put(1.5f, 3L);
    Double2LongOpenHashMap double2LongMap = new Double2LongOpenHashMap();
    double2LongMap.put(1.5, 3L);
    Object2LongOpenHashMap<String> string2LongMap = new Object2LongOpenHashMap<>();
    string2LongMap.put("ab", 3L);
    // A surrogate pair, then an unpaired high surrogate that encodes as '?'
    String surrogates = "😀\uD83D";
    byte[] surrogatesPayload = HexFormat.of().parseHex("0000000100000005f09f98803f");
    Object2LongOpenHashMap<String> surrogates2LongMap = new Object2LongOpenHashMap<>();
    surrogates2LongMap.put(surrogates, 3L);
    UpdatableThetaSketch updatableSketch = updatableThetaSketch(1, 2, 3);
    CompactThetaSketch orderedSketch = updatableThetaSketch(1, 2, 3).compact(true, null);
    return new Object[][]{
        {new IntArrayList(), ObjectSerDeUtils.INT_ARRAY_LIST_SER_DE, HexFormat.of().parseHex("00000000")},
        {IntArrayList.wrap(new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE, 0, 99}, 3),
            ObjectSerDeUtils.INT_ARRAY_LIST_SER_DE, HexFormat.of().parseHex("00000003800000007fffffff00000000")},
        {new LongArrayList(), ObjectSerDeUtils.LONG_ARRAY_LIST_SER_DE, HexFormat.of().parseHex("00000000")},
        {LongArrayList.wrap(new long[]{Long.MIN_VALUE, Long.MAX_VALUE, 0, 99}, 3),
            ObjectSerDeUtils.LONG_ARRAY_LIST_SER_DE,
            HexFormat.of().parseHex("0000000380000000000000007fffffffffffffff0000000000000000")},
        {new FloatArrayList(), ObjectSerDeUtils.FLOAT_ARRAY_LIST_SER_DE, HexFormat.of().parseHex("00000000")},
        {FloatArrayList.wrap(new float[]{Float.intBitsToFloat(0x7fa12345), -0.0f, Float.POSITIVE_INFINITY,
            Float.NEGATIVE_INFINITY, 99}, 4), ObjectSerDeUtils.FLOAT_ARRAY_LIST_SER_DE,
            HexFormat.of().parseHex("000000047fa12345800000007f800000ff800000")},
        {new DoubleArrayList(), ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE, HexFormat.of().parseHex("00000000")},
        {largeIntList, ObjectSerDeUtils.INT_ARRAY_LIST_SER_DE,
            ObjectSerDeUtils.INT_ARRAY_LIST_SER_DE.serialize(largeIntList)},
        {largeLongList, ObjectSerDeUtils.LONG_ARRAY_LIST_SER_DE,
            ObjectSerDeUtils.LONG_ARRAY_LIST_SER_DE.serialize(largeLongList)},
        {largeFloatList, ObjectSerDeUtils.FLOAT_ARRAY_LIST_SER_DE,
            ObjectSerDeUtils.FLOAT_ARRAY_LIST_SER_DE.serialize(largeFloatList)},
        {largeDoubleList, ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE,
            ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.serialize(largeDoubleList)},
        {largeIntSet, ObjectSerDeUtils.INT_SET_SER_DE, ObjectSerDeUtils.INT_SET_SER_DE.serialize(largeIntSet)},
        {largeStringList, ObjectSerDeUtils.STRING_ARRAY_LIST_SER_DE,
            ObjectSerDeUtils.STRING_ARRAY_LIST_SER_DE.serialize(largeStringList)},
        {DoubleArrayList.wrap(new double[]{Double.longBitsToDouble(0x7ff0123456789abcL), -0.0d,
            Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 99}, 4), ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE,
            HexFormat.of().parseHex("000000047ff0123456789abc80000000000000007ff0000000000000fff0000000000000")},
        {new long[0], ObjectSerDeUtils.LONG_ARRAY_SER_DE, HexFormat.of().parseHex("00000000")},
        {new long[]{Long.MIN_VALUE, Long.MAX_VALUE, 0}, ObjectSerDeUtils.LONG_ARRAY_SER_DE,
            HexFormat.of().parseHex("0000000380000000000000007fffffffffffffff0000000000000000")},
        {largeLongList.toLongArray(), ObjectSerDeUtils.LONG_ARRAY_SER_DE,
            ObjectSerDeUtils.LONG_ARRAY_LIST_SER_DE.serialize(largeLongList)},
        {new double[0], ObjectSerDeUtils.DOUBLE_ARRAY_SER_DE, HexFormat.of().parseHex("00000000")},
        {new double[]{Double.longBitsToDouble(0x7ff0123456789abcL), -0.0d, Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY}, ObjectSerDeUtils.DOUBLE_ARRAY_SER_DE,
            HexFormat.of().parseHex("000000047ff0123456789abc80000000000000007ff0000000000000fff0000000000000")},
        {largeDoubleList.toDoubleArray(), ObjectSerDeUtils.DOUBLE_ARRAY_SER_DE,
            ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.serialize(largeDoubleList)},
        {new HyperLogLog(4), ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE,
            HexFormat.of().parseHex("000000040000000c" + "000000000000000000000000")},
        {hyperLogLog, ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE, hyperLogLog.getBytes()},
        {UltraLogLog.create(3), ObjectSerDeUtils.ULTRA_LOG_LOG_SER_DE,
            HexFormat.of().parseHex("030000000000000000")},
        {UltraLogLog.wrap(new byte[]{0, 1, 2, 3, 4, 5, 6, 7}), ObjectSerDeUtils.ULTRA_LOG_LOG_SER_DE,
            HexFormat.of().parseHex("030001020304050607")},
        {new IntOpenHashSet(), ObjectSerDeUtils.INT_SET_SER_DE, HexFormat.of().parseHex("00000000")},
        {new IntOpenHashSet(new int[]{7}), ObjectSerDeUtils.INT_SET_SER_DE,
            HexFormat.of().parseHex("0000000100000007")},
        {new LongOpenHashSet(new long[]{7}), ObjectSerDeUtils.LONG_SET_SER_DE,
            HexFormat.of().parseHex("000000010000000000000007")},
        {new FloatOpenHashSet(new float[]{1.5f}), ObjectSerDeUtils.FLOAT_SET_SER_DE,
            HexFormat.of().parseHex("000000013fc00000")},
        {new DoubleOpenHashSet(new double[]{1.5}), ObjectSerDeUtils.DOUBLE_SET_SER_DE,
            HexFormat.of().parseHex("000000013ff8000000000000")},
        {new ObjectOpenHashSet<>(new BigDecimal[]{new BigDecimal("1.5")}), ObjectSerDeUtils.BIG_DECIMAL_SET_SER_DE,
            singleBigDecimalBytes(new BigDecimal("1.5"))},
        {new ObjectOpenHashSet<>(new String[]{"ab"}), ObjectSerDeUtils.STRING_SET_SER_DE,
            HexFormat.of().parseHex("00000001000000026162")},
        {new ObjectOpenHashSet<>(new String[]{surrogates}), ObjectSerDeUtils.STRING_SET_SER_DE, surrogatesPayload},
        {new ObjectOpenHashSet<>(new ByteArray[]{new ByteArray(new byte[]{1, 2})}), ObjectSerDeUtils.BYTES_SET_SER_DE,
            HexFormat.of().parseHex("00000001000000020102")},
        {new ObjectLinkedOpenHashSet<>(new String[]{"b", "a"}), ObjectSerDeUtils.ORDERED_STRING_SET_SER_DE,
            HexFormat.of().parseHex("00000002000000016200000001" + "61")},
        {new ObjectLinkedOpenHashSet<>(new String[]{surrogates}), ObjectSerDeUtils.ORDERED_STRING_SET_SER_DE,
            surrogatesPayload},
        {new ObjectArrayList<>(new BigDecimal[]{new BigDecimal("1.5")}),
            ObjectSerDeUtils.BIG_DECIMAL_ARRAY_LIST_SER_DE, singleBigDecimalBytes(new BigDecimal("1.5"))},
        {new ObjectArrayList<String>(), ObjectSerDeUtils.STRING_ARRAY_LIST_SER_DE, HexFormat.of().parseHex("00000000")},
        {new ObjectArrayList<>(new String[]{"ab", "c"}), ObjectSerDeUtils.STRING_ARRAY_LIST_SER_DE,
            HexFormat.of().parseHex("0000000200000002616200000001" + "63")},
        {new ObjectArrayList<>(new String[]{surrogates}), ObjectSerDeUtils.STRING_ARRAY_LIST_SER_DE,
            surrogatesPayload},
        {new ObjectArrayList<>(new ByteArray[]{new ByteArray(new byte[]{1, 2})}),
            ObjectSerDeUtils.BYTES_ARRAY_LIST_SER_DE, HexFormat.of().parseHex("00000001000000020102")},
        {int2LongMap, ObjectSerDeUtils.INT_2_LONG_MAP_SER_DE,
            HexFormat.of().parseHex("00000001000000070000000000000003")},
        {long2LongMap, ObjectSerDeUtils.LONG_2_LONG_MAP_SER_DE,
            HexFormat.of().parseHex("0000000100000000000000070000000000000003")},
        {float2LongMap, ObjectSerDeUtils.FLOAT_2_LONG_MAP_SER_DE,
            HexFormat.of().parseHex("000000013fc000000000000000000003")},
        {double2LongMap, ObjectSerDeUtils.DOUBLE_2_LONG_MAP_SER_DE,
            HexFormat.of().parseHex("000000013ff80000000000000000000000000003")},
        {string2LongMap, ObjectSerDeUtils.STRING_2_LONG_MAP_SER_DE,
            HexFormat.of().parseHex("0000000100000002" + "61620000000000000003")},
        {surrogates2LongMap, ObjectSerDeUtils.STRING_2_LONG_MAP_SER_DE,
            HexFormat.of().parseHex("0000000100000005f09f98803f" + "0000000000000003")},
        {updatableThetaSketch(), ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE,
            updatableThetaSketch().compact(false, null).toByteArray()},
        {UpdatableThetaSketch.builder().setP(0.5f).build(), ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE,
            HexFormat.of().parseHex("01030300001e0000")},
        {updatableSketch, ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE,
            updatableSketch.compact(false, null).toByteArray()},
        {orderedSketch, ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE, orderedSketch.toByteArray()}
    };
  }

  private static UpdatableThetaSketch updatableThetaSketch(long... values) {
    UpdatableThetaSketch sketch = UpdatableThetaSketch.builder().build();
    for (long value : values) {
      sketch.update(value);
    }
    return sketch;
  }

  /// The serialized form of a set or list holding one BigDecimal: the count, then the value behind its length.
  private static byte[] singleBigDecimalBytes(BigDecimal value) {
    byte[] valueBytes = BigDecimalUtils.serialize(value);
    ByteBuffer buffer = ByteBuffer.allocate(2 * Integer.BYTES + valueBytes.length);
    buffer.putInt(1).putInt(valueBytes.length).put(valueBytes);
    return buffer.array();
  }

  @Test(dataProvider = "directSerializationValues")
  public void testSerializeIntoBuffer(Object value, SizedObjectSerDe<Object> serDe, byte[] expected)
      throws IOException {
    Object inputState = copyInputState(value);
    assertEquals(serDe.getSerializedSize(value), expected.length);
    assertEquals(serDe.serialize(value), expected);
    for (boolean direct : new boolean[]{false, true}) {
      for (int extraRemaining : new int[]{0, 5}) {
        ByteBuffer buffer = direct
            ? ByteBuffer.allocateDirect(expected.length + 10)
            : ByteBuffer.allocate(expected.length + 10);
        while (buffer.hasRemaining()) {
          buffer.put((byte) 0x5a);
        }
        buffer.position(5).limit(5 + expected.length + extraRemaining);
        serDe.serialize(value, buffer);
        assertEquals(buffer.position(), 5 + expected.length);
        assertEquals(buffer.limit(), 5 + expected.length + extraRemaining);
        assertSame(buffer.order(), ByteOrder.BIG_ENDIAN);
        buffer.clear();
        for (int i = 0; i < 5; i++) {
          assertEquals(buffer.get(), (byte) 0x5a);
        }
        byte[] actual = new byte[expected.length];
        buffer.get(actual);
        assertEquals(actual, expected);
        while (buffer.hasRemaining()) {
          assertEquals(buffer.get(), (byte) 0x5a);
        }
      }
    }
    try (PagedPinotOutputStream output = new PagedPinotOutputStream(new HeapPageAllocator(PAGE_SIZE))) {
      serDe.serialize(value, output);
      assertEquals(output.getCurrentOffset(), expected.length);
      assertEquals(toBytes(output), expected);
    }
    Object decoded = serDe.deserialize(expected);
    if (value instanceof Set<?> set && set.size() > 1 && !(value instanceof ObjectLinkedOpenHashSet)) {
      // A hash set of several elements iterates in an order that depends on how it was built, so a decoded set holds
      // the same elements but may write them in another order
      assertEquals(decoded, value);
    } else {
      assertEquals(serDe.serialize(decoded), expected);
    }
    assertEquals(copyInputState(value), inputState);
    assertEquals(serDe.getSerializedSize(value), expected.length);
    assertEquals(serDe.serialize(value), expected);
  }

  private static byte[] toBytes(PagedPinotOutputStream output) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    for (ByteBuffer page : output.getPages()) {
      byte[] pageBytes = new byte[page.remaining()];
      page.get(pageBytes);
      bytes.writeBytes(pageBytes);
    }
    return bytes.toByteArray();
  }

  private static Object copyInputState(Object value)
      throws IOException {
    if (value instanceof IntArrayList list) {
      return list.elements().clone();
    }
    if (value instanceof LongArrayList list) {
      return list.elements().clone();
    }
    if (value instanceof FloatArrayList list) {
      float[] elements = list.elements();
      int[] bits = new int[elements.length];
      for (int i = 0; i < elements.length; i++) {
        bits[i] = Float.floatToRawIntBits(elements[i]);
      }
      return bits;
    }
    if (value instanceof DoubleArrayList list) {
      double[] elements = list.elements();
      long[] bits = new long[elements.length];
      for (int i = 0; i < elements.length; i++) {
        bits[i] = Double.doubleToRawLongBits(elements[i]);
      }
      return bits;
    }
    if (value instanceof long[] values) {
      return values.clone();
    }
    if (value instanceof double[] values) {
      long[] bits = new long[values.length];
      for (int i = 0; i < values.length; i++) {
        bits[i] = Double.doubleToRawLongBits(values[i]);
      }
      return bits;
    }
    if (value instanceof HyperLogLog hyperLogLog) {
      return hyperLogLog.getBytes();
    }
    if (value instanceof UltraLogLog sketch) {
      return sketch.getState().clone();
    }
    if (value instanceof Set<?> set) {
      return new HashSet<>(set);
    }
    if (value instanceof List<?> list) {
      return new ArrayList<>(list);
    }
    if (value instanceof Map<?, ?> map) {
      return new HashMap<>(map);
    }
    return ((ThetaSketch) value).toByteArray();
  }

  @Test
  public void testDoubleArrayList() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      DoubleArrayList expected = new DoubleArrayList(size);
      for (int j = 0; j < size; j++) {
        expected.add(RANDOM.nextDouble());
      }

      byte[] bytes = ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.serialize(expected);
      DoubleArrayList actual = ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.deserialize(bytes);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testDoubleArray() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      double[] expected = RANDOM.doubles(RANDOM.nextInt(100)).toArray();
      byte[] bytes = ObjectSerDeUtils.DOUBLE_ARRAY_SER_DE.serialize(expected);
      assertEquals(bytes, ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.serialize(DoubleArrayList.wrap(expected)),
          ERROR_MESSAGE);
      assertEquals(ObjectSerDeUtils.DOUBLE_ARRAY_SER_DE.deserialize(bytes), expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testHyperLogLog() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      HyperLogLog expected = new HyperLogLog(7);

      byte[] bytes = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(expected);
      HyperLogLog actual = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytes);

      assertEquals(actual.cardinality(), expected.cardinality(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testHyperLogLogDeserializeThrowsForSizeMismatch()
      throws Exception {
    // We serialize a HLL w/ log2m of 12 and then trim 1024 bytes from the end of it and try to deserialize it. An
    // exception should occur because 2732 bytes are expected after the headers, but instead it will only find 1708.
    byte[] bytes = (new HyperLogLog(12)).getBytes();
    byte[] trimmed = Arrays.copyOfRange(bytes, 0, bytes.length - 1024);
    assertThrows(RuntimeException.class,
        () -> ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(trimmed));
  }

  @Test
  public void testIntSet() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      IntSet expected = new IntOpenHashSet(size);
      for (int j = 0; j < size; j++) {
        expected.add(RANDOM.nextInt());
      }

      byte[] bytes = ObjectSerDeUtils.INT_SET_SER_DE.serialize(expected);
      IntSet actual = ObjectSerDeUtils.INT_SET_SER_DE.deserialize(bytes);

      // NOTE: use Object comparison instead of Collection comparison because the order might be different
      assertEquals((Object) actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testTDigest() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      TDigest expected = TDigestAccumulator.forLegacyAggregation(100);
      int size = RANDOM.nextInt(100) + 1;
      for (int j = 0; j < size; j++) {
        expected.add(RANDOM.nextDouble());
      }

      byte[] bytes = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(expected);
      TDigest actual = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytes);

      for (int j = 0; j <= 100; j++) {
        assertEquals(actual.quantile(j / 100.0), expected.quantile(j / 100.0), 1e-5);
      }
    }

    // Try some custom compression values
    List<Double> compressionFactorsToTest = List.of(10d, 200d, 500d, 1000d, 10000d);
    for (double compressionFactor : compressionFactorsToTest) {
      for (int i = 0; i < NUM_ITERATIONS; i++) {
        TDigest expected = TDigestAccumulator.forLegacyAggregation(compressionFactor);
        int size = RANDOM.nextInt(100) + 1;
        for (int j = 0; j < size; j++) {
          expected.add(RANDOM.nextDouble());
        }

        byte[] bytes = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(expected);
        TDigest actual = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytes);

        for (int j = 0; j <= 100; j++) {
          assertEquals(actual.quantile(j / 100.0), expected.quantile(j / 100.0), 1e-5);
        }
      }
    }
  }

  @Test
  public void testTDigestIntermediateReductionStrategy() {
    TDigest source = TDigestAccumulator.forLegacyAggregation(100.0);
    Random random = new Random(42);
    for (int i = 0; i < 1000; i++) {
      source.add(random.nextDouble());
    }
    byte[] bytes = source.serialize();
    SerializedTDigestInput input = new SerializedTDigestInput();
    input.reset(bytes);
    TDigestAccumulator expected = TDigestAccumulator.forReduction(input.getCompression());
    expected.addSerializedTDigest(input);
    TDigest fromBytes = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytes);
    TDigest fromBuffer = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(ByteBuffer.wrap(bytes));
    // Raw values appended to a peer's intermediate must use reducer compression, rather than the stored-sketch policy.
    for (int i = 0; i < 5000; i++) {
      double value = random.nextDouble();
      expected.add(value);
      fromBytes.add(value);
      fromBuffer.add(value);
    }
    byte[] expectedBytes = expected.serialize();
    assertEquals(ObjectSerDeUtils.TDIGEST_SER_DE.serialize(fromBytes), expectedBytes);
    assertEquals(ObjectSerDeUtils.TDIGEST_SER_DE.serialize(fromBuffer), expectedBytes);
    assertEquals(fromBytes.getTotalWeight(), 6000.0);
  }

  @DataProvider(name = "tdigest32Fixtures")
  public static Object[][] tdigest32Fixtures() {
    return new Object[][]{
        {TDIGEST_3_2_VERBOSE_COMPRESSION_20, 1, 20.0, 256L, 0.1, 0.9},
        {TDIGEST_3_2_SMALL_COMPRESSION_1000, 2, 1_000.0, 64L, 0.011822292565892623, 1209.4004609431959}
    };
  }

  @Test(dataProvider = "tdigest32Fixtures")
  public void testTDigest32Fixtures(String base64Bytes, int expectedEncoding, double expectedCompression,
      long expectedSize, double expectedMin, double expectedMax) {
    byte[] bytes = Base64.getDecoder().decode(base64Bytes);
    assertEquals(ByteBuffer.wrap(bytes).getInt(), expectedEncoding);
    TDigest digest = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytes);
    assertEquals(digest.compression(), expectedCompression);
    assertEquals(digest.size(), expectedSize);
    assertEquals(digest.getMin(), expectedMin);
    assertEquals(digest.getMax(), expectedMax);

    long centroidWeight = 0L;
    double previousMean = Double.NEGATIVE_INFINITY;
    for (Centroid centroid : digest.centroids()) {
      assertTrue(Double.isFinite(centroid.mean()));
      assertTrue(centroid.weight() > 0);
      assertTrue(centroid.mean() + 1e-12 >= previousMean);
      centroidWeight += centroid.weight();
      previousMean = centroid.mean();
    }
    assertEquals(centroidWeight, expectedSize);

    double previousQuantile = Double.NEGATIVE_INFINITY;
    for (double quantile : new double[]{0.0, 0.5, 0.75, 0.95, 0.99, 1.0}) {
      double value = digest.quantile(quantile);
      assertTrue(Double.isFinite(value));
      assertTrue(value >= previousQuantile);
      previousQuantile = value;
    }
  }

  @Test
  public void testInt2LongMap() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      Int2LongOpenHashMap expected = new Int2LongOpenHashMap(size);
      for (int j = 0; j < size; j++) {
        expected.put(RANDOM.nextInt(20), RANDOM.nextLong());
      }

      byte[] bytes = ObjectSerDeUtils.INT_2_LONG_MAP_SER_DE.serialize(expected);
      Int2LongMap actual = ObjectSerDeUtils.INT_2_LONG_MAP_SER_DE.deserialize(bytes);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testLong2LongMap() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      Long2LongOpenHashMap expected = new Long2LongOpenHashMap(size);
      for (int j = 0; j < size; j++) {
        expected.put(RANDOM.nextLong(), RANDOM.nextLong());
      }

      byte[] bytes = ObjectSerDeUtils.LONG_2_LONG_MAP_SER_DE.serialize(expected);
      Long2LongMap actual = ObjectSerDeUtils.LONG_2_LONG_MAP_SER_DE.deserialize(bytes);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testFloat2LongMap() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      Float2LongOpenHashMap expected = new Float2LongOpenHashMap(size);
      for (int j = 0; j < size; j++) {
        expected.put(RANDOM.nextFloat(), RANDOM.nextLong());
      }

      byte[] bytes = ObjectSerDeUtils.FLOAT_2_LONG_MAP_SER_DE.serialize(expected);
      Float2LongMap actual = ObjectSerDeUtils.FLOAT_2_LONG_MAP_SER_DE.deserialize(bytes);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testDouble2LongMap() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      Double2LongOpenHashMap expected = new Double2LongOpenHashMap(size);
      for (int j = 0; j < size; j++) {
        expected.put(RANDOM.nextDouble(), RANDOM.nextLong());
      }

      byte[] bytes = ObjectSerDeUtils.DOUBLE_2_LONG_MAP_SER_DE.serialize(expected);
      Double2LongMap actual = ObjectSerDeUtils.DOUBLE_2_LONG_MAP_SER_DE.deserialize(bytes);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testString2LongMap() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      Object2LongOpenHashMap<String> expected = new Object2LongOpenHashMap<>(size);
      for (int j = 0; j < size; j++) {
        expected.put(RandomStringUtils.secure().next(RANDOM.nextInt(5)), RANDOM.nextLong());
      }

      byte[] bytes = ObjectSerDeUtils.STRING_2_LONG_MAP_SER_DE.serialize(expected);
      Object2LongMap<String> actual = ObjectSerDeUtils.STRING_2_LONG_MAP_SER_DE.deserialize(bytes);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testIntArrayList() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      IntArrayList expected = new IntArrayList(size);
      for (int j = 0; j < size; j++) {
        expected.add(RANDOM.nextInt());
      }
      byte[] bytes = ObjectSerDeUtils.INT_ARRAY_LIST_SER_DE.serialize(expected);
      IntArrayList actual = ObjectSerDeUtils.INT_ARRAY_LIST_SER_DE.deserialize(bytes);
      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testLongArrayList() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      LongArrayList expected = new LongArrayList(size);
      for (int j = 0; j < size; j++) {
        expected.add(RANDOM.nextLong());
      }
      byte[] bytes = ObjectSerDeUtils.LONG_ARRAY_LIST_SER_DE.serialize(expected);
      LongArrayList actual = ObjectSerDeUtils.LONG_ARRAY_LIST_SER_DE.deserialize(bytes);
      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testLongArray() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      long[] expected = RANDOM.longs(RANDOM.nextInt(100)).toArray();
      byte[] bytes = ObjectSerDeUtils.LONG_ARRAY_SER_DE.serialize(expected);
      assertEquals(bytes, ObjectSerDeUtils.LONG_ARRAY_LIST_SER_DE.serialize(LongArrayList.wrap(expected)),
          ERROR_MESSAGE);
      assertEquals(ObjectSerDeUtils.LONG_ARRAY_SER_DE.deserialize(bytes), expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testFloatArrayList() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      FloatArrayList expected = new FloatArrayList(size);
      for (int j = 0; j < size; j++) {
        expected.add(RANDOM.nextFloat());
      }
      byte[] bytes = ObjectSerDeUtils.FLOAT_ARRAY_LIST_SER_DE.serialize(expected);
      FloatArrayList actual = ObjectSerDeUtils.FLOAT_ARRAY_LIST_SER_DE.deserialize(bytes);
      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testStringArrayList() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      ObjectArrayList<String> expected = new ObjectArrayList<>(size);
      for (int j = 0; j < size; j++) {
        expected.add(RandomStringUtils.secure().next(RANDOM.nextInt(20)));
      }
      byte[] bytes = ObjectSerDeUtils.STRING_ARRAY_LIST_SER_DE.serialize(expected);
      ObjectArrayList<String> actual = ObjectSerDeUtils.STRING_ARRAY_LIST_SER_DE.deserialize(bytes);
      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testULL() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      UltraLogLog ull = UltraLogLog.create(12);
      int size = RANDOM.nextInt(100) + 1;
      for (int j = 0; j < size; j++) {
        UltraLogLogUtils.hashObject(RANDOM.nextLong()).ifPresent(ull::add);
      }

      byte[] bytes = ObjectSerDeUtils.ULTRA_LOG_LOG_SER_DE.serialize(ull);
      UltraLogLog actual = ObjectSerDeUtils.ULTRA_LOG_LOG_SER_DE.deserialize(bytes);

      assertEquals(actual.getDistinctCountEstimate(), ull.getDistinctCountEstimate(), ERROR_MESSAGE);
      assertEquals(actual.getState(), ull.getState(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testThetaSketch() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      UpdatableThetaSketch input = UpdatableThetaSketch.builder().build();
      int size = RANDOM.nextInt(100) + 10;
      boolean shouldOrder = RANDOM.nextBoolean();

      for (int j = 0; j < size; j++) {
        input.update(j);
      }

      ThetaSketch sketch = input.compact(shouldOrder, null);

      byte[] bytes = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(sketch);
      ThetaSketch actual = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(bytes);

      assertEquals(actual.getEstimate(), sketch.getEstimate(), ERROR_MESSAGE);
      assertEquals(actual.toByteArray(), sketch.toByteArray(), ERROR_MESSAGE);
      assertEquals(actual.isOrdered(), shouldOrder, ERROR_MESSAGE);
    }
  }

  @Test
  public void testOrderedStringSet() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      ObjectLinkedOpenHashSet<String> expected = new ObjectLinkedOpenHashSet<>(size);
      for (int j = 0; j < size; j++) {
        expected.add(RandomStringUtils.secure().next(RANDOM.nextInt(20)));
      }
      byte[] bytes = ObjectSerDeUtils.ORDERED_STRING_SET_SER_DE.serialize(expected);
      ObjectLinkedOpenHashSet<String> actual = ObjectSerDeUtils.ORDERED_STRING_SET_SER_DE.deserialize(bytes);
      assertEquals(new ArrayList<>(actual), new ArrayList<>(expected), ERROR_MESSAGE);
    }
  }
}
