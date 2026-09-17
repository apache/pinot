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
package org.apache.pinot.segment.local.aggregator;

import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class ArrayAggDistinctValueAggregatorTest {

  @Test
  public void aggregatedValueTypeIsBytes() {
    assertEquals(new ArrayAggDistinctValueAggregator().getAggregatedValueType(), DataType.BYTES);
    assertEquals(ArrayAggDistinctValueAggregator.AGGREGATED_VALUE_TYPE, DataType.BYTES);
    assertEquals(new ArrayAggDistinctValueAggregator().isAggregatedValueFixedSize(), false);
  }

  @Test
  public void initialAndApplyRawValueDedup() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> set = agg.getInitialAggregatedValue(1L);
    set = agg.applyRawValue(set, 2L);
    set = agg.applyRawValue(set, 1L); // duplicate
    assertEquals(set.size(), 2);
    assertTrue(set.contains(1L));
    assertTrue(set.contains(2L));
  }

  @Test
  public void applyAggregatedValueUnions() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> a = agg.getInitialAggregatedValue(1);
    a = agg.applyRawValue(a, 2);
    ObjectSet<Object> b = agg.getInitialAggregatedValue(2);
    b = agg.applyRawValue(b, 3);
    ObjectSet<Object> merged = agg.applyAggregatedValue(a, b);
    assertEquals(merged.size(), 3);
    assertTrue(merged.contains(1));
    assertTrue(merged.contains(2));
    assertTrue(merged.contains(3));
  }

  @Test
  public void nullRawValueIgnored() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> set = agg.getInitialAggregatedValue(null);
    assertEquals(set.size(), 0);
    set = agg.applyRawValue(set, null);
    assertEquals(set.size(), 0);
    set = agg.applyRawValue(set, 5L);
    assertEquals(set.size(), 1);
  }

  @Test
  public void serializeDeserializeRoundTripInt() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> set = agg.getInitialAggregatedValue(10);
    set = agg.applyRawValue(set, 20);
    set = agg.applyRawValue(set, 30);
    byte[] bytes = agg.serializeAggregatedValue(set);
    assertEquals(agg.deserializeAggregatedValue(bytes), set);
    assertEquals(agg.getMaxAggregatedValueByteSize(), bytes.length);
  }

  @Test
  public void serializeDeserializeRoundTripLong() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> set = agg.getInitialAggregatedValue(10L);
    set = agg.applyRawValue(set, 20L);
    byte[] bytes = agg.serializeAggregatedValue(set);
    assertEquals(agg.deserializeAggregatedValue(bytes), set);
  }

  @Test
  public void serializeDeserializeRoundTripFloat() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> set = agg.getInitialAggregatedValue(1.5f);
    set = agg.applyRawValue(set, 2.5f);
    byte[] bytes = agg.serializeAggregatedValue(set);
    assertEquals(agg.deserializeAggregatedValue(bytes), set);
  }

  @Test
  public void serializeDeserializeRoundTripDouble() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> set = agg.getInitialAggregatedValue(1.5d);
    set = agg.applyRawValue(set, 2.5d);
    byte[] bytes = agg.serializeAggregatedValue(set);
    assertEquals(agg.deserializeAggregatedValue(bytes), set);
  }

  @Test
  public void serializeDeserializeRoundTripString() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> set = agg.getInitialAggregatedValue("hello");
    set = agg.applyRawValue(set, "world");
    byte[] bytes = agg.serializeAggregatedValue(set);
    assertEquals(agg.deserializeAggregatedValue(bytes), set);
  }

  @Test
  public void serializeDeserializeRoundTripBigDecimal() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> set = agg.getInitialAggregatedValue(new BigDecimal("1.23"));
    set = agg.applyRawValue(set, new BigDecimal("4.56"));
    byte[] bytes = agg.serializeAggregatedValue(set);
    assertEquals(agg.deserializeAggregatedValue(bytes), set);
  }

  @Test
  public void emptySetSerializesToTagPlusSizeZero() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> set = agg.getInitialAggregatedValue(null);
    byte[] bytes = agg.serializeAggregatedValue(set);
    assertEquals(bytes.length, Byte.BYTES + Integer.BYTES);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.get(); // element type tag
    assertEquals(buffer.getInt(), 0);
    assertEquals(agg.deserializeAggregatedValue(bytes).size(), 0);
  }

  /// After the 1-byte element type tag, the serialized payload must match `ObjectSerDeUtils.LONG_SET_SER_DE` in
  /// pinot-core (int size, then each long) so the query-time arrayAgg function can deserialize the star-tree cell.
  /// This asserts the layout directly, since pinot-segment-local cannot depend on pinot-core.
  @Test
  public void serializedLongLayoutMatchesSetSerDe() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> set = agg.getInitialAggregatedValue(7L);
    byte[] bytes = agg.serializeAggregatedValue(set);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.get(); // element type tag
    assertEquals(buffer.getInt(), 1);
    assertEquals(buffer.getLong(), 7L);
    assertEquals(bytes.length, Byte.BYTES + Integer.BYTES + Long.BYTES);
  }

  /// After the tag, the serialized string payload must match `ObjectSerDeUtils.STRING_SET_SER_DE`: int size, then per
  /// value (int length, UTF-8 bytes).
  @Test
  public void serializedStringLayoutMatchesSetSerDe() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> set = agg.getInitialAggregatedValue("ab");
    byte[] bytes = agg.serializeAggregatedValue(set);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.get(); // element type tag
    assertEquals(buffer.getInt(), 1);
    int length = buffer.getInt();
    assertEquals(length, 2);
    byte[] valueBytes = new byte[length];
    buffer.get(valueBytes);
    assertEquals(new String(valueBytes, StandardCharsets.UTF_8), "ab");
  }

  @Test
  public void bytesElementsNormalizedToByteArray() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> set = agg.getInitialAggregatedValue(new byte[]{1, 2});
    set = agg.applyRawValue(set, new byte[]{1, 2}); // duplicate content
    set = agg.applyRawValue(set, new byte[]{3, 4});
    assertEquals(set.size(), 2);
    assertTrue(set.contains(new ByteArray(new byte[]{1, 2})));
    byte[] bytes = agg.serializeAggregatedValue(set);
    assertEquals(agg.deserializeAggregatedValue(bytes), set);
  }

  @Test
  public void unsupportedTypeThrows() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    assertThrows(IllegalStateException.class, () -> agg.getInitialAggregatedValue(new Object()));
  }

  @Test
  public void cloneIsIndependentCopy() {
    ArrayAggDistinctValueAggregator agg = new ArrayAggDistinctValueAggregator();
    ObjectSet<Object> set = agg.getInitialAggregatedValue(1L);
    ObjectSet<Object> clone = agg.cloneAggregatedValue(set);
    assertEquals(clone, set);
    agg.applyRawValue(set, 2L);
    assertEquals(clone.size(), 1);
  }
}
