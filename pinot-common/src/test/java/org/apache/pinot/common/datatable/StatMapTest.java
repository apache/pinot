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
package org.apache.pinot.common.datatable;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class StatMapTest {

  @Test(dataProvider = "allTypeStats", expectedExceptions = IllegalArgumentException.class)
  public void dynamicTypeCheckWhenAddInt(MyStats stat) {
    if (stat.getType() == StatMap.Type.INT) {
      throw new SkipException("Skipping INT test");
    }
    if (stat.getType() == StatMap.Type.LONG) {
      throw new SkipException("Skipping LONG test");
    }
    StatMap<MyStats> statMap = new StatMap<>(MyStats.class);
    statMap.merge(stat, 1);
  }

  @Test(dataProvider = "allTypeStats", expectedExceptions = IllegalArgumentException.class)
  public void dynamicTypeCheckWhenAddLong(MyStats stat) {
    if (stat.getType() == StatMap.Type.LONG) {
      throw new SkipException("Skipping LONG test");
    }
    StatMap<MyStats> statMap = new StatMap<>(MyStats.class);
    statMap.merge(stat, 1L);
  }

  @Test(dataProvider = "allTypeStats", expectedExceptions = IllegalArgumentException.class)
  public void dynamicTypeCheckPutBoolean(MyStats stat) {
    if (stat.getType() == StatMap.Type.BOOLEAN) {
      throw new SkipException("Skipping BOOLEAN test");
    }
    StatMap<MyStats> statMap = new StatMap<>(MyStats.class);
    statMap.merge(stat, true);
  }

  @Test(dataProvider = "allTypeStats", expectedExceptions = IllegalArgumentException.class)
  public void dynamicTypeCheckPutString(MyStats stat) {
    if (stat.getType() == StatMap.Type.STRING) {
      throw new SkipException("Skipping STRING test");
    }
    StatMap<MyStats> statMap = new StatMap<>(MyStats.class);
    statMap.merge(stat, "foo");
  }

  @Test(dataProvider = "allTypeStats")
  public void singleEncodeDecode(MyStats stat)
      throws IOException {
    StatMap<MyStats> statMap = new StatMap<>(MyStats.class);
    switch (stat.getType()) {
      case BOOLEAN:
        statMap.merge(stat, true);
        break;
      case INT:
        statMap.merge(stat, 1);
        break;
      case LONG:
        statMap.merge(stat, 1L);
        break;
      case STRING:
        statMap.merge(stat, "foo");
        break;
      default:
        throw new IllegalStateException();
    }
    testSerializeDeserialize(statMap);
  }

  @Test
  public void encodeDecodeAll()
      throws IOException {
    StatMap<MyStats> statMap = new StatMap<>(MyStats.class);
    for (MyStats stat : MyStats.values()) {
      switch (stat.getType()) {
        case BOOLEAN:
          statMap.merge(stat, true);
          break;
        case INT:
          statMap.merge(stat, 1);
          break;
        case LONG:
          statMap.merge(stat, 1L);
          break;
        case STRING:
          statMap.merge(stat, "foo");
          break;
        default:
          throw new IllegalStateException();
      }
    }
    testSerializeDeserialize(statMap);
  }

  private <K extends Enum<K> & StatMap.Key> void testSerializeDeserialize(StatMap<K> statMap)
      throws IOException {
    ByteArrayDataOutput output = ByteStreams.newDataOutput();
    statMap.serialize(output);

    ByteArrayDataInput input = ByteStreams.newDataInput(output.toByteArray());
    StatMap<MyStats> deserializedStatMap = StatMap.deserialize(input, MyStats.class);

    Assert.assertEquals(statMap, deserializedStatMap);
  }

  @Test(dataProvider = "complexStats")
  public void mergeEquivalence(StatMap<?> statMap)
      throws IOException {
    StatMap mergedOnHeap = new StatMap<>(statMap.getKeyClass());
    mergedOnHeap.merge(statMap);

    ByteArrayDataOutput output = ByteStreams.newDataOutput();
    statMap.serialize(output);

    ByteArrayDataInput input = ByteStreams.newDataInput(output.toByteArray());
    StatMap mergedSerialized = new StatMap<>(statMap.getKeyClass());
    mergedSerialized.merge(input);

    Assert.assertEquals(mergedOnHeap, mergedSerialized,
        "Merging objects should be equal to merging serialized buffers");
  }

  @DataProvider(name = "complexStats")
  static StatMap<?>[] complexStats() {
    return new StatMap<?>[] {
      new StatMap<>(MyStats.class)
        .merge(MyStats.BOOL_KEY, true)
        .merge(MyStats.LONG_KEY, 1L)
        .merge(MyStats.INT_KEY, 1)
        .merge(MyStats.STR_KEY, "foo"),
      new StatMap<>(MyStats.class)
        .merge(MyStats.BOOL_KEY, false)
        .merge(MyStats.LONG_KEY, 1L)
        .merge(MyStats.INT_KEY, 1)
        .merge(MyStats.STR_KEY, "foo"),
      new StatMap<>(MyStats.class)
        .merge(MyStats.BOOL_KEY, true)
        .merge(MyStats.LONG_KEY, 0L)
        .merge(MyStats.INT_KEY, 1)
        .merge(MyStats.STR_KEY, "foo"),
      new StatMap<>(MyStats.class)
        .merge(MyStats.BOOL_KEY, false)
        .merge(MyStats.LONG_KEY, 1L)
        .merge(MyStats.INT_KEY, 0)
        .merge(MyStats.STR_KEY, "foo"),
      new StatMap<>(MyStats.class)
        .merge(MyStats.BOOL_KEY, false)
        .merge(MyStats.LONG_KEY, 1L)
        .merge(MyStats.INT_KEY, 1),
      new StatMap<>(BrokerResponseNativeV2.StatKey.class)
        .merge(BrokerResponseNativeV2.StatKey.NUM_SEGMENTS_QUERIED, 1)
        .merge(BrokerResponseNativeV2.StatKey.NUM_SEGMENTS_PROCESSED, 1)
        .merge(BrokerResponseNativeV2.StatKey.NUM_SEGMENTS_MATCHED, 1)
        .merge(BrokerResponseNativeV2.StatKey.NUM_DOCS_SCANNED, 10)
        .merge(BrokerResponseNativeV2.StatKey.NUM_ENTRIES_SCANNED_POST_FILTER, 5)
        .merge(BrokerResponseNativeV2.StatKey.TOTAL_DOCS, 5)
    };
  }

  @DataProvider(name = "allTypeStats")
  static MyStats[] allTypeStats() {
    return MyStats.values();
  }

  public enum MyStats implements StatMap.Key {
    BOOL_KEY(StatMap.Type.BOOLEAN),
    LONG_KEY(StatMap.Type.LONG),
    INT_KEY(StatMap.Type.INT),
    STR_KEY(StatMap.Type.STRING);

    private final StatMap.Type _type;

    MyStats(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
