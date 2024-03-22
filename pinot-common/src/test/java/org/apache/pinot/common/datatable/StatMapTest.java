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
import org.locationtech.jts.util.Assert;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class StatMapTest {

  @Test(dataProvider = "allTypeStats", expectedExceptions = IllegalArgumentException.class)
  public void dynamicTypeCheckWhenAddInt(MyStats stat) {
    if (stat.getType() == StatMap.Type.INT) {
      throw new SkipException("Skipping INT test");
    }
    StatMap<MyStats> statMap = new StatMap<>(MyStats.class);
    statMap.add(stat, 1);
  }

  @Test(dataProvider = "allTypeStats", expectedExceptions = IllegalArgumentException.class)
  public void dynamicTypeCheckWhenAddLong(MyStats stat) {
    if (stat.getType() == StatMap.Type.LONG) {
      throw new SkipException("Skipping LONG test");
    }
    StatMap<MyStats> statMap = new StatMap<>(MyStats.class);
    statMap.add(stat, 1L);
  }

  @Test(dataProvider = "allTypeStats", expectedExceptions = IllegalArgumentException.class)
  public void dynamicTypeCheckWhenAddDouble(MyStats stat) {
    if (stat.getType() == StatMap.Type.DOUBLE) {
      throw new SkipException("Skipping DOUBLE test");
    }
    StatMap<MyStats> statMap = new StatMap<>(MyStats.class);
    statMap.add(stat, 1d);
  }

  @Test(dataProvider = "allTypeStats", expectedExceptions = IllegalArgumentException.class)
  public void dynamicTypeCheckPutBoolean(MyStats stat) {
    if (stat.getType() == StatMap.Type.BOOLEAN) {
      throw new SkipException("Skipping BOOLEAN test");
    }
    StatMap<MyStats> statMap = new StatMap<>(MyStats.class);
    statMap.put(stat, true);
  }

  @Test(dataProvider = "allTypeStats", expectedExceptions = IllegalArgumentException.class)
  public void dynamicTypeCheckPutString(MyStats stat) {
    if (stat.getType() == StatMap.Type.STRING) {
      throw new SkipException("Skipping STRING test");
    }
    StatMap<MyStats> statMap = new StatMap<>(MyStats.class);
    statMap.put(stat, "foo");
  }

  @Test(dataProvider = "allTypeStats")
  public void singleEncodeDecode(MyStats stat)
      throws IOException {
    StatMap<MyStats> statMap = new StatMap<>(MyStats.class);
    switch (stat.getType()) {
      case BOOLEAN:
        statMap.put(stat, true);
        break;
      case INT:
        statMap.add(stat, 1);
        break;
      case LONG:
        statMap.add(stat, 1L);
        break;
      case DOUBLE:
        statMap.add(stat, 1d);
        break;
      case STRING:
        statMap.put(stat, "foo");
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
          statMap.put(stat, true);
          break;
        case INT:
          statMap.add(stat, 1);
          break;
        case LONG:
          statMap.add(stat, 1L);
          break;
        case DOUBLE:
          statMap.add(stat, 1d);
          break;
        case STRING:
          statMap.put(stat, "foo");
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

    Assert.equals(statMap, deserializedStatMap);
  }

  @DataProvider(name = "allTypeStats")
  static MyStats[] allTypeStats() {
    return MyStats.values();
  }

  public enum MyStats implements StatMap.Key {
    BOOL_KEY(StatMap.Type.BOOLEAN),
    LONG_KEY(StatMap.Type.LONG),
    DOUBLE_KEY(StatMap.Type.DOUBLE),
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
