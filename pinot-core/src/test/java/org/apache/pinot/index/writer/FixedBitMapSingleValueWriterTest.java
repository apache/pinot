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
package org.apache.pinot.index.writer;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import org.apache.pinot.core.io.reader.impl.v1.FixedBitMapSingleValueReader;
import org.apache.pinot.core.io.util.PinotDataBitSet;
import org.apache.pinot.core.io.writer.impl.v1.FixedBitMapSingleValueWriter;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FixedBitMapSingleValueWriterTest {

  @Test
  public void testSimple() throws Exception {

    List<Map<String, String>> data = new ArrayList<>();

    int numUniqueKeys = 10;
    int numUniqueValues = 100;
    List<String> keyList = new ArrayList<>();
    for (int i = 0; i < numUniqueKeys; i++) {
      keyList.add("key-" + i);
    }
    int numDocs = 1000;
    Random random = new Random();
    for (int i = 0; i < numDocs; i++) {
      Collections.shuffle(keyList);
      int numKeys = random.nextInt(numUniqueKeys);
      Map<String, String> rowMap = new HashMap<>();

      for (int j = 0; j < numKeys; j++) {
        String value = keyList.get(j) + "-" + "value-" + random.nextInt(numUniqueValues);
        rowMap.put(keyList.get(j), value);
      }
      data.add(rowMap);
    }

    File file = new File("/tmp/map-writer.fwd.sv");
    int numTotalEntries = 0;
    Set<String> keys = new HashSet<>();
    Set<String> values = new HashSet<>();

    for (Map<String, String> rowMap : data) {
      numTotalEntries += rowMap.size();
      keys.addAll(rowMap.keySet());
      values.addAll(rowMap.values());
    }

    int keySizeInBits = PinotDataBitSet.getNumBitsPerValue(keys.size() - 1);
    int valueSizeInBits = PinotDataBitSet.getNumBitsPerValue(values.size() - 1);

    ArrayList<String> sortedKeyList = new ArrayList<>(keys);
    Collections.sort(sortedKeyList);
    ArrayList<String> sortedValueList = new ArrayList<>(values);
    Collections.sort(sortedValueList);

    Map<String, Integer> keyDictionary = new HashMap<>();
    for (int i = 0; i < sortedKeyList.size(); i++) {
      String key = sortedKeyList.get(i);
      keyDictionary.put(key, i);
    }

    Map<String, Integer> valueDictionary = new HashMap<>();
    for (int i = 0; i < sortedValueList.size(); i++) {
      String value = sortedValueList.get(i);
      valueDictionary.put(value, i);
    }
    int numKeys = keys.size();
    FixedBitMapSingleValueWriter writer = new FixedBitMapSingleValueWriter(file, numDocs,
        numTotalEntries, numKeys, keySizeInBits, valueSizeInBits);
    for (int i = 0; i < data.size(); i++) {
      Map<String, String> rowMap = data.get(i);
      Map<Integer, Integer> rowIntMap = new HashMap<>();
      for (String key : rowMap.keySet()) {
        rowIntMap.put(keyDictionary.get(key), valueDictionary.get(rowMap.get(key)));
      }
      writer.setIntIntMap(i, rowIntMap);
    }
    System.out.println("file.length() = " + file.length());
    FixedBitMapSingleValueReader reader = new FixedBitMapSingleValueReader(file, numDocs,
        numTotalEntries, numKeys, keySizeInBits, valueSizeInBits);
    for (int i = 0; i < data.size(); i++) {
      Map<String, String> rowMap = data.get(i);
      for (Entry<String, String> entry : rowMap.entrySet()) {
        Integer keyDictId = keyDictionary.get(entry.getKey());
        int valueDictId = reader.getIntValue(i, keyDictId);
        Assert.assertEquals((Integer) valueDictId, valueDictionary.get(entry.getValue()));
      }
    }
  }
}
