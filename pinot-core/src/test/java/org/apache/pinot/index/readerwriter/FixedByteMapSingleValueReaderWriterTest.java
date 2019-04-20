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
package org.apache.pinot.index.readerwriter;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.io.readerwriter.impl.FixedByteMapSingleValueReaderWriter;
import org.apache.pinot.core.io.writer.impl.DirectMemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class FixedByteMapSingleValueReaderWriterTest {

  protected static Logger LOGGER = LoggerFactory
      .getLogger(FixedByteMapSingleValueReaderWriterTest.class);

  private PinotDataBufferMemoryManager _memoryManager;
  final List<Map<Integer, Integer>> data = new ArrayList<>();
  FixedByteMapSingleValueReaderWriter _readerWriter;
  final Random r = new Random();
  int _rows = 10000;
  int _numUniqueKeys = 10;
  int _numUniqueValues = 1000;
  AtomicInteger _numLookUps = new AtomicInteger();
  int _keyArray[] = new int[_numUniqueKeys];

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(FixedByteMapSingleValueReaderWriter.class.getName());
    _readerWriter = new FixedByteMapSingleValueReaderWriter(100, 5, Integer.BYTES, Integer.BYTES,
        _memoryManager, "test-int-int-map");
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  @Test
  public void testIntMap() {

    for (int i = 0; i < _rows; i++) {
      int numEntries = r.nextInt(10) + 1;
      Map<Integer, Integer> map = new HashMap<>();
      while (numEntries > 0) {
        map.put(r.nextInt(_numUniqueKeys), r.nextInt(_numUniqueValues));
        numEntries--;
      }
      data.add(map);
      _readerWriter.setIntIntMap(i, map);
      testRow(i);
    }
  }

  private synchronized void testRow(int rowId) {
    _numLookUps.incrementAndGet();
    Map<Integer, Integer> map = data.get(rowId);
    int length = _readerWriter.getIntKeySet(rowId, _keyArray);
    Assert.assertEquals(length, map.size());
    for (int i = 0; i < length; i++) {
      Assert.assertTrue(map.containsKey(_keyArray[i]));
    }

    for (Integer key : map.keySet()) {
      int expected = map.get(key).intValue();
      int actual = _readerWriter.getIntValue(rowId, key);
      Assert.assertEquals(actual, expected);
    }

  }

}
