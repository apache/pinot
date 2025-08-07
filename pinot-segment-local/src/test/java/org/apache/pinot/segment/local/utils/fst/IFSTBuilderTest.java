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
package org.apache.pinot.segment.local.utils.fst;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.FST;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.index.readers.LuceneIFSTIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class IFSTBuilderTest implements PinotBuffersAfterMethodCheckRule {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "IFSTBuilderTest");

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testIFSTBuilder()
      throws IOException {
    SortedMap<String, Integer> x = new TreeMap<>();
    x.put("Hello-World", 12);
    x.put("HELLO-WORLD", 13);  // Same key in different case
    x.put("hello-world123", 21);
    x.put("HELLO-WORLD123", 22);  // Same key in different case
    x.put("Still", 123);
    x.put("STILL", 124);  // Same key in different case

    FST<BytesRef> ifst = IFSTBuilder.buildIFST(x);
    File outputFile = new File(TEMP_DIR, "test_case_insensitive.lucene");
    try (FileOutputStream outputStream = new FileOutputStream(outputFile);
        OutputStreamDataOutput dataOutput = new OutputStreamDataOutput(outputStream)) {
      ifst.save(dataOutput, dataOutput);
    }

    try (PinotDataBuffer dataBuffer = PinotDataBuffer.loadBigEndianFile(outputFile);
        LuceneIFSTIndexReader reader = new LuceneIFSTIndexReader(dataBuffer)) {
      // Test that case-insensitive matching works
      ImmutableRoaringBitmap result = reader.getDictIds("hello.*world");
      assertEquals(result.getCardinality(), 2); // Should match both "Hello-World" and "HELLO-WORLD"
      assertTrue(result.contains(12));
      assertTrue(result.contains(13));

      result = reader.getDictIds("hello.*world123");
      assertEquals(result.getCardinality(), 2); // Should match both "hello-world123" and "HELLO-WORLD123"
      assertTrue(result.contains(21));
      assertTrue(result.contains(22));

      result = reader.getDictIds("still");
      assertEquals(result.getCardinality(), 2); // Should match both "Still" and "STILL"
      assertTrue(result.contains(123));
      assertTrue(result.contains(124));

      // Test with uppercase regex
      result = reader.getDictIds("HELLO.*WORLD");
      assertEquals(result.getCardinality(), 2); // Should still match both
      assertTrue(result.contains(12));
      assertTrue(result.contains(13));

      // Test with mixed case regex
      result = reader.getDictIds("HeLLo.*WoRlD");
      assertEquals(result.getCardinality(), 2); // Should still match both
      assertTrue(result.contains(12));
      assertTrue(result.contains(13));
    }
  }

  @Test
  public void testSerializeDeserializeIntegerList()
      throws Exception {
    // Test empty list
    List<Integer> emptyList = new ArrayList<>();
    BytesRef serializedEmpty = IFSTBuilder.serializeIntegerList(emptyList);
    List<Integer> deserializedEmpty = IFSTBuilder.deserializeBytesRefToIntegerList(serializedEmpty);
    assertEquals(deserializedEmpty.size(), 0);
    assertTrue(deserializedEmpty.isEmpty());

    // Test single value
    List<Integer> singleList = new ArrayList<>();
    singleList.add(42);
    BytesRef serializedSingle = IFSTBuilder.serializeIntegerList(singleList);
    List<Integer> deserializedSingle = IFSTBuilder.deserializeBytesRefToIntegerList(serializedSingle);
    assertEquals(deserializedSingle.size(), 1);
    assertEquals(deserializedSingle.get(0).intValue(), 42);

    // Test multiple values
    List<Integer> multiList = new ArrayList<>();
    multiList.add(1);
    multiList.add(2);
    multiList.add(3);
    multiList.add(4);
    multiList.add(5);
    BytesRef serializedMulti = IFSTBuilder.serializeIntegerList(multiList);
    List<Integer> deserializedMulti = IFSTBuilder.deserializeBytesRefToIntegerList(serializedMulti);
    assertEquals(deserializedMulti.size(), 5);
    for (int i = 0; i < 5; i++) {
      assertEquals(deserializedMulti.get(i).intValue(), i + 1);
    }

    // Test large values
    List<Integer> largeList = new ArrayList<>();
    largeList.add(Integer.MAX_VALUE);
    largeList.add(Integer.MIN_VALUE);
    largeList.add(0);
    BytesRef serializedLarge = IFSTBuilder.serializeIntegerList(largeList);
    List<Integer> deserializedLarge = IFSTBuilder.deserializeBytesRefToIntegerList(serializedLarge);
    assertEquals(deserializedLarge.size(), 3);
    assertEquals(deserializedLarge.get(0).intValue(), Integer.MAX_VALUE);
    assertEquals(deserializedLarge.get(1).intValue(), Integer.MIN_VALUE);
    assertEquals(deserializedLarge.get(2).intValue(), 0);

    // Test null input (should throw exception)
    try {
      IFSTBuilder.serializeIntegerList(null);
      Assert.fail("Expected IllegalArgumentException for null input");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    // Test null BytesRef (should return empty list)
    List<Integer> nullResult = IFSTBuilder.deserializeBytesRefToIntegerList(null);
    assertEquals(nullResult.size(), 0);
    assertTrue(nullResult.isEmpty());

    // Test empty BytesRef (should return empty list)
    List<Integer> emptyResult = IFSTBuilder.deserializeBytesRefToIntegerList(new BytesRef());
    assertEquals(emptyResult.size(), 0);
    assertTrue(emptyResult.isEmpty());

    // Test BytesRef with insufficient bytes (should throw exception)
    byte[] insufficientBytes = new byte[3]; // Less than 4 bytes for count
    BytesRef insufficientBytesRef = new BytesRef(insufficientBytes);
    try {
      IFSTBuilder.deserializeBytesRefToIntegerList(insufficientBytesRef);
      Assert.fail("Expected RuntimeException for insufficient bytes");
    } catch (RuntimeException e) {
      // Expected
      assertTrue(e.getMessage().contains("not enough bytes for list size"));
    }
  }

  @Test
  public void testSerializeDeserializeEdgeCases()
      throws Exception {
    // Test with negative values
    List<Integer> negativeList = new ArrayList<>();
    negativeList.add(-1);
    negativeList.add(-100);
    negativeList.add(-1000);
    BytesRef serializedNegative = IFSTBuilder.serializeIntegerList(negativeList);
    List<Integer> deserializedNegative = IFSTBuilder.deserializeBytesRefToIntegerList(serializedNegative);
    assertEquals(deserializedNegative.size(), 3);
    assertEquals(deserializedNegative.get(0).intValue(), -1);
    assertEquals(deserializedNegative.get(1).intValue(), -100);
    assertEquals(deserializedNegative.get(2).intValue(), -1000);

    // Test with mixed positive and negative values
    List<Integer> mixedList = new ArrayList<>();
    mixedList.add(-5);
    mixedList.add(0);
    mixedList.add(5);
    mixedList.add(-10);
    mixedList.add(10);
    BytesRef serializedMixed = IFSTBuilder.serializeIntegerList(mixedList);
    List<Integer> deserializedMixed = IFSTBuilder.deserializeBytesRefToIntegerList(serializedMixed);
    assertEquals(deserializedMixed.size(), 5);
    assertEquals(deserializedMixed.get(0).intValue(), -5);
    assertEquals(deserializedMixed.get(1).intValue(), 0);
    assertEquals(deserializedMixed.get(2).intValue(), 5);
    assertEquals(deserializedMixed.get(3).intValue(), -10);
    assertEquals(deserializedMixed.get(4).intValue(), 10);
  }
}
