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
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class FSTBuilderTest implements PinotBuffersAfterMethodCheckRule {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "FST");

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    TEMP_DIR.mkdirs();
  }

  @Test
  public void testRegexMatch() {
    RegexpMatcher regexpMatcher = new RegexpMatcher("hello.*ld", null);
    Assert.assertTrue(regexpMatcher.match("helloworld"));
    Assert.assertTrue(regexpMatcher.match("helloworld"));
    Assert.assertTrue(regexpMatcher.match("helloasdfworld"));
    Assert.assertFalse(regexpMatcher.match("ahelloasdfworld"));
  }

  @Test
  public void testFSTBuilder()
      throws IOException {
    SortedMap<String, Integer> x = new TreeMap<>();
    x.put("hello-world", 12);
    x.put("hello-world123", 21);
    x.put("still", 123);

    FST<Long> fst = FSTBuilder.buildFST(x);
    File outputFile = new File(TEMP_DIR, "test.lucene");
    FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
    OutputStreamDataOutput d = new OutputStreamDataOutput(fileOutputStream);
    fst.save(d, d);
    fileOutputStream.close();

    Outputs<Long> outputs = PositiveIntOutputs.getSingleton();
    File fstFile = new File(outputFile.getAbsolutePath());

    try (PinotDataBuffer pinotDataBuffer =
        PinotDataBuffer.mapFile(fstFile, true, 0, fstFile.length(), ByteOrder.BIG_ENDIAN, "")) {
      PinotBufferIndexInput indexInput = new PinotBufferIndexInput(pinotDataBuffer, 0L, fstFile.length());

      List<Long> results = RegexpMatcher.regexMatch("hello.*123", fst);
      Assert.assertEquals(results.size(), 1);
      Assert.assertEquals(results.get(0).longValue(), 21L);

      results = RegexpMatcher.regexMatch(".*world", fst);
      Assert.assertEquals(results.size(), 1);
      Assert.assertEquals(results.get(0).longValue(), 12L);
    }
  }

  @Test
  public void testCaseInsensitiveFSTBuilder()
      throws IOException {
    SortedMap<String, Integer> x = new TreeMap<>();
    x.put("Hello-World", 12);
    x.put("HELLO-WORLD", 13);  // Same key in different case
    x.put("hello-world123", 21);
    x.put("HELLO-WORLD123", 22);  // Same key in different case
    x.put("Still", 123);
    x.put("STILL", 124);  // Same key in different case

    // Build case-insensitive FST
    FST<?> fst = FSTBuilder.buildFST(x, false); // caseSensitive = false

    // Save to file
    File outputFile = new File(TEMP_DIR, "test_case_insensitive.lucene");
    FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
    OutputStreamDataOutput d = new OutputStreamDataOutput(fileOutputStream);
    fst.save(d, d);
    fileOutputStream.close();

    File fstFile = new File(outputFile.getAbsolutePath());

    try (PinotDataBuffer pinotDataBuffer =
        PinotDataBuffer.mapFile(fstFile, true, 0, fstFile.length(), ByteOrder.BIG_ENDIAN, "")) {
      PinotBufferIndexInput indexInput = new PinotBufferIndexInput(pinotDataBuffer, 0L, fstFile.length());

      // Test that case-insensitive matching works
      List<Long> results = RegexpMatcher.regexMatch("hello.*world", fst);
      Assert.assertEquals(results.size(), 2); // Should match both "Hello-World" and "HELLO-WORLD"
      Assert.assertTrue(results.contains(12L));
      Assert.assertTrue(results.contains(13L));

      results = RegexpMatcher.regexMatch("hello.*world123", fst);
      Assert.assertEquals(results.size(), 2); // Should match both "hello-world123" and "HELLO-WORLD123"
      Assert.assertTrue(results.contains(21L));
      Assert.assertTrue(results.contains(22L));

      results = RegexpMatcher.regexMatch("still", fst);
      Assert.assertEquals(results.size(), 2); // Should match both "Still" and "STILL"
      Assert.assertTrue(results.contains(123L));
      Assert.assertTrue(results.contains(124L));

      // Test with uppercase regex
      results = RegexpMatcher.regexMatch("HELLO.*WORLD", fst);
      Assert.assertEquals(results.size(), 2); // Should still match both
      Assert.assertTrue(results.contains(12L));
      Assert.assertTrue(results.contains(13L));

      // Test with mixed case regex
      results = RegexpMatcher.regexMatch("HeLLo.*WoRlD", fst);
      Assert.assertEquals(results.size(), 2); // Should still match both
      Assert.assertTrue(results.contains(12L));
      Assert.assertTrue(results.contains(13L));
    }
  }

  @Test
  public void testSerializeDeserializeIntegerList()
      throws Exception {
    // Test empty list
    List<Integer> emptyList = new ArrayList<>();
    BytesRef serializedEmpty = FSTBuilder.serializeIntegerList(emptyList);
    List<Integer> deserializedEmpty = FSTBuilder.deserializeBytesRefToIntegerList(serializedEmpty);
    Assert.assertEquals(deserializedEmpty.size(), 0);
    Assert.assertTrue(deserializedEmpty.isEmpty());

    // Test single value
    List<Integer> singleList = new ArrayList<>();
    singleList.add(42);
    BytesRef serializedSingle = FSTBuilder.serializeIntegerList(singleList);
    List<Integer> deserializedSingle = FSTBuilder.deserializeBytesRefToIntegerList(serializedSingle);
    Assert.assertEquals(deserializedSingle.size(), 1);
    Assert.assertEquals(deserializedSingle.get(0).intValue(), 42);

    // Test multiple values
    List<Integer> multiList = new ArrayList<>();
    multiList.add(1);
    multiList.add(2);
    multiList.add(3);
    multiList.add(4);
    multiList.add(5);
    BytesRef serializedMulti = FSTBuilder.serializeIntegerList(multiList);
    List<Integer> deserializedMulti = FSTBuilder.deserializeBytesRefToIntegerList(serializedMulti);
    Assert.assertEquals(deserializedMulti.size(), 5);
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(deserializedMulti.get(i).intValue(), i + 1);
    }

    // Test large values
    List<Integer> largeList = new ArrayList<>();
    largeList.add(Integer.MAX_VALUE);
    largeList.add(Integer.MIN_VALUE);
    largeList.add(0);
    BytesRef serializedLarge = FSTBuilder.serializeIntegerList(largeList);
    List<Integer> deserializedLarge = FSTBuilder.deserializeBytesRefToIntegerList(serializedLarge);
    Assert.assertEquals(deserializedLarge.size(), 3);
    Assert.assertEquals(deserializedLarge.get(0).intValue(), Integer.MAX_VALUE);
    Assert.assertEquals(deserializedLarge.get(1).intValue(), Integer.MIN_VALUE);
    Assert.assertEquals(deserializedLarge.get(2).intValue(), 0);

    // Test null input (should throw exception)
    try {
      FSTBuilder.serializeIntegerList(null);
      Assert.fail("Expected IllegalArgumentException for null input");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    // Test null BytesRef (should return empty list)
    List<Integer> nullResult = FSTBuilder.deserializeBytesRefToIntegerList(null);
    Assert.assertEquals(nullResult.size(), 0);
    Assert.assertTrue(nullResult.isEmpty());

    // Test empty BytesRef (should return empty list)
    List<Integer> emptyResult = FSTBuilder.deserializeBytesRefToIntegerList(new BytesRef());
    Assert.assertEquals(emptyResult.size(), 0);
    Assert.assertTrue(emptyResult.isEmpty());

    // Test BytesRef with insufficient bytes (should throw exception)
    byte[] insufficientBytes = new byte[3]; // Less than 4 bytes for count
    BytesRef insufficientBytesRef = new BytesRef(insufficientBytes);
    try {
      FSTBuilder.deserializeBytesRefToIntegerList(insufficientBytesRef);
      Assert.fail("Expected RuntimeException for insufficient bytes");
    } catch (RuntimeException e) {
      // Expected
      Assert.assertTrue(e.getMessage().contains("not enough bytes for list size"));
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
    BytesRef serializedNegative = FSTBuilder.serializeIntegerList(negativeList);
    List<Integer> deserializedNegative = FSTBuilder.deserializeBytesRefToIntegerList(serializedNegative);
    Assert.assertEquals(deserializedNegative.size(), 3);
    Assert.assertEquals(deserializedNegative.get(0).intValue(), -1);
    Assert.assertEquals(deserializedNegative.get(1).intValue(), -100);
    Assert.assertEquals(deserializedNegative.get(2).intValue(), -1000);

    // Test with mixed positive and negative values
    List<Integer> mixedList = new ArrayList<>();
    mixedList.add(-5);
    mixedList.add(0);
    mixedList.add(5);
    mixedList.add(-10);
    mixedList.add(10);
    BytesRef serializedMixed = FSTBuilder.serializeIntegerList(mixedList);
    List<Integer> deserializedMixed = FSTBuilder.deserializeBytesRefToIntegerList(serializedMixed);
    Assert.assertEquals(deserializedMixed.size(), 5);
    Assert.assertEquals(deserializedMixed.get(0).intValue(), -5);
    Assert.assertEquals(deserializedMixed.get(1).intValue(), 0);
    Assert.assertEquals(deserializedMixed.get(2).intValue(), 5);
    Assert.assertEquals(deserializedMixed.get(3).intValue(), -10);
    Assert.assertEquals(deserializedMixed.get(4).intValue(), 10);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
