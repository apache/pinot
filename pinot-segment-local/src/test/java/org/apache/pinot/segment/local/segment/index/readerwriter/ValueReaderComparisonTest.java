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
package org.apache.pinot.segment.local.segment.index.readerwriter;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.segment.local.io.util.ValueReader;
import org.apache.pinot.segment.local.io.util.VarLengthValueReader;
import org.apache.pinot.segment.local.io.util.VarLengthValueWriter;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ValueReaderComparisonTest {

  // Number of rounds to run the test for, change this number to test locally for catching the corner cases.
  private static final int NUM_ROUNDS = 1;

  @DataProvider
  public static Object[] text() {
    return Collections.nCopies(NUM_ROUNDS,
            Stream.of(
                    Pair.of(ByteOrder.BIG_ENDIAN, true),
                    Pair.of(ByteOrder.LITTLE_ENDIAN, true),
                    Pair.of(ByteOrder.BIG_ENDIAN, false))
                .flatMap(
                    pair -> Stream.of(
                        new AsciiTestCase(pair.getLeft(), pair.getRight()),
                        new Utf8TestCase(pair.getLeft(), pair.getRight()),
                        new RandomBytesTextTestCase(pair.getLeft(), pair.getRight()),
                        new OrderedInvalidUtf8TestCase(pair.getLeft(), pair.getRight())))
                .collect(Collectors.toList()))
        .stream()
        .flatMap(List::stream).toArray(Object[]::new);
  }

  static abstract class TestCase {

    private final ByteOrder _byteOrder;
    private final boolean _fixed;

    public TestCase(ByteOrder byteOrder, boolean fixed) {
      _byteOrder = byteOrder;
      _fixed = fixed;
    }

    ByteOrder getByteOrder() {
      return _byteOrder;
    }

    boolean isFixed() {
      return _fixed;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " " + _byteOrder + (_fixed ? " fixed width" : " variable width");
    }
  }

  static abstract class TextTestCase extends TestCase {

    public TextTestCase(ByteOrder byteOrder, boolean fixed) {
      super(byteOrder, fixed);
    }

    abstract String[] generateStrings();

    void testEqual(ValueReader reader, int numBytesPerValue, String... strings) {
      for (int i = 0; i < strings.length; i++) {
        assertUtf8Comparison(reader, i, numBytesPerValue, strings[i], 0);
      }
    }

    void testEmpty(ValueReader reader, int numBytesPerValue, String... strings) {
      for (int i = 0; i < strings.length; i++) {
        assertUtf8Comparison(reader, i, numBytesPerValue, "", strings[i].isEmpty() ? 0 : 1);
      }
    }

    void testCommonPrefixStoredLonger(ValueReader reader, int numBytesPerValue, String... strings) {
      for (int i = 0; i < strings.length; i++) {
        if (!strings[i].isEmpty()) {
          assertUtf8Comparison(reader, i, numBytesPerValue, strings[i].substring(0, strings[i].length() - 1), 1);
        }
      }
    }

    void testCommonPrefixParameterLonger(ValueReader reader, int numBytesPerValue, String... strings) {
      for (int i = 0; i < strings.length; i++) {
        assertUtf8Comparison(reader, i, numBytesPerValue, strings[i] + "a", -1);
        assertUtf8Comparison(reader, i, numBytesPerValue, strings[i] + fromHex("efbfbdd8"), -1);
        assertUtf8Comparison(reader, i, numBytesPerValue, strings[i] + "\uD841\uDF0E", -1);
      }
    }

    void testCommonPrefixParameterOverflowing(ValueReader reader, int numBytesPerValue, String... strings) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < numBytesPerValue; i++) {
        sb.append("a");
      }
      String overflow = sb.toString();
      for (int i = 0; i < strings.length; i++) {
        assertUtf8Comparison(reader, i, numBytesPerValue, strings[i] + overflow, -1);
      }
    }

    void testInferiorPrefixes(ValueReader reader, int numBytesPerValue, String... strings) {
      for (int i = 0; i < strings.length; i++) {
        char[] chars = strings[i].toCharArray();
        for (int j = 0; j < strings[i].length(); j++) {
          chars[j]--;
          assertUtf8Comparison(reader, i, numBytesPerValue, new String(chars), 1);
          chars[j]++; // fix the string for the next iteration
        }
      }
    }

    void testSuperiorPrefixes(ValueReader reader, int numBytesPerValue, String... strings) {
      for (int i = 0; i < strings.length; i++) {
        char[] chars = strings[i].toCharArray();
        for (int j = 0; j < strings[i].length(); j++) {
          // this test's ordering assumption is not valid for surrogates
          char nextChar = (char) (chars[j] + 1);
          if (!Character.isSurrogate(chars[j]) && !Character.isSurrogate(nextChar)) {
            chars[j]++;
            String string = new String(chars);
            int signum = strings[i].compareTo(string);
            assertUtf8Comparison(reader, i, numBytesPerValue, string, Integer.compare(signum, 0));
            chars[j]--; // fix the string for the next iteration
          }
        }
      }
    }

    void testConsistent(ValueReader reader, int numBytesPerValue, int numValuesToCompare) {
      for (int i = 0; i < numValuesToCompare; i++) {
        byte[] bytes = new byte[ThreadLocalRandom.current().nextInt(numBytesPerValue * 2)];

        for (int j = 1; j < 128; j++) {
          Arrays.fill(bytes, (byte) j);
          assertConsistentUtf8Comparison(reader, i, numBytesPerValue, new String(bytes, StandardCharsets.UTF_8));
        }

        byte[] utf8 = "ß".getBytes(StandardCharsets.UTF_8);
        for (int j = 0; j + 1 < bytes.length; j += 2) {
          bytes[j] = utf8[0];
          bytes[j + 1] = utf8[1];
        }
        assertConsistentUtf8Comparison(reader, i, numBytesPerValue, new String(bytes, StandardCharsets.UTF_8));

        utf8 = "道".getBytes(StandardCharsets.UTF_8);
        for (int j = 0; j + 2 < bytes.length; j += 3) {
          bytes[j] = utf8[0];
          bytes[j + 1] = utf8[1];
          bytes[j + 2] = utf8[2];
        }
        assertConsistentUtf8Comparison(reader, i, numBytesPerValue, new String(bytes, StandardCharsets.UTF_8));

        utf8 = "\uD841\uDF0E".getBytes(StandardCharsets.UTF_8);
        for (int j = 0; j + 3 < bytes.length; j += 4) {
          bytes[j] = utf8[0];
          bytes[j + 1] = utf8[1];
          bytes[j + 2] = utf8[2];
          bytes[j + 3] = utf8[3];
        }
        assertConsistentUtf8Comparison(reader, i, numBytesPerValue, new String(bytes, StandardCharsets.UTF_8));

        ThreadLocalRandom.current().nextBytes(bytes);
        for (int j = 0; j < bytes.length; j++) {
          if (bytes[j] == 0) {
            bytes[j] = '0';
          }
        }
        assertConsistentUtf8Comparison(reader, i, numBytesPerValue, new String(bytes, StandardCharsets.UTF_8));

        assertConsistentUtf8Comparison(reader, i, numBytesPerValue, fromHex("efbfbdd8"));
        assertConsistentUtf8Comparison(reader, i, numBytesPerValue, fromHex("7fbfbdd8"));
        assertConsistentUtf8Comparison(reader, i, numBytesPerValue, fromHex("7f818181"));
        assertConsistentUtf8Comparison(reader, i, numBytesPerValue, fromHex("7ff2bd9fbf"));
        assertConsistentUtf8Comparison(reader, i, numBytesPerValue, fromHex("7f3fee8080"));
      }
    }
  }

  static class AsciiTestCase extends TextTestCase {

    public AsciiTestCase(ByteOrder byteOrder, boolean fixed) {
      super(byteOrder, fixed);
    }

    @Override
    public String[] generateStrings() {
      char[] symbols = "abcdefghijklmnopqrstuvwxyz0123456789".toCharArray();
      // generate [a, ab, abc, ...]
      return IntStream.range(0, 33).mapToObj(size -> new String(Arrays.copyOf(symbols, size))).toArray(String[]::new);
    }
  }

  static class Utf8TestCase extends TextTestCase {
    public Utf8TestCase(ByteOrder byteOrder, boolean fixed) {
      super(byteOrder, fixed);
    }

    @Override
    public String[] generateStrings() {
      String[] symbols = new String[]{"a", "ß", "道", "\uD841\uDF0E", "é", "e\u0301"};
      return IntStream.range(1, 100).mapToObj(size -> {
        StringBuilder sb = new StringBuilder();
        int offset = size % symbols.length;
        for (int i = 0; i < size; i++) {
          sb.append(symbols[(i + offset) % symbols.length]);
        }
        return sb.toString();
      }).toArray(String[]::new);
    }
  }

  static class RandomBytesTextTestCase extends TextTestCase {
    public RandomBytesTextTestCase(ByteOrder byteOrder, boolean fixed) {
      super(byteOrder, fixed);
    }

    @Override
    String[] generateStrings() {
      return IntStream.range(1, 128).mapToObj(size -> {
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        // String sanitization truncates to first zero, so remove zeros here
        for (int i = 0; i < size; i++) {
          if (bytes[i] == 0) {
            bytes[i] = '0';
          }
        }
        return new String(bytes, StandardCharsets.UTF_8);
      }).toArray(String[]::new);
    }
  }

  static class OrderedInvalidUtf8TestCase extends TextTestCase {

    public OrderedInvalidUtf8TestCase(ByteOrder byteOrder, boolean fixed) {
      super(byteOrder, fixed);
    }

    @Override
    String[] generateStrings() {
      String[] strings = new String[7];
      strings[0] = fromHex("edbfbdd8bb");
      strings[1] = fromHex("eebfbdd8bb");
      strings[2] = fromHex("efbfbdd8bb");
      strings[3] = fromHex("f0bfbdd8bb");
      strings[4] = fromHex("f0a09c8e");
      strings[5] = fromHex("7ff2bd9fbf");
      strings[6] = fromHex("7f3fee8080");
      return strings;
    }
  }

  @Test(dataProvider = "text")
  public void testUtf8Comparison(TextTestCase testCase)
      throws IOException {
    String[] strings = testCase.generateStrings();
    int maxUtf8Length = Integer.MIN_VALUE;
    for (String string : strings) {
      maxUtf8Length = Math.max(string.getBytes(StandardCharsets.UTF_8).length, maxUtf8Length);
    }
    // round up to next power of 2
    int numBytesPerValue =
        (maxUtf8Length & -maxUtf8Length) == 0 ? maxUtf8Length : 1 << (32 - Integer.numberOfLeadingZeros(maxUtf8Length));
    Pair<ValueReader, PinotDataBuffer> readerAndBuffer =
        prepare(testCase.isFixed(), numBytesPerValue, testCase.getByteOrder(), strings);
    ValueReader reader = readerAndBuffer.getKey();
    try {
      testCase.testEqual(reader, numBytesPerValue, strings);
      testCase.testEmpty(reader, numBytesPerValue, strings);
      testCase.testCommonPrefixStoredLonger(reader, numBytesPerValue, strings);
      testCase.testCommonPrefixParameterLonger(reader, numBytesPerValue, strings);
      testCase.testCommonPrefixParameterOverflowing(reader, numBytesPerValue, strings);
      testCase.testInferiorPrefixes(reader, numBytesPerValue, strings);
      testCase.testSuperiorPrefixes(reader, numBytesPerValue, strings);
      testCase.testConsistent(reader, numBytesPerValue, strings.length);
    } finally {
      readerAndBuffer.getValue().close();
    }
  }

  private static void assertUtf8Comparison(ValueReader readerWriter, int index, int numBytesPerValue, String string,
      int signum) {
    byte[] value = string.getBytes(StandardCharsets.UTF_8);
    String stored = readerWriter.getUnpaddedString(index, numBytesPerValue, new byte[numBytesPerValue]);
    String error = stored + " " + string;
    int comparisonViaMaterialization = stored.compareTo(string);
    assertEquals(Integer.compare(comparisonViaMaterialization, 0), signum, error);
    int utf8Comparison = readerWriter.compareUtf8Bytes(index, numBytesPerValue, value);
    assertEquals(Integer.compare(utf8Comparison, 0), signum, error);
  }

  private static void assertConsistentUtf8Comparison(ValueReader readerWriter, int index, int numBytesPerValue,
      String string) {
    byte[] value = string.getBytes(StandardCharsets.UTF_8);
    int utf8Comparison = readerWriter.compareUtf8Bytes(index, numBytesPerValue, value);
    String stored = readerWriter.getUnpaddedString(index, numBytesPerValue, new byte[numBytesPerValue]);
    int comparisonViaMaterialization = stored.compareTo(string);
    assertTrue(
        (utf8Comparison == comparisonViaMaterialization) || (utf8Comparison < 0 && comparisonViaMaterialization < 0)
            || (utf8Comparison > 0 && comparisonViaMaterialization > 0),
        "stored=" + BytesUtils.toHexString(stored.getBytes(StandardCharsets.UTF_8)) + ", parameter="
            + BytesUtils.toHexString(value));
  }

  private static Pair<ValueReader, PinotDataBuffer> prepare(boolean fixed, int numBytesPerValue, ByteOrder byteOrder,
      String... storedValues)
      throws IOException {
    byte[][] bytes = new byte[storedValues.length][];
    for (int i = 0; i < storedValues.length; i++) {
      bytes[i] = storedValues[i].getBytes(StandardCharsets.UTF_8);
    }
    return prepare(fixed, numBytesPerValue, byteOrder, bytes);
  }

  private static Pair<ValueReader, PinotDataBuffer> prepare(boolean fixed, int numBytesPerValue, ByteOrder byteOrder,
      byte[]... storedValues)
      throws IOException {
    if (fixed) {
      PinotDataBuffer buffer =
          PinotDataBuffer.allocateDirect(1000L + (long) storedValues.length * numBytesPerValue, byteOrder,
              "ValueReaderComparisonTest");
      FixedByteValueReaderWriter readerWriter = new FixedByteValueReaderWriter(buffer);
      for (int i = 0; i < storedValues.length; i++) {
        readerWriter.writeBytes(i, numBytesPerValue, storedValues[i]);
      }
      return Pair.of(readerWriter, buffer);
    } else {
      assert byteOrder == ByteOrder.BIG_ENDIAN : "little endian unsupported by VarLengthValueWriter";
      Path file = Files.createTempFile(ValueReaderComparisonTest.class.getName() + "-" + UUID.randomUUID(), ".tmp");
      VarLengthValueWriter writer = new VarLengthValueWriter(file.toFile(), storedValues.length);
      for (byte[] storedValue : storedValues) {
        writer.add(storedValue);
      }
      writer.close();
      PinotDataBuffer buffer =
          PinotDataBuffer.mapFile(file.toFile(), true, 0, Files.size(file), byteOrder, "ValueReaderComparisonTest");
      return Pair.of(new VarLengthValueReader(buffer), buffer);
    }
  }

  private static String fromHex(String hex) {
    return new String(BytesUtils.toBytes(hex), StandardCharsets.UTF_8);
  }
}
