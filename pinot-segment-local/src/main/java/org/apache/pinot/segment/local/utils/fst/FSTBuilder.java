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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  Builds FST using lucene org.apache.lucene.util.fst.Builder library. FSTBuilder requires all the key/values
 *  be added in sorted order. Supports both case-sensitive (FST<Long>) and case-insensitive (FST<BytesRef>) modes.
 */
public class FSTBuilder {
  public static final Logger LOGGER = LoggerFactory.getLogger(FSTBuilder.class);

  private final boolean _caseSensitive;
  private final FSTCompiler<Long> _longFstCompiler;
  private final FSTCompiler<BytesRef> _bytesRefFstCompiler;
  private final IntsRefBuilder _scratch = new IntsRefBuilder();

  // For case-insensitive mode: aggregate values by normalized keys
  private final Map<String, List<Integer>> _aggregatedValues = new TreeMap<>();

  public FSTBuilder() {
    this(true); // Default to case-sensitive for backward compatibility
  }

  public FSTBuilder(boolean caseSensitive) {
    _caseSensitive = caseSensitive;
    if (caseSensitive) {
      _longFstCompiler = (new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE4, PositiveIntOutputs.getSingleton())).build();
      _bytesRefFstCompiler = null;
    } else {
      _longFstCompiler = null;
      _bytesRefFstCompiler =
          (new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE4, ByteSequenceOutputs.getSingleton())).build();
    }
  }

  public static FST<Long> buildFST(SortedMap<String, Integer> input)
      throws IOException {
    return (FST<Long>) buildFST(input, true); // Default to case-sensitive
  }

  public static FST<?> buildFST(SortedMap<String, Integer> input, boolean caseSensitive)
      throws IOException {
    if (caseSensitive) {
      return buildCaseSensitiveFST(input);
    } else {
      return buildCaseInsensitiveFST(input);
    }
  }

  private static FST<Long> buildCaseSensitiveFST(SortedMap<String, Integer> input)
      throws IOException {
    PositiveIntOutputs fstOutput = PositiveIntOutputs.getSingleton();
    FSTCompiler.Builder<Long> fstCompilerBuilder = new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE4, fstOutput);
    FSTCompiler<Long> fstCompiler = fstCompilerBuilder.build();

    IntsRefBuilder scratch = new IntsRefBuilder();
    for (Map.Entry<String, Integer> entry : input.entrySet()) {
      fstCompiler.add(Util.toUTF16(entry.getKey(), scratch), entry.getValue().longValue());
    }

    return FST.fromFSTReader(fstCompiler.compile(), fstCompiler.getFSTReader());
  }

  private static FST<BytesRef> buildCaseInsensitiveFST(SortedMap<String, Integer> input)
      throws IOException {
    // Aggregate values by normalized (lowercase) keys
    Map<String, List<Integer>> aggregatedValues = new TreeMap<>();
    for (Map.Entry<String, Integer> entry : input.entrySet()) {
      String normalizedKey = entry.getKey().toLowerCase();
      aggregatedValues.computeIfAbsent(normalizedKey, k -> new ArrayList<>()).add(entry.getValue());
    }

    // Build FST with aggregated values using direct FST compiler
    FSTCompiler<BytesRef> fstCompiler =
        (new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE4, ByteSequenceOutputs.getSingleton())).build();
    IntsRefBuilder scratch = new IntsRefBuilder();

    for (Map.Entry<String, List<Integer>> entry : aggregatedValues.entrySet()) {
      String normalizedKey = entry.getKey();
      List<Integer> values = entry.getValue();

      // Serialize the list of integers to BytesRef
      BytesRef serializedValue = serializeIntegerList(values);
      fstCompiler.add(Util.toUTF16(normalizedKey, scratch), serializedValue);
    }

    return FST.fromFSTReader(fstCompiler.compile(), fstCompiler.getFSTReader());
  }

  public void addEntry(String key, Integer value)
      throws IOException {
    if (_caseSensitive) {
      _longFstCompiler.add(Util.toUTF16(key, _scratch), value.longValue());
    } else {
      // For case-insensitive, aggregate values by normalized key
      String normalizedKey = key.toLowerCase();
      _aggregatedValues.computeIfAbsent(normalizedKey, k -> new ArrayList<>()).add(value);
    }
  }

  public FST<?> done()
      throws IOException {
    if (_caseSensitive) {
      return FST.fromFSTReader(_longFstCompiler.compile(), _longFstCompiler.getFSTReader());
    } else {
      // Build FST with aggregated values using _bytesRefFstCompiler
      for (Map.Entry<String, List<Integer>> entry : _aggregatedValues.entrySet()) {
        String normalizedKey = entry.getKey();
        List<Integer> values = entry.getValue();

        // Serialize the list of integers to BytesRef
        BytesRef serializedValue = serializeIntegerList(values);
        _bytesRefFstCompiler.add(Util.toUTF16(normalizedKey, _scratch), serializedValue);
      }

      return FST.fromFSTReader(_bytesRefFstCompiler.compile(), _bytesRefFstCompiler.getFSTReader());
    }
  }

  /**
   * Helper method to serialize a List<Integer> into a BytesRef.
   * Uses simple format: [4-byte count][4-byte value1][4-byte value2]...
   * @param integerList The list of integers to serialize.
   * @return A BytesRef containing the serialized integers.
   */
  public static BytesRef serializeIntegerList(List<Integer> integerList) {
    if (integerList == null) {
      throw new IllegalArgumentException("Cannot serialize null integer list");
    }
    ByteBuffer buffer = ByteBuffer.allocate(4 + (integerList.size() * 4));
    buffer.putInt(integerList.size());
    for (Integer value : integerList) {
      buffer.putInt(value);
    }
    return new BytesRef(buffer.array(), 0, buffer.position());
  }

  public static List<Integer> deserializeBytesRefToIntegerList(BytesRef bytesRef) {
    if (bytesRef == null || bytesRef.length == 0) {
      return new ArrayList<>();
    }

    ByteBuffer buffer = ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length);
    if (buffer.remaining() < 4) {
      throw new RuntimeException("Corrupt BytesRef: not enough bytes for list size. Length: " + bytesRef.length);
    }
    int listSize = buffer.getInt();
    List<Integer> result = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      if (buffer.remaining() < 4) {
        throw new RuntimeException(
            "Corrupt BytesRef: not enough bytes for integer at index " + i + ". Expected " + listSize
                + " integers but only found " + i);
      }
      result.add(buffer.getInt());
    }
    return result;
  }
}
