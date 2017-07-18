/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.readers;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import joptsimple.internal.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of String immutable dictionary based using on-heap String array and HashMap.
 * This is useful for:
 * <ul>
 *   <li> Low cardinality string dictionaries where memory footprint on-heap is acceptably small. </li>
 *   <li> Heavily queried columns: This helps avoid creation of String from byte[], which is expensive
 *        as well as creates garbage. </li>
 * </ul>
 * This is useful for low cardinality dictionaries that can be stored in-heap with small memory footprint.
 * For
 */
public class OnHeapStringDictionary extends ImmutableDictionaryReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(OnHeapStringDictionary.class);
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private final int _lengthOfMaxEntry;
  private final char _paddingChar;

  private String[] _idToStringMap;
  private String[] _idToRawStringMap;
  private Map<String, Integer> _stringToIdMap;

  /**
   * Constructor for the class.
   *
   * @param dataBuffer Pinot Data buffer for the dictionary
   * @param metadata Column metadata
   */
  public OnHeapStringDictionary(PinotDataBuffer dataBuffer, ColumnMetadata metadata) {
    super(dataBuffer, metadata.getCardinality(), metadata.getStringColumnMaxLength());
    _lengthOfMaxEntry = metadata.getStringColumnMaxLength();
    _paddingChar = metadata.getPaddingCharacter();

    int cardinality = metadata.getCardinality();
    _idToStringMap = new String[cardinality];
    _idToRawStringMap = new String[cardinality];
    _stringToIdMap = new HashMap<>(cardinality);

    for (int id = 0; id < cardinality; id++) {
      byte[] bytes = dataFileReader.getBytes(id, 0);
      String value = getStringFromBytes(bytes, true);
      _idToStringMap[id] = value;
      _idToRawStringMap[id] = getStringFromBytes(bytes, false);
      _stringToIdMap.put(value, id);
    }

    // Close the data-buffer since on-heap dictionary has already been built.
    try {
      close();
    } catch (IOException e) {
      LOGGER.error("Error closing data-buffer for on-heap dictionary for column: " + metadata.getColumnName());
      Utils.rethrowException(e);
    }
    dataBuffer.close();
  }

  @Override
  @SuppressWarnings("unchecked")
  public int indexOf(Object rawValue) {
    final String lookup = (String) rawValue; // This will always be a string
    Integer index = _stringToIdMap.get(lookup);
    return (index != null) ? index : -1;
  }

  @Override
  public String get(int dictionaryId) {
    if ((dictionaryId == -1) || (dictionaryId >= length())) {
      return "null";
    }
    return _idToStringMap[dictionaryId];
  }

  @Override
  public long getLongValue(int dictionaryId) {
    throw new RuntimeException("cannot converted string to long");
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    throw new RuntimeException("cannot converted string to double");
  }

  @Override
  public int getIntValue(int dictionaryId) {
    throw new RuntimeException("cannot converted string to int");
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    throw new RuntimeException("cannot converted string to float");
  }

  @Override
  public String getStringValue(int dictionaryId) {
    if ((dictionaryId == -1) || (dictionaryId >= length())) {
      return "null";
    }
    return _idToRawStringMap[dictionaryId];
  }

  @Override
  public void readIntValues(int[] dictionaryIds, int startPos, int limit, int[] outValues, int outStartPos) {
    throw new RuntimeException("Can not convert string to int");
  }

  @Override
  public void readLongValues(int[] dictionaryIds, int startPos, int limit, long[] outValues, int outStartPos) {
    throw new RuntimeException("Can not convert string to long");
  }

  @Override
  public void readFloatValues(int[] dictionaryIds, int startPos, int limit, float[] outValues, int outStartPos) {
    throw new RuntimeException("Can not convert string to float");
  }

  @Override
  public void readDoubleValues(int[] dictionaryIds, int startPos, int limit, double[] outValues, int outStartPos) {
    throw new RuntimeException("Can not convert string to double");
  }

  @Override
  public void readStringValues(int[] dictionaryIds, int startPos, int limit, String[] outValues, int outStartPos) {
    for (int i = 0; i < limit; i++) {
      outValues[i + outStartPos] = get(dictionaryIds[i + startPos]);
    }
  }

  /**
   * Helper method to read string value from the data file.
   *
   * @param bytes Raw bytes from which to build String.
   * @param stripPadding Strip padding character if true.
   * @return String value corresponding to the raw bytes
   */
  private String getStringFromBytes(byte[] bytes, boolean stripPadding) {
    if (!stripPadding) {
      return new String(bytes, UTF_8);
    } else {
      for (int i = _lengthOfMaxEntry - 1; i >= 0; i--) {
        if (bytes[i] != _paddingChar) {
          return new String(bytes, 0, i + 1, UTF_8);
        }
      }
      return "";
    }
  }
}
