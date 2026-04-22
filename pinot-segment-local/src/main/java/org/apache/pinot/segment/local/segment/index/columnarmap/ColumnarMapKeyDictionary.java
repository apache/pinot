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
package org.apache.pinot.segment.local.segment.index.columnarmap;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * In-memory {@link Dictionary} implementation backed by sorted distinct values extracted from the
 * columnar map value dictionary section. Enables dictionary-based GROUP BY for columnar map key columns.
 *
 * <p>All values are stored as sorted strings regardless of the key's declared type, with typed
 * parsing on demand (e.g. {@code Integer.parseInt} in {@link #getIntValue}). This string-based
 * approach was chosen because:
 * <ul>
 *   <li>SQL predicates arrive as strings ({@code WHERE metrics['clicks'] = '42'}) — even with
 *       native typed arrays, the predicate string would need parsing for binary search</li>
 *   <li>Dictionary value reads only happen when materializing final results (small result set);
 *       the hot path for GROUP BY is {@code readDictIds()} on the bit-packed forward index</li>
 *   <li>A single code path for all types keeps the writer, reader, and dictionary simple —
 *       typed arrays would mean 4+ code paths with more surface area for bugs</li>
 * </ul>
 *
 * <p>TODO: If profiling shows dictionary value access is a bottleneck, switch to native typed
 * arrays (int[], long[], double[], String[]) for zero-parse forward lookups and type-aware
 * binary search for indexOf. This would require corresponding changes in
 * {@link OnHeapColumnarMapIndexCreator#buildValueDictionarySection} (write fixed-width values)
 * and {@link ImmutableColumnarMapIndexReader} (read into typed arrays).
 *
 * <p>The reverse map provides O(1) string-to-dictId lookup. Since the source values come from a
 * sorted inverted index (TreeMap or sorted binary scan), the dictionary is always sorted.
 *
 * <p>Thread safety: this class is immutable after construction and safe for concurrent reads.
 */
public class ColumnarMapKeyDictionary implements Dictionary {

  private final DataType _valueType;
  private final String[] _sortedValues;
  private final Object2IntOpenHashMap<String> _valueToIdMap;

  public ColumnarMapKeyDictionary(DataType valueType, String[] sortedDistinctValues) {
    _valueType = valueType;
    _sortedValues = sortedDistinctValues;
    _valueToIdMap = new Object2IntOpenHashMap<>(sortedDistinctValues.length);
    _valueToIdMap.defaultReturnValue(NULL_VALUE_INDEX);
    for (int i = 0; i < sortedDistinctValues.length; i++) {
      _valueToIdMap.put(sortedDistinctValues[i], i);
    }
  }

  @Override
  public boolean isSorted() {
    return true;
  }

  @Override
  public DataType getValueType() {
    return _valueType;
  }

  @Override
  public int length() {
    return _sortedValues.length;
  }

  @Override
  public int indexOf(String stringValue) {
    return _valueToIdMap.getInt(stringValue);
  }

  @Override
  public int indexOf(int intValue) {
    return indexOf(String.valueOf(intValue));
  }

  @Override
  public int indexOf(long longValue) {
    return indexOf(String.valueOf(longValue));
  }

  @Override
  public int indexOf(float floatValue) {
    return indexOf(String.valueOf(floatValue));
  }

  @Override
  public int indexOf(double doubleValue) {
    return indexOf(String.valueOf(doubleValue));
  }

  @Override
  public int indexOf(BigDecimal bigDecimalValue) {
    return indexOf(bigDecimalValue.toPlainString());
  }

  @Override
  public int indexOf(ByteArray bytesValue) {
    return indexOf(bytesValue.toHexString());
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    int index = _valueToIdMap.getInt(stringValue);
    if (index != NULL_VALUE_INDEX) {
      return index;
    }
    Comparator<String> cmp = getComparator(_valueType);
    if (cmp != null) {
      return Arrays.binarySearch(_sortedValues, stringValue, cmp);
    }
    return Arrays.binarySearch(_sortedValues, stringValue);
  }

  @Override
  public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
    int numValues = _sortedValues.length;
    if (numValues == 0) {
      return IntSets.EMPTY_SET;
    }

    // Determine the start index (inclusive) using binary search on the sorted array.
    int startIndex;
    if (lower.equals(RangePredicate.UNBOUNDED)) {
      startIndex = 0;
    } else {
      int idx = binarySearch(lower);
      if (idx >= 0) {
        // Exact match found
        startIndex = includeLower ? idx : idx + 1;
      } else {
        // Not found: insertion point is -(idx + 1), the first element greater than lower
        startIndex = -(idx + 1);
      }
    }

    // Determine the end index (exclusive) using binary search on the sorted array.
    int endIndex;
    if (upper.equals(RangePredicate.UNBOUNDED)) {
      endIndex = numValues;
    } else {
      int idx = binarySearch(upper);
      if (idx >= 0) {
        // Exact match found
        endIndex = includeUpper ? idx + 1 : idx;
      } else {
        // Not found: insertion point is the first element greater than upper
        endIndex = -(idx + 1);
      }
    }

    int count = endIndex - startIndex;
    if (count <= 0) {
      return IntSets.EMPTY_SET;
    }
    IntSet dictIds = new IntOpenHashSet(count);
    for (int i = startIndex; i < endIndex; i++) {
      dictIds.add(i);
    }
    return dictIds;
  }

  /// Performs a binary search on the sorted values array using the type-appropriate comparator.
  private int binarySearch(String value) {
    Comparator<String> cmp = getComparator(_valueType);
    if (cmp != null) {
      return Arrays.binarySearch(_sortedValues, value, cmp);
    }
    return Arrays.binarySearch(_sortedValues, value);
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    return Integer.compare(dictId1, dictId2);
  }

  @Override
  public Comparable getMinVal() {
    if (_sortedValues.length == 0) {
      return null;
    }
    return parseValue(_sortedValues[0]);
  }

  @Override
  public Comparable getMaxVal() {
    if (_sortedValues.length == 0) {
      return null;
    }
    return parseValue(_sortedValues[_sortedValues.length - 1]);
  }

  @Override
  public Object getSortedValues() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(int dictId) {
    checkDictId(dictId);
    return parseValue(_sortedValues[dictId]);
  }

  @Override
  public int getIntValue(int dictId) {
    checkDictId(dictId);
    return Integer.parseInt(_sortedValues[dictId]);
  }

  @Override
  public long getLongValue(int dictId) {
    checkDictId(dictId);
    return Long.parseLong(_sortedValues[dictId]);
  }

  @Override
  public float getFloatValue(int dictId) {
    checkDictId(dictId);
    return Float.parseFloat(_sortedValues[dictId]);
  }

  @Override
  public double getDoubleValue(int dictId) {
    checkDictId(dictId);
    return Double.parseDouble(_sortedValues[dictId]);
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    checkDictId(dictId);
    return new BigDecimal(_sortedValues[dictId]);
  }

  @Override
  public String getStringValue(int dictId) {
    checkDictId(dictId);
    return _sortedValues[dictId];
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    checkDictId(dictId);
    return BytesUtils.toBytes(_sortedValues[dictId]);
  }

  private void checkDictId(int dictId) {
    Preconditions.checkArgument(dictId >= 0 && dictId < _sortedValues.length,
        "Invalid dictId: %s, dictionary size: %s", dictId, _sortedValues.length);
  }

  @Override
  public void close()
      throws IOException {
    // no-op: in-memory dictionary
  }

  private Comparable parseValue(String stringValue) {
    switch (_valueType) {
      case INT:
        return Integer.parseInt(stringValue);
      case LONG:
        return Long.parseLong(stringValue);
      case FLOAT:
        return Float.parseFloat(stringValue);
      case DOUBLE:
        return Double.parseDouble(stringValue);
      default:
        return stringValue;
    }
  }

  /**
   * Returns the string representation of the default (null-substitute) value for the given type.
   * INT/LONG default to "0", FLOAT/DOUBLE to "0.0", and STRING/BYTES to "".
   */
  static String getDefaultValueString(DataType dataType) {
    switch (dataType) {
      case INT:
        return "0";
      case LONG:
        return "0";
      case FLOAT:
        return "0.0";
      case DOUBLE:
        return "0.0";
      default:
        return "";
    }
  }

  /**
   * Returns a numeric comparator for the given type, or null for STRING/BYTES (lexicographic).
   */
  static Comparator<String> getComparator(DataType valueType) {
    switch (valueType) {
      case INT:
        return Comparator.comparingInt(Integer::parseInt);
      case LONG:
        return Comparator.comparingLong(Long::parseLong);
      case FLOAT:
        return (a, b) -> Float.compare(Float.parseFloat(a), Float.parseFloat(b));
      case DOUBLE:
        return Comparator.comparingDouble(Double::parseDouble);
      default:
        return null;
    }
  }
}
