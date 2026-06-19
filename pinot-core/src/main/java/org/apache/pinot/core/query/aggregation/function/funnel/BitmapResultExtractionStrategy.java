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
package org.apache.pinot.core.query.aggregation.function.funnel;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * Extracts intermediate bitmap results for cross-segment merging.
 *
 * <p>The bitmap strategy stores entities as 32-bit hash codes in a {@link RoaringBitmap}. For single-key INT
 * columns, the actual int values are stored directly (exact). For other single-key types and all multi-key
 * composites, hash codes are used (approximate — hash collisions can cause under-counting).
 */
class BitmapResultExtractionStrategy implements ResultExtractionStrategy<DictIdsWrapper, List<RoaringBitmap>> {
  protected final int _numSteps;

  BitmapResultExtractionStrategy(int numSteps) {
    _numSteps = numSteps;
  }

  @Override
  public List<RoaringBitmap> extractIntermediateResult(DictIdsWrapper dictIdsWrapper) {
    if (dictIdsWrapper == null) {
      List<RoaringBitmap> result = new ArrayList<>(_numSteps);
      for (int i = 0; i < _numSteps; i++) {
        result.add(new RoaringBitmap());
      }
      return result;
    }
    List<RoaringBitmap> result = new ArrayList<>(_numSteps);
    if (dictIdsWrapper.isMultiKey()) {
      for (RoaringBitmap compositeIdBitmap : dictIdsWrapper._stepsBitmaps) {
        result.add(convertCompositeToValueBitmap(dictIdsWrapper, compositeIdBitmap));
      }
    } else {
      Dictionary dictionary = dictIdsWrapper._dictionaries[0];
      for (RoaringBitmap dictIdBitmap : dictIdsWrapper._stepsBitmaps) {
        result.add(convertToValueBitmap(dictionary, dictIdBitmap));
      }
    }
    return result;
  }

  /// Converts segment-local composite dictionary IDs to hash-coded value bitmaps for cross-segment merging.
  /// Uses {@code .hashCode()} on the length-prefix-encoded composite string — same approximation as the
  /// single-key non-INT path in {@link #convertToValueBitmap}, so hash collisions may cause under-counting.
  private RoaringBitmap convertCompositeToValueBitmap(DictIdsWrapper wrapper, RoaringBitmap compositeIdBitmap) {
    RoaringBitmap valueBitmap = new RoaringBitmap();
    PeekableIntIterator iterator = compositeIdBitmap.getIntIterator();
    int numKeys = wrapper._dictionaries.length;
    int[] dictIds = new int[numKeys];
    while (iterator.hasNext()) {
      wrapper.reverseCompositeId(iterator.next(), dictIds);
      valueBitmap.add(DictIdsWrapper.toCompositeString(wrapper._dictionaries, dictIds).hashCode());
    }
    return valueBitmap;
  }

  /**
   * Helper method to read dictionary and convert dictionary ids to hash code of the values for dictionary-encoded
   * expression.
   */
  private RoaringBitmap convertToValueBitmap(Dictionary dictionary, RoaringBitmap dictIdBitmap) {
    RoaringBitmap valueBitmap = new RoaringBitmap();
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    FieldSpec.DataType storedType = dictionary.getValueType();
    switch (storedType) {
      case INT:
        while (iterator.hasNext()) {
          valueBitmap.add(dictionary.getIntValue(iterator.next()));
        }
        break;
      case LONG:
        while (iterator.hasNext()) {
          valueBitmap.add(Long.hashCode(dictionary.getLongValue(iterator.next())));
        }
        break;
      case FLOAT:
        while (iterator.hasNext()) {
          valueBitmap.add(Float.hashCode(dictionary.getFloatValue(iterator.next())));
        }
        break;
      case DOUBLE:
        while (iterator.hasNext()) {
          valueBitmap.add(Double.hashCode(dictionary.getDoubleValue(iterator.next())));
        }
        break;
      case STRING:
        while (iterator.hasNext()) {
          valueBitmap.add(dictionary.getStringValue(iterator.next()).hashCode());
        }
        break;
      default:
        throw new IllegalArgumentException("Illegal data type for FUNNEL_COUNT aggregation function: " + storedType);
    }
    return valueBitmap;
  }
}
