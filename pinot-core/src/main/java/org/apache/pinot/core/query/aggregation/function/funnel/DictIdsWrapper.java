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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.roaringbitmap.RoaringBitmap;


/**
 * Holds per-step RoaringBitmaps keyed by correlation dictionary IDs.
 * For multi-key correlate-by, composite IDs are assigned via stride-based
 * arithmetic (when the combined key space fits in int) or a HashMap fallback.
 */
final class DictIdsWrapper {
  final Dictionary[] _dictionaries;
  final RoaringBitmap[] _stepsBitmaps;

  // Stride-based composite mapping (non-null when product of dict sizes fits in int)
  private final int[] _strides;

  // HashMap-based composite mapping (non-null when stride overflows int)
  private final Map<IntArrayList, Integer> _compositeKeyMap;
  private final List<int[]> _compositeKeyReverse;
  private final IntArrayList _lookupKey;

  DictIdsWrapper(int numSteps, Dictionary[] dictionaries) {
    _dictionaries = dictionaries;
    _stepsBitmaps = new RoaringBitmap[numSteps];
    for (int n = 0; n < numSteps; n++) {
      _stepsBitmaps[n] = new RoaringBitmap();
    }

    if (dictionaries.length > 1) {
      long totalSpace = 1;
      boolean fitsInInt = true;
      for (Dictionary d : dictionaries) {
        totalSpace *= d.length();
        if (totalSpace > Integer.MAX_VALUE) {
          fitsInInt = false;
          break;
        }
      }

      if (fitsInInt) {
        _strides = new int[dictionaries.length];
        _strides[dictionaries.length - 1] = 1;
        for (int k = dictionaries.length - 2; k >= 0; k--) {
          _strides[k] = _strides[k + 1] * dictionaries[k + 1].length();
        }
        _compositeKeyMap = null;
        _compositeKeyReverse = null;
        _lookupKey = null;
      } else {
        _strides = null;
        _compositeKeyMap = new HashMap<>();
        _compositeKeyReverse = new ArrayList<>();
        _lookupKey = new IntArrayList(dictionaries.length);
      }
    } else {
      _strides = null;
      _compositeKeyMap = null;
      _compositeKeyReverse = null;
      _lookupKey = null;
    }
  }

  boolean isMultiKey() {
    return _dictionaries.length > 1;
  }

  /**
   * Maps a tuple of per-column dictionary IDs to a single composite int suitable for RoaringBitmap.
   * For single-key, returns the dict ID directly.
   */
  int getCorrelationId(int[] dictIds) {
    if (dictIds.length == 1) {
      return dictIds[0];
    }
    if (_strides != null) {
      int id = 0;
      for (int k = 0; k < dictIds.length; k++) {
        id += dictIds[k] * _strides[k];
      }
      return id;
    }
    _lookupKey.clear();
    for (int dictId : dictIds) {
      _lookupKey.add(dictId);
    }
    Integer existingId = _compositeKeyMap.get(_lookupKey);
    if (existingId != null) {
      return existingId;
    }
    IntArrayList insertKey = new IntArrayList(dictIds);
    int id = _compositeKeyReverse.size();
    _compositeKeyMap.put(insertKey, id);
    _compositeKeyReverse.add(dictIds.clone());
    return id;
  }

  /**
   * Builds a collision-free composite string from dictionary values using length-prefix encoding.
   * Each component is encoded as {@code length:value}, e.g. ("alice", "home") becomes "5:alice4:home".
   */
  static String toCompositeString(Dictionary[] dictionaries, int[] dictIds) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < dictionaries.length; k++) {
      String val = dictionaries[k].getStringValue(dictIds[k]);
      sb.append(val.length()).append(':').append(val);
    }
    return sb.toString();
  }

  /**
   * Reverse-maps a composite ID back to per-column dictionary IDs.
   */
  void reverseDictIds(int compositeId, int[] outDictIds) {
    if (outDictIds.length == 1) {
      outDictIds[0] = compositeId;
      return;
    }
    if (_strides != null) {
      int remaining = compositeId;
      for (int k = 0; k < outDictIds.length - 1; k++) {
        outDictIds[k] = remaining / _strides[k];
        remaining %= _strides[k];
      }
      outDictIds[outDictIds.length - 1] = remaining;
      return;
    }
    int[] stored = _compositeKeyReverse.get(compositeId);
    System.arraycopy(stored, 0, outDictIds, 0, outDictIds.length);
  }
}
