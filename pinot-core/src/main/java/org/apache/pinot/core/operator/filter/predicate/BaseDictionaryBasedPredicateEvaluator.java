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
package org.apache.pinot.core.operator.filter.predicate;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.math.BigDecimal;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public abstract class BaseDictionaryBasedPredicateEvaluator extends BasePredicateEvaluator {
  protected final Dictionary _dictionary;
  protected boolean _alwaysTrue;
  protected boolean _alwaysFalse;
  protected int[] _matchingDictIds;
  protected int[] _nonMatchingDictIds;

  protected BaseDictionaryBasedPredicateEvaluator(Predicate predicate, Dictionary dictionary) {
    super(predicate);
    _dictionary = dictionary;
  }

  @Override
  public final boolean isDictionaryBased() {
    return true;
  }

  @Override
  public DataType getDataType() {
    return DataType.INT;
  }

  @Override
  public boolean isAlwaysTrue() {
    return _alwaysTrue;
  }

  @Override
  public boolean isAlwaysFalse() {
    return _alwaysFalse;
  }

  @Override
  public int[] getMatchingDictIds() {
    if (_matchingDictIds == null) {
      _matchingDictIds = calculateMatchingDictIds();
    }
    return _matchingDictIds;
  }

  protected int[] calculateMatchingDictIds() {
    IntList matchingDictIds = new IntArrayList();
    int dictionarySize = _dictionary.length();
    for (int dictId = 0; dictId < dictionarySize; dictId++) {
      if (applySV(dictId)) {
        matchingDictIds.add(dictId);
      }
    }
    return matchingDictIds.toIntArray();
  }

  public int[] getNonMatchingDictIds() {
    if (_nonMatchingDictIds == null) {
      _nonMatchingDictIds = calculateNonMatchingDictIds();
    }
    return _nonMatchingDictIds;
  }

  protected int[] calculateNonMatchingDictIds() {
    return PredicateUtils.flipDictIds(getMatchingDictIds(), _dictionary.length());
  }

  @Override
  public final boolean applySV(long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean applyMV(long[] values, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean applySV(float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean applyMV(float[] values, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean applySV(double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean applyMV(double[] values, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean applySV(BigDecimal value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean applySV(String value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean applyMV(String[] values, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean applySV(byte[] value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final boolean applyMV(byte[][] values, int length) {
    throw new UnsupportedOperationException();
  }

  /**
   * Apply a single-value entry to the predicate.
   *
   * @param dictId Dictionary id
   * @return Whether the entry matches the predicate
   */
  @Override
  public abstract boolean applySV(int dictId);

  /**
   * Apply a multi-value entry to the predicate.
   *
   * @param dictIds Array of dictionary ids
   * @param length Number of dictionary ids in the entry
   * @return Whether the entry matches the predicate
   */
  @SuppressWarnings("Duplicates")
  @Override
  public boolean applyMV(int[] dictIds, int length) {
    if (isExclusive()) {
      for (int i = 0; i < length; i++) {
        if (!applySV(dictIds[i])) {
          return false;
        }
      }
      return true;
    } else {
      for (int i = 0; i < length; i++) {
        if (applySV(dictIds[i])) {
          return true;
        }
      }
      return false;
    }
  }
}
