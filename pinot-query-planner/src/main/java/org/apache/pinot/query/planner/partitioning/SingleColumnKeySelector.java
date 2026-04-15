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
package org.apache.pinot.query.planner.partitioning;

import javax.annotation.Nullable;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.pinot.common.datablock.ArrowDataBlock;


public class SingleColumnKeySelector implements KeySelector<Object> {
  private final int _keyId;
  private final String _hashFunction;
  private final HashFunctionSelector.SvHasher _hasher;

  public SingleColumnKeySelector(int keyId) {
    this(keyId, KeySelector.DEFAULT_HASH_ALGORITHM);
  }

  public SingleColumnKeySelector(int keyId, String hashFunction) {
    _keyId = keyId;
    _hashFunction = hashFunction.toLowerCase();
    _hasher = HashFunctionSelector.getSvHasher(_hashFunction);
  }

  @Nullable
  @Override
  public Object getKey(Object[] row) {
    return row[_keyId];
  }

  @Override
  public int computeHash(Object[] input) {
    Object key = input[_keyId];
    return _hasher.hash(key);
  }

  @Override
  public String hashAlgorithm() {
    return _hashFunction;
  }

  @Override
  public ArrowKeyHasher getArrowHasher(ArrowDataBlock arrowDataBlock) {
    return getArrowHasher(arrowDataBlock, _keyId);
  }

  /** Builds a type-specific hasher for a single column in the given block. */
  public static ArrowKeyHasher getArrowHasher(ArrowDataBlock arrowDataBlock, int columnId) {
    FieldVector vector = arrowDataBlock.getRoot().getVector(columnId);
    DictionaryEncoding encoding = vector.getField().getDictionary();
    if (encoding != null && arrowDataBlock.getDictionaryProvider() != null) {
      FieldVector dictVector = arrowDataBlock.getDictionaryProvider().lookup(encoding.getId()).getVector();
      return new DictionaryKeyHasher(dictVector, (IntVector) vector);
    }
    if (vector instanceof IntVector) {
      return new IntKeyHasher((IntVector) vector);
    }
    if (vector instanceof BigIntVector) {
      return new LongKeyHasher((BigIntVector) vector);
    }
    return new GenericKeyHasher(vector);
  }

  @Override
  public ArrowKeyComparator getArrowKeyComparator(ArrowDataBlock left, ArrowDataBlock right, KeySelector<?> other) {
    int rightKeyId = ((SingleColumnKeySelector) other)._keyId;
    return getArrowKeyComparator(left, right, _keyId, rightKeyId);
  }

  /** Builds a type-specific comparator for a single column pair across two blocks. */
  public static ArrowKeyComparator getArrowKeyComparator(ArrowDataBlock left, ArrowDataBlock right,
      int leftColumnId, int rightColumnId) {
    FieldVector leftVector = left.getRoot().getVector(leftColumnId);
    FieldVector rightVector = right.getRoot().getVector(rightColumnId);
    DictionaryEncoding leftEnc = leftVector.getField().getDictionary();
    DictionaryEncoding rightEnc = rightVector.getField().getDictionary();

    if (leftEnc != null && rightEnc != null && left.getDictionaryProvider() != null
        && right.getDictionaryProvider() != null) {
      return new BothDictionaryKeyComparator(
          left.getDictionaryProvider().lookup(leftEnc.getId()).getVector(), (IntVector) leftVector,
          right.getDictionaryProvider().lookup(rightEnc.getId()).getVector(), (IntVector) rightVector);
    }
    if (leftEnc != null && left.getDictionaryProvider() != null) {
      return new LeftDictionaryKeyComparator(
          left.getDictionaryProvider().lookup(leftEnc.getId()).getVector(), (IntVector) leftVector, rightVector);
    }
    if (leftVector instanceof IntVector && rightVector instanceof IntVector) {
      return new IntKeyComparator((IntVector) leftVector, (IntVector) rightVector);
    }
    if (leftVector instanceof BigIntVector && rightVector instanceof BigIntVector) {
      return new LongKeyComparator((BigIntVector) leftVector, (BigIntVector) rightVector);
    }
    return new GenericKeyComparator(leftVector, rightVector);
  }

  // ----- hasher implementations -----

  public static class IntKeyHasher implements ArrowKeyHasher {
    private final IntVector _vector;

    public IntKeyHasher(IntVector vector) {
      _vector = vector;
    }

    @Override
    public int computeHash(int rowIdx) {
      return _vector.isNull(rowIdx) ? 0 : _vector.get(rowIdx);
    }
  }

  public static class LongKeyHasher implements ArrowKeyHasher {
    private final BigIntVector _vector;

    public LongKeyHasher(BigIntVector vector) {
      _vector = vector;
    }

    @Override
    public int computeHash(int rowIdx) {
      return _vector.isNull(rowIdx) ? 0 : Long.hashCode(_vector.get(rowIdx));
    }
  }

  public static class DictionaryKeyHasher implements ArrowKeyHasher {
    private final FieldVector _dictVector;
    private final IntVector _indexVector;

    public DictionaryKeyHasher(FieldVector dictVector, IntVector indexVector) {
      _dictVector = dictVector;
      _indexVector = indexVector;
    }

    @Override
    public int computeHash(int rowIdx) {
      if (_indexVector.isNull(rowIdx)) {
        return 0;
      }
      return _dictVector.getObject(_indexVector.get(rowIdx)).hashCode();
    }
  }

  public static class GenericKeyHasher implements ArrowKeyHasher {
    private final ValueVector _vector;

    public GenericKeyHasher(ValueVector vector) {
      _vector = vector;
    }

    @Override
    public int computeHash(int rowIdx) {
      Object key = _vector.getObject(rowIdx);
      return key != null ? key.hashCode() : 0;
    }
  }

  // ----- comparator implementations -----

  public static class IntKeyComparator implements ArrowKeyComparator {
    private final IntVector _left;
    private final IntVector _right;

    public IntKeyComparator(IntVector left, IntVector right) {
      _left = left;
      _right = right;
    }

    @Override
    public boolean equals(int leftIdx, int rightIdx) {
      return !_left.isNull(leftIdx) && !_right.isNull(rightIdx) && _left.get(leftIdx) == _right.get(rightIdx);
    }
  }

  public static class LongKeyComparator implements ArrowKeyComparator {
    private final BigIntVector _left;
    private final BigIntVector _right;

    public LongKeyComparator(BigIntVector left, BigIntVector right) {
      _left = left;
      _right = right;
    }

    @Override
    public boolean equals(int leftIdx, int rightIdx) {
      return !_left.isNull(leftIdx) && !_right.isNull(rightIdx) && _left.get(leftIdx) == _right.get(rightIdx);
    }
  }

  public static class BothDictionaryKeyComparator implements ArrowKeyComparator {
    private final FieldVector _leftDict;
    private final IntVector _leftIdx;
    private final FieldVector _rightDict;
    private final IntVector _rightIdx;

    public BothDictionaryKeyComparator(FieldVector leftDict, IntVector leftIdx, FieldVector rightDict,
        IntVector rightIdx) {
      _leftDict = leftDict;
      _leftIdx = leftIdx;
      _rightDict = rightDict;
      _rightIdx = rightIdx;
    }

    @Override
    public boolean equals(int leftRow, int rightRow) {
      if (_leftIdx.isNull(leftRow) || _rightIdx.isNull(rightRow)) {
        return false;
      }
      return _leftDict.getObject(_leftIdx.get(leftRow)).equals(_rightDict.getObject(_rightIdx.get(rightRow)));
    }
  }

  public static class LeftDictionaryKeyComparator implements ArrowKeyComparator {
    private final FieldVector _leftDict;
    private final IntVector _leftIdx;
    private final FieldVector _rightVector;

    public LeftDictionaryKeyComparator(FieldVector leftDict, IntVector leftIdx, FieldVector rightVector) {
      _leftDict = leftDict;
      _leftIdx = leftIdx;
      _rightVector = rightVector;
    }

    @Override
    public boolean equals(int leftRow, int rightRow) {
      if (_leftIdx.isNull(leftRow) || _rightVector.isNull(rightRow)) {
        return false;
      }
      return _leftDict.getObject(_leftIdx.get(leftRow)).equals(_rightVector.getObject(rightRow));
    }
  }

  public static class GenericKeyComparator implements ArrowKeyComparator {
    private final ValueVector _left;
    private final ValueVector _right;

    public GenericKeyComparator(ValueVector left, ValueVector right) {
      _left = left;
      _right = right;
    }

    @Override
    public boolean equals(int leftIdx, int rightIdx) {
      if (_left.isNull(leftIdx) || _right.isNull(rightIdx)) {
        return false;
      }
      return _left.getObject(leftIdx).equals(_right.getObject(rightIdx));
    }
  }
}
