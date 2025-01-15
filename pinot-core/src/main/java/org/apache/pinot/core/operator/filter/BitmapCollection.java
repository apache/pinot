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
package org.apache.pinot.core.operator.filter;

import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Encapsulates a collection of bitmaps, and allows inversion without modifying the bitmaps.
 * Provides simplified access to efficient cardinality calculation which work regardless of
 * inversion status without computing the complement of the union of the bitmaps.
 */
public class BitmapCollection {
  private final int _numDocs;
  private boolean _inverted;
  private final ImmutableRoaringBitmap[] _bitmaps;

  public BitmapCollection(int numDocs, boolean inverted, ImmutableRoaringBitmap... bitmaps) {
    _numDocs = numDocs;
    _inverted = inverted;
    _bitmaps = bitmaps;
  }

  /**
   * Inverts the bitmaps in constant time and space.
   * @return this bitmap collection inverted.
   */
  public BitmapCollection invert() {
    _inverted = !_inverted;
    return this;
  }

  /**
   * Computes the size of the intersection of the bitmaps efficiently regardless of negation, without
   * needing to invert inputs or materialize an intermediate bitmap.
   *
   * @param bitmaps to intersect with
   * @return the size of the intersection of the bitmaps in this collection and in the other collection
   */
  public int andCardinality(BitmapCollection bitmaps) {
    ImmutableRoaringBitmap left = reduceInternal();
    ImmutableRoaringBitmap right = bitmaps.reduceInternal();
    if (!_inverted) {
      if (!bitmaps._inverted) {
        return ImmutableRoaringBitmap.andCardinality(left, right);
      }
      return ImmutableRoaringBitmap.andNotCardinality(left, right);
    } else {
      if (!bitmaps._inverted) {
        return ImmutableRoaringBitmap.andNotCardinality(right, left);
      }
      return _numDocs - ImmutableRoaringBitmap.orCardinality(left, right);
    }
  }

  /**
   * Computes the size of the union of the bitmaps efficiently regardless of negation, without
   * needing to invert inputs or materialize an intermediate bitmap. If either this collection
   * or the other collection has more than one bitmap, the union will be materialized.
   *
   * @param bitmaps to intersect with
   * @return the size of the union of the bitmaps in this collection and in the other collection
   */
  public int orCardinality(BitmapCollection bitmaps) {
    ImmutableRoaringBitmap left = reduceInternal();
    ImmutableRoaringBitmap right = bitmaps.reduceInternal();
    if (!_inverted) {
      if (!bitmaps._inverted) {
        return ImmutableRoaringBitmap.orCardinality(left, right);
      }
      return _numDocs - right.getCardinality() + ImmutableRoaringBitmap.andCardinality(left, right);
    } else {
      if (!bitmaps._inverted) {
        return _numDocs - left.getCardinality() + ImmutableRoaringBitmap.andCardinality(right, left);
      }
      return _numDocs - ImmutableRoaringBitmap.andCardinality(left, right);
    }
  }

  private ImmutableRoaringBitmap reduceInternal() {
    if (_bitmaps.length == 1) {
      return _bitmaps[0];
    }
    return BufferFastAggregation.or(_bitmaps);
  }

  /**
   * Reduces the bitmaps to a single bitmap. In common cases, when the collection
   * is not inverted and only has one bitmap, this operation is cheap. However,
   * this may be a costly operation: a new bitmap may be allocated, one or many
   * bitmaps may need to be inverted. Prefer {@see andCardinality} or {@see orCardinality}
   * when appropriate.
   * @return a bitmap
   */
  public ImmutableRoaringBitmap reduce() {
    if (!_inverted) {
      return reduceInternal();
    }
    return invertedOr();
  }

  private MutableRoaringBitmap invertedOr() {
    MutableRoaringBitmap complement = new MutableRoaringBitmap();
    complement.add(0L, _numDocs);
    for (ImmutableRoaringBitmap bitmap : _bitmaps) {
      complement.andNot(bitmap);
    }
    return complement;
  }
}
