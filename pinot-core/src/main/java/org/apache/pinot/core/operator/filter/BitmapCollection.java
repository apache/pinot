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

import javax.annotation.Nullable;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// Encapsulates a collection of bitmaps, and allows inversion without modifying the bitmaps.
/// Provides simplified access to efficient cardinality calculation which work regardless of
/// inversion status without computing the complement of the union of the bitmaps.
///
/// The collection is two-valued by default: the true documents are the union of the bitmaps, or its complement when
/// inverted, and every other document is false. [#excludingNulls] attaches the documents the predicate is UNKNOWN
/// for, which are then neither true nor false: they are left out of the true documents whether or not they fall in
/// the bitmaps, and they stay UNKNOWN under inversion, since NOT of UNKNOWN is UNKNOWN. Every cardinality and
/// reduction honors the null bitmap.
public class BitmapCollection {
  private final int _numDocs;
  private boolean _inverted;
  private final ImmutableRoaringBitmap[] _bitmaps;
  @Nullable
  private final ImmutableRoaringBitmap _nullBitmap;

  public BitmapCollection(int numDocs, boolean inverted, ImmutableRoaringBitmap... bitmaps) {
    this(numDocs, inverted, bitmaps, null);
  }

  private BitmapCollection(int numDocs, boolean inverted, ImmutableRoaringBitmap[] bitmaps,
      @Nullable ImmutableRoaringBitmap nullBitmap) {
    _numDocs = numDocs;
    _inverted = inverted;
    _bitmaps = bitmaps;
    _nullBitmap = nullBitmap;
  }

  /// Returns a collection over the same bitmaps that treats the documents in `nullBitmap` as UNKNOWN, or this
  /// collection when `nullBitmap` is `null` or empty.
  public BitmapCollection excludingNulls(@Nullable ImmutableRoaringBitmap nullBitmap) {
    if (nullBitmap == null || nullBitmap.isEmpty()) {
      return this;
    }
    return new BitmapCollection(_numDocs, _inverted, _bitmaps, nullBitmap);
  }

  /// Returns the documents the predicate is UNKNOWN for, or `null` when there is none.
  @Nullable
  public ImmutableRoaringBitmap getNullBitmap() {
    return _nullBitmap;
  }

  /// Inverts the bitmaps in constant time and space. The null bitmap is kept: NOT of UNKNOWN is UNKNOWN.
  /// @return this bitmap collection inverted.
  public BitmapCollection invert() {
    _inverted = !_inverted;
    return this;
  }

  /// Returns the number of true documents.
  public int getCardinality() {
    return getCardinality(reduceInternal());
  }

  private int getCardinality(ImmutableRoaringBitmap union) {
    if (_nullBitmap == null) {
      return _inverted ? _numDocs - union.getCardinality() : union.getCardinality();
    }
    if (_inverted) {
      return _numDocs - ImmutableRoaringBitmap.orCardinality(union, _nullBitmap);
    }
    return union.getCardinality() - ImmutableRoaringBitmap.andCardinality(union, _nullBitmap);
  }

  /// Computes the size of the intersection of the bitmaps efficiently regardless of negation, without
  /// needing to invert inputs or materialize an intermediate bitmap. The UNKNOWN documents of either collection are
  /// taken out through bitmaps no larger than the null bitmaps themselves.
  ///
  /// @param bitmaps to intersect with
  /// @return the size of the intersection of the bitmaps in this collection and in the other collection
  public int andCardinality(BitmapCollection bitmaps) {
    return andCardinality(reduceInternal(), bitmaps.reduceInternal(), bitmaps);
  }

  private int andCardinality(ImmutableRoaringBitmap left, ImmutableRoaringBitmap right, BitmapCollection bitmaps) {
    int cardinality;
    if (!_inverted) {
      if (!bitmaps._inverted) {
        cardinality = ImmutableRoaringBitmap.andCardinality(left, right);
      } else {
        cardinality = ImmutableRoaringBitmap.andNotCardinality(left, right);
      }
    } else {
      if (!bitmaps._inverted) {
        cardinality = ImmutableRoaringBitmap.andNotCardinality(right, left);
      } else {
        cardinality = _numDocs - ImmutableRoaringBitmap.orCardinality(left, right);
      }
    }
    ImmutableRoaringBitmap nulls = unionOfNulls(bitmaps);
    if (nulls != null) {
      // A document UNKNOWN to either side is not true on that side, so it leaves the intersection
      cardinality -= andCardinalityWithin(nulls, left, _inverted, right, bitmaps._inverted);
    }
    return cardinality;
  }

  /// Returns the size of the intersection of the two unions, each complemented when inverted, restricted to
  /// `within`. Only bitmaps no larger than `within` are materialized.
  private static int andCardinalityWithin(ImmutableRoaringBitmap within, ImmutableRoaringBitmap left,
      boolean leftInverted, ImmutableRoaringBitmap right, boolean rightInverted) {
    if (!leftInverted) {
      MutableRoaringBitmap leftWithin = ImmutableRoaringBitmap.and(left, within);
      return rightInverted
          ? ImmutableRoaringBitmap.andNotCardinality(leftWithin, right)
          : ImmutableRoaringBitmap.andCardinality(leftWithin, right);
    }
    if (!rightInverted) {
      return ImmutableRoaringBitmap.andNotCardinality(ImmutableRoaringBitmap.and(right, within), left);
    }
    // Both complemented: what is left of `within` once the documents of either union are removed
    return within.getCardinality() - ImmutableRoaringBitmap.orCardinality(ImmutableRoaringBitmap.and(left, within),
        ImmutableRoaringBitmap.and(right, within));
  }

  /// Returns the documents UNKNOWN to either collection, or `null` when there is none.
  @Nullable
  private ImmutableRoaringBitmap unionOfNulls(BitmapCollection bitmaps) {
    if (_nullBitmap == null) {
      return bitmaps._nullBitmap;
    }
    if (bitmaps._nullBitmap == null) {
      return _nullBitmap;
    }
    return ImmutableRoaringBitmap.or(_nullBitmap, bitmaps._nullBitmap);
  }

  /// Computes the size of the union of the bitmaps efficiently regardless of negation, without
  /// needing to invert inputs or materialize an intermediate bitmap. If either this collection
  /// or the other collection has more than one bitmap, the union will be materialized. When either collection has a
  /// null bitmap, the size follows from the two cardinalities and the intersection, so nothing larger than the null
  /// bitmaps is materialized.
  ///
  /// @param bitmaps to intersect with
  /// @return the size of the union of the bitmaps in this collection and in the other collection
  public int orCardinality(BitmapCollection bitmaps) {
    ImmutableRoaringBitmap left = reduceInternal();
    ImmutableRoaringBitmap right = bitmaps.reduceInternal();
    if (_nullBitmap != null || bitmaps._nullBitmap != null) {
      return getCardinality(left) + bitmaps.getCardinality(right) - andCardinality(left, right, bitmaps);
    }
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

  /// Reduces the bitmaps to a single bitmap of the true documents. In common cases, when the collection
  /// is not inverted, only has one bitmap and no null bitmap, this operation is cheap. However,
  /// this may be a costly operation: a new bitmap may be allocated, one or many
  /// bitmaps may need to be inverted, and the null bitmap subtracted. Prefer [#andCardinality] or [#orCardinality]
  /// when appropriate.
  /// @return a bitmap
  public ImmutableRoaringBitmap reduce() {
    if (_nullBitmap == null) {
      return _inverted ? invertedOr() : reduceInternal();
    }
    if (_inverted) {
      MutableRoaringBitmap complement = invertedOr();
      complement.andNot(_nullBitmap);
      return complement;
    }
    return ImmutableRoaringBitmap.andNot(reduceInternal(), _nullBitmap);
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
