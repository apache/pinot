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
package org.apache.pinot.segment.local.segment.index.openstruct;

import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// {@link NullValueVectorReader} backed by an OPEN_STRUCT key's presence bitmap.
///
/// A document is null for a key when the key was never set on that document, i.e. when the
/// document is absent from the presence bitmap.
///
/// Reads the live presence bitmap rather than a snapshot: the bitmap is a
/// {@link ThreadSafeMutableRoaringBitmap} and ingestion only ever appends docIds >= the numDocs
/// captured by the enclosing DataSource, so the [0, numDocs) range this vector reports on is
/// already frozen. That also makes the null bitmap invariant for this object's lifetime, so it is
/// computed once and memoized — recomputing per block would clone the whole presence bitmap under
/// the wrapper's monitor and stall the ingestion thread each time.
///
/// As with every other {@link NullValueVectorReader}, the returned bitmap is shared; callers must
/// not mutate it.
public class PresenceBasedNullValueVector implements NullValueVectorReader {
  private final ThreadSafeMutableRoaringBitmap _presenceBitmap;
  private final int _numDocs;
  /// Benign race: concurrent computations observe the same frozen [0, numDocs) range and agree.
  private volatile ImmutableRoaringBitmap _nullBitmap;

  public PresenceBasedNullValueVector(ThreadSafeMutableRoaringBitmap presenceBitmap, int numDocs) {
    _presenceBitmap = presenceBitmap;
    _numDocs = numDocs;
  }

  @Override
  public boolean isNull(int docId) {
    return !_presenceBitmap.contains(docId);
  }

  @Override
  public ImmutableRoaringBitmap getNullBitmap() {
    ImmutableRoaringBitmap nullBitmap = _nullBitmap;
    if (nullBitmap == null) {
      MutableRoaringBitmap computed = new MutableRoaringBitmap();
      if (_numDocs > 0) {
        computed.add(0L, _numDocs);
      }
      // getMutableRoaringBitmap() clones under the wrapper's monitor, so the andNot below iterates a
      // private copy that the ingestion thread cannot mutate mid-walk.
      computed.andNot(_presenceBitmap.getMutableRoaringBitmap());
      nullBitmap = computed;
      _nullBitmap = nullBitmap;
    }
    return nullBitmap;
  }
}
