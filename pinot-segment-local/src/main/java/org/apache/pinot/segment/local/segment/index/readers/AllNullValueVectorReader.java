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
package org.apache.pinot.segment.local.segment.index.readers;

import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// [NullValueVectorReader] where every document of the column is null.
///
/// Used by columns that carry no value at all: a materialized-but-absent OPEN_STRUCT child, and the segment metadata
/// virtual columns whose metadata is not available yet (e.g. the time range of a CONSUMING segment).
///
/// The bitmap is materialized lazily, because null value vectors are only consulted when null handling is enabled
/// while the reader itself is created on every data source build for a mutable segment.
public class AllNullValueVectorReader implements NullValueVectorReader {
  private final int _numDocs;
  private volatile ImmutableRoaringBitmap _nullBitmap;

  public AllNullValueVectorReader(int numDocs) {
    _numDocs = numDocs;
  }

  @Override
  public boolean isNull(int docId) {
    return true;
  }

  @Override
  public ImmutableRoaringBitmap getNullBitmap() {
    ImmutableRoaringBitmap nullBitmap = _nullBitmap;
    if (nullBitmap == null) {
      // Benign race: concurrent callers may each build an equivalent bitmap, and publication is safe because the
      // volatile write happens-after the bitmap is fully built. NOTE: toImmutableRoaringBitmap() returns `this`, so
      // the returned instance is shared and must be treated as read-only by callers.
      MutableRoaringBitmap bitmap = _numDocs > 0 ? MutableRoaringBitmap.bitmapOfRange(0, _numDocs)
          : new MutableRoaringBitmap();
      nullBitmap = bitmap.toImmutableRoaringBitmap();
      _nullBitmap = nullBitmap;
    }
    return nullBitmap;
  }
}
