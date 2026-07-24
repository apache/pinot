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

import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// {@link NullValueVectorReader} backed by an OPEN_STRUCT key's presence bitmap.
///
/// A document is null for a key when the key was never set on that document, i.e. when the
/// document is absent from the presence bitmap. The null bitmap is computed on demand (not cached)
/// because the presence bitmap is mutable during real-time consumption.
public class PresenceBasedNullValueVector implements NullValueVectorReader {
  private final ImmutableRoaringBitmap _presenceBitmap;
  private final int _numDocs;

  public PresenceBasedNullValueVector(ImmutableRoaringBitmap presenceBitmap, int numDocs) {
    _presenceBitmap = presenceBitmap;
    _numDocs = numDocs;
  }

  @Override
  public boolean isNull(int docId) {
    return !_presenceBitmap.contains(docId);
  }

  @Override
  public ImmutableRoaringBitmap getNullBitmap() {
    MutableRoaringBitmap nullBitmap = new MutableRoaringBitmap();
    if (_numDocs > 0) {
      nullBitmap.add(0L, _numDocs);
    }
    // Clone the presence bitmap before the full-bitmap iteration — the ingestion thread may
    // concurrently mutate the live bitmap via MutableKeyColumn.setValue().
    nullBitmap.andNot(_presenceBitmap.clone());
    return nullBitmap;
  }
}
