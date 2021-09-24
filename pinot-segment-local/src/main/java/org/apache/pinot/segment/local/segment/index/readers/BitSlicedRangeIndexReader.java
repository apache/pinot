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

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitSlicedRangeIndexCreator;
import org.apache.pinot.segment.local.utils.FPOrdering;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class BitSlicedRangeIndexReader implements RangeIndexReader<ImmutableRoaringBitmap> {

  private final PinotDataBuffer _dataBuffer;
  private final long _offset;
  private final long _min;

  public BitSlicedRangeIndexReader(PinotDataBuffer dataBuffer) {
    _dataBuffer = dataBuffer;
    long offset = 0;
    int version = dataBuffer.getInt(offset);
    assert version == BitSlicedRangeIndexCreator.VERSION : "invalid version";
    offset += Integer.BYTES;
    _min = dataBuffer.getLong(offset);
    offset += Long.BYTES;
    _offset = offset;
  }

  @Nullable
  @Override
  public ImmutableRoaringBitmap getMatchingDocIds(int min, int max) {
    return queryRangeBitmap(Math.max(min, _min) - _min, max - _min);
  }

  @Nullable
  @Override
  public ImmutableRoaringBitmap getMatchingDocIds(long min, long max) {
    return queryRangeBitmap(Math.max(min, _min) - _min, max - _min);
  }

  @Nullable
  @Override
  public ImmutableRoaringBitmap getMatchingDocIds(float min, float max) {
    return queryRangeBitmap(FPOrdering.ordinalOf(min), FPOrdering.ordinalOf(max));
  }

  @Nullable
  @Override
  public ImmutableRoaringBitmap getMatchingDocIds(double min, double max) {
    return queryRangeBitmap(FPOrdering.ordinalOf(min), FPOrdering.ordinalOf(max));
  }

  // this index supports exact matches, so always return null for partial matches

  @Nullable
  @Override
  public ImmutableRoaringBitmap getPartiallyMatchingDocIds(int min, int max) {
    return null;
  }

  @Nullable
  @Override
  public ImmutableRoaringBitmap getPartiallyMatchingDocIds(long min, long max) {
    return null;
  }

  @Nullable
  @Override
  public ImmutableRoaringBitmap getPartiallyMatchingDocIds(float min, float max) {
    return null;
  }

  @Nullable
  @Override
  public ImmutableRoaringBitmap getPartiallyMatchingDocIds(double min, double max) {
    return null;
  }

  private ImmutableRoaringBitmap queryRangeBitmap(long min, long max) {
    RangeBitmap rangeBitmap = mapRangeBitmap();
    RoaringBitmap lte = rangeBitmap.lte(max);
    RoaringBitmap gte = rangeBitmap.gte(min);
    lte.and(gte);
    return lte.toMutableRoaringBitmap();
  }

  private RangeBitmap mapRangeBitmap() {
    // note that this is a very cheap operation, no deserialization is required
    ByteBuffer buffer = _dataBuffer.toDirectByteBuffer(_offset, (int) (_dataBuffer.size() - _offset));
    return RangeBitmap.map(buffer);
  }

  @Override
  public void close()
      throws IOException {
  }
}
