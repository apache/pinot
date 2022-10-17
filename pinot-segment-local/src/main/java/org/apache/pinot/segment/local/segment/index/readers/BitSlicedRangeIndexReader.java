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
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class BitSlicedRangeIndexReader implements RangeIndexReader<ImmutableRoaringBitmap> {

  private final PinotDataBuffer _dataBuffer;
  private final long _offset;
  private final long _min;
  private final long _max;
  private final int _numDocs;

  public BitSlicedRangeIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata) {
    _dataBuffer = dataBuffer;
    long offset = 0;
    int version = dataBuffer.getInt(offset);
    assert version == BitSlicedRangeIndexCreator.VERSION : "invalid version";
    offset += Integer.BYTES;
    _min = dataBuffer.getLong(offset);
    offset += Long.BYTES;
    _offset = offset;
    // TODO: Read max from header to prevent cases where max value is not available in the column metadata
    if (metadata.hasDictionary()) {
      _max = metadata.getCardinality() - 1;
    } else {
      Number maxValue = (Number) metadata.getMaxValue();
      _max = maxValue != null ? maxValue.longValue() : Long.MAX_VALUE;
    }
    _numDocs = metadata.getTotalDocs();
  }

  @Override
  public int getNumMatchingDocs(int min, int max) {
    // TODO: Handle this before reading the range index
    if (min > max || min > _max || max < _min) {
      return 0;
    }
    return queryRangeBitmapCardinality(Math.max(min, _min) - _min, max - _min, _max - _min);
  }

  @Override
  public int getNumMatchingDocs(long min, long max) {
    // TODO: Handle this before reading the range index
    if (min > max || min > _max || max < _min) {
      return 0;
    }
    return queryRangeBitmapCardinality(Math.max(min, _min) - _min, max - _min, _max - _min);
  }

  @Override
  public int getNumMatchingDocs(float min, float max) {
    // TODO: Handle this before reading the range index
    if (min > max) {
      return 0;
    }
    return queryRangeBitmapCardinality(FPOrdering.ordinalOf(min), FPOrdering.ordinalOf(max), 0xFFFFFFFFL);
  }

  @Override
  public int getNumMatchingDocs(double min, double max) {
    // TODO: Handle this before reading the range index
    if (min > max) {
      return 0;
    }
    return queryRangeBitmapCardinality(FPOrdering.ordinalOf(min), FPOrdering.ordinalOf(max), 0xFFFFFFFFFFFFFFFFL);
  }

  @Override
  public ImmutableRoaringBitmap getMatchingDocIds(int min, int max) {
    // TODO: Handle this before reading the range index
    if (min > max || min > _max || max < _min) {
      return new MutableRoaringBitmap();
    }
    return queryRangeBitmap(Math.max(min, _min) - _min, max - _min, _max - _min);
  }

  @Override
  public ImmutableRoaringBitmap getMatchingDocIds(long min, long max) {
    // TODO: Handle this before reading the range index
    if (min > max || min > _max || max < _min) {
      return new MutableRoaringBitmap();
    }
    return queryRangeBitmap(Math.max(min, _min) - _min, max - _min, _max - _min);
  }

  @Override
  public ImmutableRoaringBitmap getMatchingDocIds(float min, float max) {
    // TODO: Handle this before reading the range index
    if (min > max) {
      return new MutableRoaringBitmap();
    }
    return queryRangeBitmap(FPOrdering.ordinalOf(min), FPOrdering.ordinalOf(max), 0xFFFFFFFFL);
  }

  @Override
  public ImmutableRoaringBitmap getMatchingDocIds(double min, double max) {
    // TODO: Handle this before reading the range index
    if (min > max) {
      return new MutableRoaringBitmap();
    }
    return queryRangeBitmap(FPOrdering.ordinalOf(min), FPOrdering.ordinalOf(max), 0xFFFFFFFFFFFFFFFFL);
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

  private ImmutableRoaringBitmap queryRangeBitmap(long min, long max, long columnMax) {
    RangeBitmap rangeBitmap = mapRangeBitmap();
    if (Long.compareUnsigned(max, columnMax) < 0) {
      if (Long.compareUnsigned(min, 0) > 0) {
        // TODO: RangeBitmap has a bug in version 0.9.28 which gives wrong result computing between for 2 numbers with
        //       different sign. The bug is tracked here: https://github.com/RoaringBitmap/RoaringBitmap/issues/586.
        //       This is a work-around for the bug.
        if (columnMax > 0) {
          return rangeBitmap.between(min, max).toMutableRoaringBitmap();
        } else {
          return rangeBitmap.gte(min, rangeBitmap.lte(max)).toMutableRoaringBitmap();
        }
      }
      return rangeBitmap.lte(max).toMutableRoaringBitmap();
    } else {
      if (Long.compareUnsigned(min, 0) > 0) {
        return rangeBitmap.gte(min).toMutableRoaringBitmap();
      }
      MutableRoaringBitmap all = new MutableRoaringBitmap();
      all.add(0, _numDocs);
      return all;
    }
  }

  private int queryRangeBitmapCardinality(long min, long max, long columnMax) {
    RangeBitmap rangeBitmap = mapRangeBitmap();
    if (Long.compareUnsigned(max, columnMax) < 0) {
      if (Long.compareUnsigned(min, 0) > 0) {
        // TODO: RangeBitmap has a bug in version 0.9.28 which gives wrong result computing between for 2 numbers with
        //       different sign. The bug is tracked here: https://github.com/RoaringBitmap/RoaringBitmap/issues/586.
        //       This is a work-around for the bug.
        if (columnMax > 0) {
          return (int) rangeBitmap.betweenCardinality(min, max);
        } else {
          return (int) rangeBitmap.gteCardinality(min, rangeBitmap.lte(max));
        }
      }
      return (int) rangeBitmap.lteCardinality(max);
    } else {
      if (Long.compareUnsigned(min, 0) > 0) {
        return (int) rangeBitmap.gteCardinality(min);
      }
      return _numDocs;
    }
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
