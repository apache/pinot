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
package org.apache.pinot.segment.local.segment.virtualcolumn;

import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.column.DefaultNullValueVirtualColumnProvider;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// Base class for the built-in virtual columns that expose a piece of the segment metadata (creation time, time range,
/// CRC, etc.) as a constant single-value column.
///
/// The value is read from [SegmentMetadata] every time the column is built rather than baked into the field spec, so
/// that mutable segments - which rebuild their virtual data sources on every access - always observe the current
/// metadata.
///
/// When the metadata is not available at all, or the specific piece of metadata is not set (e.g. the time range of a
/// CONSUMING segment), the column stores the default null value of its data type *and* reports every document as null,
/// so that the placeholder is not mistaken for a real value once null handling is enabled.
public abstract class BaseSegmentMetadataVirtualColumnProvider extends DefaultNullValueVirtualColumnProvider {

  @Override
  protected Object getValue(VirtualColumnContext context) {
    Object value = extractValueOrNull(context);
    return value != null ? value : super.getValue(context);
  }

  @Nullable
  @Override
  public NullValueVectorReader buildNullValueVector(VirtualColumnContext context) {
    return extractValueOrNull(context) != null ? null : new AllNullValueVector(context.getTotalDocCount());
  }

  @Nullable
  private Object extractValueOrNull(VirtualColumnContext context) {
    SegmentMetadata segmentMetadata = context.getSegmentMetadata();
    return segmentMetadata != null ? extractValue(segmentMetadata) : null;
  }

  /// Extracts the value for this column from the given segment metadata, or `null` when it is not available.
  @Nullable
  protected abstract Object extractValue(SegmentMetadata segmentMetadata);

  /// [NullValueVectorReader] where every document is null.
  private static class AllNullValueVector implements NullValueVectorReader {
    private final ImmutableRoaringBitmap _nullBitmap;

    AllNullValueVector(int numDocs) {
      MutableRoaringBitmap nullBitmap = new MutableRoaringBitmap();
      if (numDocs > 0) {
        nullBitmap.add(0L, numDocs);
      }
      _nullBitmap = nullBitmap.toImmutableRoaringBitmap();
    }

    @Override
    public boolean isNull(int docId) {
      return true;
    }

    @Override
    public ImmutableRoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }
  }
}
