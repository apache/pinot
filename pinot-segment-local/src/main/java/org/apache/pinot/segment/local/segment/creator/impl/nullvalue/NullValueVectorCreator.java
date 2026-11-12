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
package org.apache.pinot.segment.local.segment.creator.impl.nullvalue;

import com.google.common.annotations.VisibleForTesting;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;


/// Used to persist the null bitmap on disk. This is used by SegmentCreator while indexing rows.
///
/// Although this class implements [IndexCreator], it is not intended to be used as a normal IndexCreator.
/// Specifically, neither [#add(Object, int)] or [#add(Object[], int[])] should be called on this object.
/// In order to make sure these methods are not being called, they throw exceptions in this class.
///
/// This requirement is a corollary from the fact that the [IndexCreator] contract assumes the value will never be
/// null, which is true for all index creators types unless this one.
public class NullValueVectorCreator implements IndexCreator {
  private final RoaringBitmapWriter<RoaringBitmap> _bitmapWriter;
  private final File _nullValueVectorFile;
  private boolean _hasNulls;
  // Materialized from the writer on first access; see getNullBitmap() for the contract
  private RoaringBitmap _nullBitmap;

  @Override
  public void add(Object value, int dictId)
      throws IOException {
    throw new UnsupportedOperationException("NullValueVector should not be built as a normal index");
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds)
      throws IOException {
    throw new UnsupportedOperationException("NullValueVector should not be built as a normal index");
  }

  public NullValueVectorCreator(File indexDir, String columnName) {
    _bitmapWriter = RoaringBitmapWriter.writer().get();
    _nullValueVectorFile = new File(indexDir, columnName + V1Constants.Indexes.NULLVALUE_VECTOR_FILE_EXTENSION);
  }

  public void setNull(int docId) {
    // Enforces the contract documented on getNullBitmap(). Kept as an assert so the check is free in production while
    // still catching a misordered caller in tests, where assertions are enabled.
    assert _nullBitmap == null : "setNull() called after the null bitmap was materialized";
    _bitmapWriter.add(docId);
    _hasNulls = true;
  }

  /// Returns `true` when no doc has been marked null, i.e. [#seal] writes no bitmap file.
  public boolean isNonNull() {
    return !_hasNulls;
  }

  /// Returns the number of docs marked null. Subject to the same contract as [#getNullBitmap].
  public int getNumNulls() {
    return _hasNulls ? getNullBitmap().getCardinality() : 0;
  }

  public void seal()
      throws IOException {
    // Create null value vector file only if at least one doc was marked null
    if (_hasNulls) {
      try (DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(_nullValueVectorFile))) {
        getNullBitmap().serialize(outputStream);
      }
    }
  }

  /// Returns the bitmap of null doc ids.
  ///
  /// Must be called only once every [#setNull] call has been made: materializing the bitmap flushes the writer, and the
  /// result is cached here so that repeated calls (e.g. [#getNumNulls] followed by [#seal]) flush at most once. Doc ids
  /// marked after the first call are therefore not guaranteed to be reflected.
  ///
  /// No explicit `runOptimize` is needed: the writer run-length encodes each container as it is appended
  /// (`runCompress` defaults to `true`), which is what keeps a clustered or all-null vector compact.
  @VisibleForTesting
  RoaringBitmap getNullBitmap() {
    if (_nullBitmap == null) {
      _nullBitmap = _bitmapWriter.get();
    }
    return _nullBitmap;
  }

  @Override
  public void close() {
  }
}
