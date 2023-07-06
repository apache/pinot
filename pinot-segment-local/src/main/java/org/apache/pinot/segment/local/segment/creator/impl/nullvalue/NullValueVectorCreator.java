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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;


/**
 * Used to persist the null bitmap on disk. This is used by SegmentCreator while indexing rows.
 *
 * Although this class implements {@link IndexCreator}, it is not intended to be used as a normal IndexCreator.
 * Specifically, neither {@link #add(Object, int)} or {@link #add(Object[], int[])} should be called on this object.
 * In order to make sure these methods are not being called, they throw exceptions in this class.
 *
 * This requirement is a corollary from the fact that the {@link IndexCreator} contract assumes the value will never be
 * null, which is true for all index creators types unless this one.
 */
public class NullValueVectorCreator implements IndexCreator {
  private final RoaringBitmapWriter<RoaringBitmap> _bitmapWriter;
  private final File _nullValueVectorFile;

  @Override
  public void add(@Nonnull Object value, int dictId)
      throws IOException {
    throw new UnsupportedOperationException("NullValueVector should not be built as a normal index");
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds)
      throws IOException {
    throw new UnsupportedOperationException("NullValueVector should not be built as a normal index");
  }

  public NullValueVectorCreator(File indexDir, String columnName) {
    _bitmapWriter = RoaringBitmapWriter.writer().get();
    _nullValueVectorFile = new File(indexDir, columnName + V1Constants.Indexes.NULLVALUE_VECTOR_FILE_EXTENSION);
  }

  public void setNull(int docId) {
    _bitmapWriter.add(docId);
  }

  public void seal()
      throws IOException {
    // Create null value vector file only if the bitmap is not empty
    RoaringBitmap nullBitmap = _bitmapWriter.get();
    if (!nullBitmap.isEmpty()) {
      try (DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(_nullValueVectorFile))) {
        nullBitmap.serialize(outputStream);
      }
    }
  }

  @VisibleForTesting
  RoaringBitmap getNullBitmap() {
    return _bitmapWriter.get();
  }

  @Override
  public void close() {
  }
}
