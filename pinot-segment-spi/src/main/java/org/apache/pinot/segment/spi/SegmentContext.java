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
package org.apache.pinot.segment.spi;

import javax.annotation.Nullable;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class SegmentContext {
  private final IndexSegment _indexSegment;
  /// Queryable-docs bitmap by default, or valid-docs (tombstones included) when the query set `useValidDocIds`.
  @Nullable
  private MutableRoaringBitmap _docIdsSnapshot = null;

  public SegmentContext(IndexSegment indexSegment) {
    _indexSegment = indexSegment;
  }

  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  /// See {@link #_docIdsSnapshot}.
  @Nullable
  public MutableRoaringBitmap getDocIdsSnapshot() {
    return _docIdsSnapshot;
  }

  /// See {@link #_docIdsSnapshot}.
  public void setDocIdsSnapshot(@Nullable MutableRoaringBitmap docIdsSnapshot) {
    _docIdsSnapshot = docIdsSnapshot;
  }
}
