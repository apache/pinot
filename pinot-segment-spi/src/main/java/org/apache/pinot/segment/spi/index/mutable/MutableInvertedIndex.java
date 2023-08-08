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
package org.apache.pinot.segment.spi.index.mutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public interface MutableInvertedIndex extends InvertedIndexReader<MutableRoaringBitmap>, MutableIndex {
  @Override
  default void add(@Nonnull Object value, int dictId, int docId) {
    add(dictId, docId);
  }

  @Override
  default void add(@Nonnull Object[] values, @Nullable int[] dictIds, int docId) {
    for (int dictId : dictIds) {
      add(dictId, docId);
    }
  }

  /**
   * Add the docId to the posting list for the dictionary id.
   * @param dictId dictionary id
   * @param docId document id
   */
  void add(int dictId, int docId);
}
