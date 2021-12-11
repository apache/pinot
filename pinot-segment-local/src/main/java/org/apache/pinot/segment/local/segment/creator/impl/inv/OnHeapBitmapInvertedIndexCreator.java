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
package org.apache.pinot.segment.local.segment.creator.impl.inv;

import java.io.File;
import java.io.IOException;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.roaringbitmap.Container;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;


/**
 * Implementation of {@link DictionaryBasedInvertedIndexCreator} that uses on-heap memory.
 */
@SuppressWarnings("unchecked")
public final class OnHeapBitmapInvertedIndexCreator implements DictionaryBasedInvertedIndexCreator {
  private final File _invertedIndexFile;
  private final RoaringBitmapWriter<RoaringBitmap>[] _bitmapWriters;
  private int _nextDocId;

  public OnHeapBitmapInvertedIndexCreator(File indexDir, String columnName, int cardinality) {
    _invertedIndexFile = new File(indexDir, columnName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    RoaringBitmapWriter.Wizard<Container, RoaringBitmap> writerWizard = RoaringBitmapWriter.writer();
    _bitmapWriters = new RoaringBitmapWriter[cardinality];
    for (int i = 0; i < cardinality; i++) {
      _bitmapWriters[i] = writerWizard.get();
    }
  }

  @Override
  public void add(int dictId) {
    _bitmapWriters[dictId].add(_nextDocId++);
  }

  @Override
  public void add(int[] dictIds, int length) {
    for (int i = 0; i < length; i++) {
      _bitmapWriters[dictIds[i]].add(_nextDocId);
    }
    _nextDocId++;
  }

  @Override
  public void seal()
      throws IOException {
    try (BitmapInvertedIndexWriter writer = new BitmapInvertedIndexWriter(_invertedIndexFile, _bitmapWriters.length)) {
      for (RoaringBitmapWriter<RoaringBitmap> bitmapWriter : _bitmapWriters) {
        writer.add(bitmapWriter.get());
      }
    }
  }

  @Override
  public void close() {
  }
}
