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
package org.apache.pinot.core.segment.index.readers.constant;

import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Inverted index reader for multi-value column with constant values.
 */
public final class ConstantMVInvertedIndexReader implements InvertedIndexReader<MutableRoaringBitmap> {
  private final MutableRoaringBitmap _bitmap;

  public ConstantMVInvertedIndexReader(int numDocs) {
    _bitmap = new MutableRoaringBitmap();
    _bitmap.add(0L, numDocs);
  }

  @Override
  public MutableRoaringBitmap getDocIds(int dictId) {
    return _bitmap;
  }

  @Override
  public void close() {
  }
}
