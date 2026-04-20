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
package org.apache.pinot.segment.local.segment.index.columnarmap;

import java.io.IOException;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Adapts the per-key inverted index from {@link ImmutableColumnarMapIndexReader} into the
 * standard {@link InvertedIndexReader} interface expected by {@code InvertedIndexFilterOperator}.
 *
 * <p>Maps dictId → value string (via dictionary) → docId bitmap (via getDocsWithKeyValue).
 */
public class ColumnarMapKeyInvertedIndexReader implements InvertedIndexReader<ImmutableRoaringBitmap> {

  private final ImmutableColumnarMapIndexReader _reader;
  private final String _key;
  private final ColumnarMapKeyDictionary _dictionary;

  public ColumnarMapKeyInvertedIndexReader(ImmutableColumnarMapIndexReader reader, String key,
      ColumnarMapKeyDictionary dictionary) {
    _reader = reader;
    _key = key;
    _dictionary = dictionary;
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(int dictId) {
    String value = _dictionary.getStringValue(dictId);
    ImmutableRoaringBitmap bitmap = _reader.getDocsWithKeyValue(_key, value);
    return bitmap != null ? bitmap : ImmutableRoaringBitmap.bitmapOf();
  }

  @Override
  public void close()
      throws IOException {
  }
}
