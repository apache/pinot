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

import java.nio.ByteOrder;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.spi.index.reader.DictionaryIdBasedBitmapProvider;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reader for bitmap based inverted index. Please reference
 * {@link BitmapInvertedIndexWriter} for the index file layout.
 */
public class BitmapInvertedIndexReader
    implements InvertedIndexReader<ImmutableRoaringBitmap>, DictionaryIdBasedBitmapProvider {
  public static final Logger LOGGER = LoggerFactory.getLogger(BitmapInvertedIndexReader.class);

  private final PinotDataBuffer _offsetBuffer;
  private final PinotDataBuffer _bitmapBuffer;

  // Use the offset of the first bitmap to support 2 different format of the inverted index:
  //   1. Offset buffer stores the offsets within the whole data buffer (including offset buffer)
  //   2. Offset buffer stores the offsets within the bitmap buffer
  private final long _firstOffset;

  public BitmapInvertedIndexReader(PinotDataBuffer dataBuffer, int numBitmaps) {
    long offsetBufferEndOffset = (long) (numBitmaps + 1) * Integer.BYTES;
    _offsetBuffer = dataBuffer.view(0, offsetBufferEndOffset, ByteOrder.BIG_ENDIAN);
    _bitmapBuffer = dataBuffer.view(offsetBufferEndOffset, dataBuffer.size());
    _firstOffset = getOffset(0);
  }

  @SuppressWarnings("unchecked")
  @Override
  public ImmutableRoaringBitmap getDocIds(int dictId) {
    long offset = getOffset(dictId);
    long length = getOffset(dictId + 1) - offset;
    return new ImmutableRoaringBitmap(_bitmapBuffer.toDirectByteBuffer(offset - _firstOffset, (int) length));
  }

  private long getOffset(int dictId) {
    return _offsetBuffer.getInt(dictId * Integer.BYTES) & 0xFFFFFFFFL;
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
