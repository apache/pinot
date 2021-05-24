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
package org.apache.pinot.segment.local.segment.index.readers.geospatial;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.pinot.segment.local.segment.creator.impl.inv.geospatial.BaseH3IndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.LongDictionary;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reader of the H3 index. Please reference {@link BaseH3IndexCreator} for the index file layout.
 */
public class ImmutableH3IndexReader implements H3IndexReader {
  public static final Logger LOGGER = LoggerFactory.getLogger(ImmutableH3IndexReader.class);

  private final H3IndexResolution _resolution;
  private final LongDictionary _dictionary;
  private final BitmapInvertedIndexReader _invertedIndex;

  /**
   * Constructs an inverted index with the specified size.
   * @param dataBuffer data buffer for the inverted index.
   */
  public ImmutableH3IndexReader(PinotDataBuffer dataBuffer) {
    int version = dataBuffer.getInt(0);
    Preconditions.checkArgument(version == BaseH3IndexCreator.VERSION, "Unsupported H3 index version: %s", version);
    int numValues = dataBuffer.getInt(Integer.BYTES);
    _resolution = new H3IndexResolution(dataBuffer.getShort(2 * Integer.BYTES));

    long dictionaryOffset = 2 * Integer.BYTES + Short.BYTES;
    long invertedIndexOffset = dictionaryOffset + (long) numValues * Long.BYTES;
    PinotDataBuffer dictionaryBuffer = dataBuffer.view(dictionaryOffset, invertedIndexOffset, ByteOrder.BIG_ENDIAN);
    PinotDataBuffer invertedIndexBuffer = dataBuffer.view(invertedIndexOffset, dataBuffer.size(), ByteOrder.BIG_ENDIAN);
    _dictionary = new LongDictionary(dictionaryBuffer, numValues);
    _invertedIndex = new BitmapInvertedIndexReader(invertedIndexBuffer, numValues);
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(long h3Id) {
    int dictId = _dictionary.indexOf(String.valueOf(h3Id));
    return dictId >= 0 ? _invertedIndex.getDocIds(dictId) : new MutableRoaringBitmap();
  }

  @Override
  public H3IndexResolution getH3IndexResolution() {
    return _resolution;
  }

  @Override
  public void close()
      throws IOException {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.

    _dictionary.close();
    _invertedIndex.close();
  }
}
