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
package org.apache.pinot.segment.local.segment.index.readers.vec;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.pinot.segment.local.segment.creator.impl.inv.geospatial.BaseH3IndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.vec.BaseVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.BytesDictionary;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.readers.Vector;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reader of the H3 index. Please reference {@link BaseH3IndexCreator} for the index file layout.
 */
public class ImmutableVectorIndexReader implements VectorIndexReader {
  public static final Logger LOGGER = LoggerFactory.getLogger(
      org.apache.pinot.segment.local.segment.index.readers.geospatial.ImmutableH3IndexReader.class);

  private final BytesDictionary _dictionary;
  private final BitmapInvertedIndexReader _invertedIndex;

  /**
   * Constructs an inverted index with the specified size.
   * @param dataBuffer data buffer for the inverted index.
   */
  public ImmutableVectorIndexReader(PinotDataBuffer dataBuffer, int vectorSize, int vectorValueSize) {
    int version = dataBuffer.getInt(0);
    Preconditions.checkArgument(version == BaseVectorIndexCreator.VERSION, "Unsupported Vector index version: %s",
        version);
    int numValues = dataBuffer.getInt(Integer.BYTES);

    long dictionaryOffset = 4 * Integer.BYTES;
    long invertedIndexOffset = dictionaryOffset + (long) numValues * vectorSize * vectorValueSize;
    PinotDataBuffer dictionaryBuffer = dataBuffer.view(dictionaryOffset, invertedIndexOffset, ByteOrder.BIG_ENDIAN);
    PinotDataBuffer invertedIndexBuffer = dataBuffer.view(invertedIndexOffset, dataBuffer.size(), ByteOrder.BIG_ENDIAN);

    _dictionary = new BytesDictionary(dictionaryBuffer, numValues, vectorSize * vectorValueSize);

    _invertedIndex = new BitmapInvertedIndexReader(invertedIndexBuffer, numValues);
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(Vector vector) {
    int dictId = _dictionary.indexOf(new ByteArray(vector.toBytes()));
    return dictId >= 0 ? _invertedIndex.getDocIds(dictId) : new MutableRoaringBitmap();
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
