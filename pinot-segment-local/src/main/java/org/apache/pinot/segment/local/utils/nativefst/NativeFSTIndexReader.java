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
package org.apache.pinot.segment.local.utils.nativefst;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class loads FST index from PinotDataBuffer and creates a FST reader which
 * is used in finding matching results for regexp queries. Since FST index currently
 * stores dict ids as values this class only implements getDictIds method.
 *
 * This class works on top of ImmutableFST.
 *
 */
public class NativeFSTIndexReader implements TextIndexReader {
  public static final Logger LOGGER =
      LoggerFactory.getLogger(org.apache.pinot.segment.local.segment.index.readers.LuceneFSTIndexReader.class);

  private final PinotDataBuffer _dataBuffer;

  private final FST _readFST;

  public NativeFSTIndexReader(PinotDataBuffer pinotDataBuffer)
      throws IOException {
    this._dataBuffer = pinotDataBuffer;

    List<ByteBuffer> inputList = new ArrayList<>();

    inputList.add(_dataBuffer.toDirectByteBuffer(0, (int) _dataBuffer.size()));

    this._readFST =
        FST.read(new ByteBufferInputStream(inputList), ImmutableFST.class, true);
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    throw new RuntimeException("LuceneFSTIndexReader only supports getDictIds currently.");
  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    try {
      RoaringBitmapWriter<MutableRoaringBitmap> dictIds = RoaringBitmapWriter
          .bufferWriter().get();

      RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
      RegexpMatcher.regexMatch(searchQuery, this._readFST, writer::add);

      MutableRoaringBitmap matchingIds = writer.get();

      for (Integer matchingId : matchingIds) {
        dictIds.add(matchingId.intValue());
      }
      return dictIds.get();
    } catch (Exception ex) {
      LOGGER.error("Error getting matching Ids from FST", ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void close()
      throws IOException {
    // Do Nothing
  }
}
