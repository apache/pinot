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

import java.io.IOException;
import java.util.List;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.OffHeapFSTStore;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.pinot.segment.local.utils.fst.PinotBufferIndexInput;
import org.apache.pinot.segment.local.utils.fst.RegexpMatcher;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class loads FST index from PinotDataBuffer and creates a FST reader which
 * is used in finding matching results for regexp queries. Since FST index currently
 * stores dict ids as values this class only implements getDictIds method.
 *
 */
public class LuceneFSTIndexReader implements TextIndexReader {
  public static final Logger LOGGER = LoggerFactory.getLogger(LuceneFSTIndexReader.class);

  private final PinotDataBuffer _dataBuffer;
  private final PinotBufferIndexInput _dataBufferIndexInput;
  private final FST<Long> _readFST;

  public LuceneFSTIndexReader(PinotDataBuffer pinotDataBuffer)
      throws IOException {
    _dataBuffer = pinotDataBuffer;
    _dataBufferIndexInput = new PinotBufferIndexInput(_dataBuffer, 0L, _dataBuffer.size());

    _readFST =
        new FST(_dataBufferIndexInput, _dataBufferIndexInput, PositiveIntOutputs.getSingleton(), new OffHeapFSTStore());
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    throw new RuntimeException("LuceneFSTIndexReader only supports getDictIds currently.");
  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    try {
      MutableRoaringBitmap dictIds = new MutableRoaringBitmap();
      List<Long> matchingIds = RegexpMatcher.regexMatch(searchQuery, _readFST);
      for (Long matchingId : matchingIds) {
        dictIds.add(matchingId.intValue());
      }
      return dictIds.toImmutableRoaringBitmap();
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
