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
package org.apache.pinot.segment.local.segment.readers;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * Compacted Pinot Segment Record Reader used for upsert compaction
 */
public class CompactedPinotSegmentRecordReader implements RecordReader {
  private final PinotSegmentRecordReader _pinotSegmentRecordReader;
  private final RoaringBitmap _validDocIdsBitmap;

  // Valid doc ids iterator
  private PeekableIntIterator _validDocIdsIterator;
  // Reusable generic row to store the next row to return
  private GenericRow _nextRow = new GenericRow();
  // Flag to mark whether we need to fetch another row
  private boolean _nextRowReturned = true;

  public CompactedPinotSegmentRecordReader(File indexDir, RoaringBitmap validDocIds) {
    _pinotSegmentRecordReader = new PinotSegmentRecordReader();
    _pinotSegmentRecordReader.init(indexDir, null, null);
    _validDocIdsBitmap = validDocIds;
    _validDocIdsIterator = validDocIds.getIntIterator();
  }

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
  }

  @Override
  public boolean hasNext() {
    if (!_validDocIdsIterator.hasNext() && _nextRowReturned) {
      return false;
    }

    // If next row has not been returned, return true
    if (!_nextRowReturned) {
      return true;
    }

    // Try to get the next row to return
    if (_validDocIdsIterator.hasNext()) {
      int docId = _validDocIdsIterator.next();
      _nextRow.clear();
      _pinotSegmentRecordReader.getRecord(docId, _nextRow);
      _nextRowReturned = false;
      return true;
    }

    // Cannot find next row to return, return false
    return false;
  }

  @Override
  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    Preconditions.checkState(!_nextRowReturned);
    reuse.init(_nextRow);
    _nextRowReturned = true;
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _pinotSegmentRecordReader.rewind();
    _nextRowReturned = true;
    _validDocIdsIterator = _validDocIdsBitmap.getIntIterator();
  }

  @Override
  public void close()
      throws IOException {
    _pinotSegmentRecordReader.close();
  }
}
