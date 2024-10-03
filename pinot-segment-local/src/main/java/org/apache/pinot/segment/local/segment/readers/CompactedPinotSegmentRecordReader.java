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
import org.apache.pinot.spi.utils.BooleanUtils;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * Compacted Pinot Segment Record Reader used for upsert compaction
 */
public class CompactedPinotSegmentRecordReader implements RecordReader {
  private final PinotSegmentRecordReader _pinotSegmentRecordReader;
  private final RoaringBitmap _validDocIdsBitmap;
  private final String _deleteRecordColumn;
  // Reusable generic row to store the next row to return
  private final GenericRow _nextRow = new GenericRow();
  // Valid doc ids iterator
  private PeekableIntIterator _validDocIdsIterator;
  // Flag to mark whether we need to fetch another row
  private boolean _nextRowReturned = true;

  public CompactedPinotSegmentRecordReader(File indexDir, RoaringBitmap validDocIds) {
    this(indexDir, validDocIds, null);
  }

  public CompactedPinotSegmentRecordReader(File indexDir, RoaringBitmap validDocIds,
      @Nullable String deleteRecordColumn) {
    _pinotSegmentRecordReader = new PinotSegmentRecordReader();
    _pinotSegmentRecordReader.init(indexDir, null, null);
    _validDocIdsBitmap = validDocIds;
    _validDocIdsIterator = validDocIds.getIntIterator();
    _deleteRecordColumn = deleteRecordColumn;
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

    // Try to get the next row to return, skip invalid docs. If _deleteRecordColumn is set, the deleteRecord (i.e.
    // the tombstone record used to soft-delete old record) is also skipped.
    // Note that dropping deleteRecord too soon may cause the old soft-deleted record to show up unexpectedly, so one
    // should be careful when to skip the deleteRecord.
    while (_validDocIdsIterator.hasNext()) {
      int docId = _validDocIdsIterator.next();
      _nextRow.clear();
      _pinotSegmentRecordReader.getRecord(docId, _nextRow);
      if (_deleteRecordColumn != null && BooleanUtils.toBoolean(_nextRow.getValue(_deleteRecordColumn))) {
        continue;
      }
      _nextRowReturned = false;
      return true;
    }

    // Cannot find next row to return, return false
    return false;
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
