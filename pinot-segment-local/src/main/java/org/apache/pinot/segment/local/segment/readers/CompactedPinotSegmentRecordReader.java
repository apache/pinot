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
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
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

  // Iterator approach for valid document IDs
  private PeekableIntIterator _validDocIdsIterator;

  // Index-based approach for sorted valid document IDs
  private int[] _sortedValidDocIds;
  private int _currentDocIndex = 0;

  // Flag to mark whether we need to fetch another row
  private boolean _nextRowReturned = true;

  public CompactedPinotSegmentRecordReader(RoaringBitmap validDocIds) {
    this(validDocIds, null);
  }

  public CompactedPinotSegmentRecordReader(RoaringBitmap validDocIds, @Nullable String deleteRecordColumn) {
    _pinotSegmentRecordReader = new PinotSegmentRecordReader();
    _validDocIdsBitmap = validDocIds;
    _validDocIdsIterator = validDocIds.getIntIterator();
    _deleteRecordColumn = deleteRecordColumn;
  }

  public CompactedPinotSegmentRecordReader(ThreadSafeMutableRoaringBitmap validDocIds) {
    this(validDocIds, null);
  }

  public CompactedPinotSegmentRecordReader(ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable String deleteRecordColumn) {
    Preconditions.checkNotNull(validDocIds, "Valid document IDs cannot be null");
    _pinotSegmentRecordReader = new PinotSegmentRecordReader();
    _validDocIdsBitmap = validDocIds.getMutableRoaringBitmap().toRoaringBitmap();
    _validDocIdsIterator = _validDocIdsBitmap.getIntIterator();
    _deleteRecordColumn = deleteRecordColumn;
  }

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    // lazy init the record reader
    _pinotSegmentRecordReader.init(dataFile, null, null);
    prepareSortedValidDocIds();
  }

  /**
   * Initializes the record reader from a mutable segment with valid document ids and optional sorted document ids.
   *
   * @param mutableSegment Mutable segment
   * @param sortedDocIds Array of sorted document ids (can be null)
   */
  public void init(MutableSegment mutableSegment, @Nullable int[] sortedDocIds) {
    _pinotSegmentRecordReader.init(mutableSegment, sortedDocIds);
    prepareSortedValidDocIds();
  }

  /**
   * Prepares the sorted valid document IDs array based on whether sorted document IDs are available.
   * If sorted document IDs are available, creates an array of valid document IDs in sorted order.
   * If not available, falls back to bitmap iteration order.
   */
  private void prepareSortedValidDocIds() {
    int[] sortedDocIds = _pinotSegmentRecordReader.getSortedDocIds();
    if (sortedDocIds != null) {
      // Create array of valid document IDs in sorted order
      _sortedValidDocIds = new int[_validDocIdsBitmap.getCardinality()];
      int index = 0;
      for (int docId : sortedDocIds) {
        if (_validDocIdsBitmap.contains(docId)) {
          _sortedValidDocIds[index++] = docId;
        }
      }
    } else {
      // No sorted order available, use bitmap iteration order (existing behavior)
      _sortedValidDocIds = null;
    }
  }

  /**
   * Returns the sorted document ids from the underlying PinotSegmentRecordReader.
   */
  @Nullable
  @Override
  public int[] getSortedDocIds() {
    return _pinotSegmentRecordReader.getSortedDocIds();
  }

  @Override
  public boolean hasNext() {
    // Check if we've exhausted all documents
    if (_sortedValidDocIds != null) {
      // Use sorted valid document IDs
      if (_currentDocIndex >= _sortedValidDocIds.length && _nextRowReturned) {
        return false;
      }
    } else {
      // Fall back to bitmap iterator
      if (!_validDocIdsIterator.hasNext() && _nextRowReturned) {
        return false;
      }
    }

    // If next row has not been returned, return true
    if (!_nextRowReturned) {
      return true;
    }

    // Try to get the next row to return, skip invalid docs and delete records
    if (_sortedValidDocIds != null) {
      return getNextRowFromSortedValidDocIds();
    } else {
      return getNextRowFromBitmapIterator();
    }
  }

  /**
   * Gets the next row using sorted valid document IDs.
   */
  private boolean getNextRowFromSortedValidDocIds() {
    while (_currentDocIndex < _sortedValidDocIds.length) {
      int docId = _sortedValidDocIds[_currentDocIndex++];
      if (processAndValidateRecord(docId)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets the next row using bitmap iterator (fallback for non-sorted case).
   */
  private boolean getNextRowFromBitmapIterator() {
    while (_validDocIdsIterator.hasNext()) {
      int docId = _validDocIdsIterator.next();
      if (processAndValidateRecord(docId)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Common method to process and validate a record for the given document ID.
   * @param docId The document ID to process
   * @return true if the record is valid and should be returned, false if it should be skipped
   */
  private boolean processAndValidateRecord(int docId) {
    _nextRow.clear();
    _pinotSegmentRecordReader.getRecord(docId, _nextRow);
    if (_deleteRecordColumn != null && BooleanUtils.toBoolean(_nextRow.getValue(_deleteRecordColumn))) {
      return false; // Skip delete records
    }
    _nextRowReturned = false;
    return true;
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
    _currentDocIndex = 0;
    _validDocIdsIterator = _validDocIdsBitmap.getIntIterator();
  }

  @Override
  public void close()
      throws IOException {
    _pinotSegmentRecordReader.close();
  }
}
