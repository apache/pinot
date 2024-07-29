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
package org.apache.pinot.segment.local.startree.v2.builder;

import com.google.common.base.Preconditions;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * The {@code OffHeapSingleTreeBuilder} class is the single star-tree builder that uses off-heap memory.
 */
public class OffHeapSingleTreeBuilder extends BaseSingleTreeBuilder {
  private static final String SEGMENT_RECORD_FILE_NAME = "segment.record";
  private static final String STAR_TREE_RECORD_FILE_NAME = "star-tree.record";
  // If the temporary buffer needed is larger than 500M, use MMAP, otherwise use DIRECT
  private static final long MMAP_SIZE_THRESHOLD = 500_000_000;

  private final File _segmentRecordFile;
  private final File _starTreeRecordFile;
  private final BufferedOutputStream _starTreeRecordOutputStream;
  private final List<Long> _starTreeRecordOffsets;

  private PinotDataBuffer _starTreeRecordBuffer;
  private int _numReadableStarTreeRecords;

  /**
   * Constructor for the off-heap single star-tree builder.
   *
   * @param builderConfig Builder config
   * @param outputDir Directory to store the index files
   * @param segment Index segment
   * @param metadataProperties Segment metadata properties
   * @throws FileNotFoundException
   */
  public OffHeapSingleTreeBuilder(StarTreeV2BuilderConfig builderConfig, File outputDir, ImmutableSegment segment,
      Configuration metadataProperties)
      throws FileNotFoundException {
    super(builderConfig, outputDir, segment, metadataProperties);
    _segmentRecordFile = new File(_outputDir, SEGMENT_RECORD_FILE_NAME);
    Preconditions
        .checkState(!_segmentRecordFile.exists(), "Segment record file: " + _segmentRecordFile + " already exists");
    _starTreeRecordFile = new File(_outputDir, STAR_TREE_RECORD_FILE_NAME);
    Preconditions
        .checkState(!_starTreeRecordFile.exists(), "Star-tree record file: " + _starTreeRecordFile + " already exists");
    _starTreeRecordOutputStream = new BufferedOutputStream(new FileOutputStream(_starTreeRecordFile));
    _starTreeRecordOffsets = new ArrayList<>();
    _starTreeRecordOffsets.add(0L);
  }

  @SuppressWarnings("unchecked")
  private byte[] serializeStarTreeRecord(Record starTreeRecord) {
    int numBytes = _numDimensions * Integer.BYTES;
    byte[][] metricBytes = new byte[_numMetrics][];
    for (int i = 0; i < _numMetrics; i++) {
      switch (_valueAggregators[i].getAggregatedValueType()) {
        case LONG:
          numBytes += Long.BYTES;
          break;
        case DOUBLE:
          numBytes += Double.BYTES;
          break;
        case BYTES:
          metricBytes[i] = _valueAggregators[i].serializeAggregatedValue(starTreeRecord._metrics[i]);
          numBytes += Integer.BYTES + metricBytes[i].length;
          break;
        default:
          throw new IllegalStateException();
      }
    }
    byte[] bytes = new byte[numBytes];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(PinotDataBuffer.NATIVE_ORDER);
    for (int dimension : starTreeRecord._dimensions) {
      byteBuffer.putInt(dimension);
    }
    for (int i = 0; i < _numMetrics; i++) {
      switch (_valueAggregators[i].getAggregatedValueType()) {
        case LONG:
          byteBuffer.putLong((Long) starTreeRecord._metrics[i]);
          break;
        case DOUBLE:
          byteBuffer.putDouble((Double) starTreeRecord._metrics[i]);
          break;
        case BYTES:
          byteBuffer.putInt(metricBytes[i].length);
          byteBuffer.put(metricBytes[i]);
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return bytes;
  }

  private Record deserializeStarTreeRecord(PinotDataBuffer buffer, long offset) {
    int[] dimensions = new int[_numDimensions];
    for (int i = 0; i < _numDimensions; i++) {
      dimensions[i] = buffer.getInt(offset);
      offset += Integer.BYTES;
    }
    Object[] metrics = new Object[_numMetrics];
    for (int i = 0; i < _numMetrics; i++) {
      switch (_valueAggregators[i].getAggregatedValueType()) {
        case LONG:
          metrics[i] = buffer.getLong(offset);
          offset += Long.BYTES;
          break;
        case DOUBLE:
          metrics[i] = buffer.getDouble(offset);
          offset += Double.BYTES;
          break;
        case BYTES:
          int numBytes = buffer.getInt(offset);
          offset += Integer.BYTES;
          byte[] bytes = new byte[numBytes];
          buffer.copyTo(offset, bytes);
          offset += numBytes;
          metrics[i] = _valueAggregators[i].deserializeAggregatedValue(bytes);
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return new Record(dimensions, metrics);
  }

  @Override
  void appendRecord(Record record)
      throws IOException {
    byte[] bytes = serializeStarTreeRecord(record);
    _starTreeRecordOutputStream.write(bytes);
    _starTreeRecordOffsets.add(_starTreeRecordOffsets.get(_numDocs) + bytes.length);
  }

  @Override
  Record getStarTreeRecord(int docId)
      throws IOException {
    ensureBufferReadable(docId);
    return deserializeStarTreeRecord(_starTreeRecordBuffer, _starTreeRecordOffsets.get(docId));
  }

  @Override
  int getDimensionValue(int docId, int dimensionId)
      throws IOException {
    ensureBufferReadable(docId);
    return _starTreeRecordBuffer.getInt(_starTreeRecordOffsets.get(docId) + dimensionId * Integer.BYTES);
  }

  private void ensureBufferReadable(int docId)
      throws IOException {
    if (_numReadableStarTreeRecords <= docId) {
      _starTreeRecordOutputStream.flush();
      if (_starTreeRecordBuffer != null) {
        _starTreeRecordBuffer.close();
      }
      _starTreeRecordBuffer = PinotDataBuffer
          .mapFile(_starTreeRecordFile, true, 0, _starTreeRecordOffsets.get(_numDocs), PinotDataBuffer.NATIVE_ORDER,
              "OffHeapSingleTreeBuilder: star-tree record buffer");
      _numReadableStarTreeRecords = _numDocs;
    }
  }

  @Override
  Iterator<Record> sortAndAggregateSegmentRecords(int numDocs)
      throws IOException {
    // Write all dimensions for segment records into the buffer, and sort all records using an int array
    PinotDataBuffer dataBuffer;
    long bufferSize = (long) numDocs * _numDimensions * Integer.BYTES;
    if (bufferSize > MMAP_SIZE_THRESHOLD) {
      dataBuffer = PinotDataBuffer.mapFile(_segmentRecordFile, false, 0, bufferSize, PinotDataBuffer.NATIVE_ORDER,
          "OffHeapSingleTreeBuilder: segment record buffer");
    } else {
      dataBuffer = PinotDataBuffer
          .allocateDirect(bufferSize, PinotDataBuffer.NATIVE_ORDER, "OffHeapSingleTreeBuilder: segment record buffer");
    }
    int[] sortedDocIds = new int[numDocs];
    for (int i = 0; i < numDocs; i++) {
      sortedDocIds[i] = i;
    }
    try {
      long offset = 0;
      for (int i = 0; i < numDocs; i++) {
        int[] dimensions = getSegmentRecordDimensions(i);
        for (int j = 0; j < _numDimensions; j++) {
          dataBuffer.putInt(offset, dimensions[j]);
          offset += Integer.BYTES;
        }
      }
      it.unimi.dsi.fastutil.Arrays.quickSort(0, numDocs, (i1, i2) -> {
        long offset1 = (long) sortedDocIds[i1] * _numDimensions * Integer.BYTES;
        long offset2 = (long) sortedDocIds[i2] * _numDimensions * Integer.BYTES;
        for (int i = 0; i < _numDimensions; i++) {
          int dimension1 = dataBuffer.getInt(offset1 + i * Integer.BYTES);
          int dimension2 = dataBuffer.getInt(offset2 + i * Integer.BYTES);
          if (dimension1 != dimension2) {
            return dimension1 - dimension2;
          }
        }
        return 0;
      }, (i1, i2) -> {
        int temp = sortedDocIds[i1];
        sortedDocIds[i1] = sortedDocIds[i2];
        sortedDocIds[i2] = temp;
      });
    } finally {
      dataBuffer.close();
      if (_segmentRecordFile.exists()) {
        FileUtils.forceDelete(_segmentRecordFile);
      }
    }

    // Create an iterator for aggregated records
    return new Iterator<Record>() {
      boolean _hasNext = true;
      Record _currentRecord = getSegmentRecord(sortedDocIds[0]);
      int _docId = 1;

      @Override
      public boolean hasNext() {
        return _hasNext;
      }

      @Override
      public Record next() {
        Record next = mergeSegmentRecord(null, _currentRecord);
        while (_docId < numDocs) {
          Record record = getSegmentRecord(sortedDocIds[_docId++]);
          if (!Arrays.equals(record._dimensions, next._dimensions)) {
            _currentRecord = record;
            return next;
          } else {
            next = mergeSegmentRecord(next, record);
          }
        }
        _hasNext = false;
        return next;
      }
    };
  }

  @Override
  Iterator<Record> generateRecordsForStarNode(int startDocId, int endDocId, int dimensionId)
      throws IOException {
    ensureBufferReadable(endDocId);

    // Sort all records using an int array
    int numDocs = endDocId - startDocId;
    int[] sortedDocIds = new int[numDocs];
    for (int i = 0; i < numDocs; i++) {
      sortedDocIds[i] = startDocId + i;
    }
    it.unimi.dsi.fastutil.Arrays.quickSort(0, numDocs, (i1, i2) -> {
      long offset1 = _starTreeRecordOffsets.get(sortedDocIds[i1]);
      long offset2 = _starTreeRecordOffsets.get(sortedDocIds[i2]);
      for (int i = dimensionId + 1; i < _numDimensions; i++) {
        int dimension1 = _starTreeRecordBuffer.getInt(offset1 + i * Integer.BYTES);
        int dimension2 = _starTreeRecordBuffer.getInt(offset2 + i * Integer.BYTES);
        if (dimension1 != dimension2) {
          return dimension1 - dimension2;
        }
      }
      return 0;
    }, (i1, i2) -> {
      int temp = sortedDocIds[i1];
      sortedDocIds[i1] = sortedDocIds[i2];
      sortedDocIds[i2] = temp;
    });

    // Create an iterator for aggregated records
    return new Iterator<Record>() {
      boolean _hasNext = true;
      Record _currentRecord = getStarTreeRecord(sortedDocIds[0]);
      int _docId = 1;

      private boolean hasSameDimensions(Record record1, Record record2) {
        for (int i = dimensionId + 1; i < _numDimensions; i++) {
          if (record1._dimensions[i] != record2._dimensions[i]) {
            return false;
          }
        }
        return true;
      }

      @Override
      public boolean hasNext() {
        return _hasNext;
      }

      @Override
      public Record next() {
        Record next = mergeStarTreeRecord(null, _currentRecord);
        next._dimensions[dimensionId] = StarTreeV2Constants.STAR_IN_FORWARD_INDEX;
        while (_docId < numDocs) {
          Record record;
          try {
            record = getStarTreeRecord(sortedDocIds[_docId++]);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          if (!hasSameDimensions(record, _currentRecord)) {
            _currentRecord = record;
            return next;
          } else {
            next = mergeStarTreeRecord(next, record);
          }
        }
        _hasNext = false;
        return next;
      }
    };
  }

  @Override
  public void close()
      throws IOException {
    super.close();
    _starTreeRecordBuffer.close();
    _starTreeRecordOutputStream.close();
    FileUtils.forceDelete(_starTreeRecordFile);
  }
}
