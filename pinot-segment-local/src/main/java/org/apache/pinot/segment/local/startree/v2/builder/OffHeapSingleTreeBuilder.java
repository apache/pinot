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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.RoaringBitmap;


/// The `OffHeapSingleTreeBuilder` class is the single star-tree builder that uses off-heap memory.
public class OffHeapSingleTreeBuilder extends BaseSingleTreeBuilder {
  private static final String SEGMENT_RECORD_FILE_NAME = "segment.record";
  private static final String STAR_TREE_RECORD_FILE_NAME = "star-tree.record";
  // If the temporary buffer needed is larger than 500M, use MMAP, otherwise use DIRECT
  private static final long MMAP_SIZE_THRESHOLD = 500_000_000;
  private static final byte[] EMPTY_BYTES = new byte[0];

  private final File _segmentRecordFile;
  private final File _starTreeRecordFile;
  private final BufferedOutputStream _starTreeRecordOutputStream;
  private final RecordOffsets _starTreeRecordOffsets;

  /// One bitmap per metric holding the doc ids whose group aggregated over no non-null input, or `null` when null
  /// handling is disabled. A record is stored the way a regular column is: the metric keeps a placeholder in the
  /// serialized record and its nullness lives beside it, so the record layout is unchanged and the arithmetic in
  /// [FixedSizeRecordOffsets] still holds.
  ///
  /// Keying on the doc id is safe because records are never moved: sorting permutes an array of doc ids and compares
  /// through it, leaving each record where it was written.
  private final RoaringBitmap[] _metricNullBitmaps;

  private PinotDataBuffer _starTreeRecordBuffer;
  private int _numReadableStarTreeRecords;

  public OffHeapSingleTreeBuilder(StarTreeV2BuilderConfig builderConfig, File outputDir, ImmutableSegment segment,
      Configuration metadataProperties)
      throws FileNotFoundException {
    super(builderConfig, outputDir, segment, metadataProperties);
    _segmentRecordFile = new File(_outputDir, SEGMENT_RECORD_FILE_NAME);
    Preconditions.checkState(!_segmentRecordFile.exists(), "Segment record file: %s already exists",
        _segmentRecordFile);
    _starTreeRecordFile = new File(_outputDir, STAR_TREE_RECORD_FILE_NAME);
    Preconditions.checkState(!_starTreeRecordFile.exists(), "Star-tree record file: %s already exists",
        _starTreeRecordFile);
    _starTreeRecordOutputStream = new BufferedOutputStream(new FileOutputStream(_starTreeRecordFile));
    _starTreeRecordOffsets = createRecordOffsets();
    _metricNullBitmaps = _nullHandlingEnabled ? new RoaringBitmap[_numMetrics] : null;
  }

  /// Returns [FixedSizeRecordOffsets] when all metrics are serialized with a fixed size (see
  /// [#serializeStarTreeRecord]), where the record start offsets can be computed arithmetically without being stored;
  /// otherwise returns [VariableSizeRecordOffsets].
  private RecordOffsets createRecordOffsets() {
    int recordSize = _numDimensions * Integer.BYTES;
    for (int i = 0; i < _numMetrics; i++) {
      switch (_valueAggregators[i].getAggregatedValueType()) {
        case LONG:
          recordSize += Long.BYTES;
          break;
        case DOUBLE:
          recordSize += Double.BYTES;
          break;
        default:
          return new VariableSizeRecordOffsets();
      }
    }
    return new FixedSizeRecordOffsets(recordSize);
  }

  /// Serializes a record into the temporary star-tree record store.
  ///
  /// A metric that aggregated over no non-null input has no value to write, so the aggregated type's zero is written
  /// in its place and [#appendRecord] records the nullness in [#_metricNullBitmaps]. The placeholder is never read
  /// back: [#deserializeStarTreeRecord] consults the bitmap first.
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
          Object bytesValue = starTreeRecord._metrics[i];
          metricBytes[i] = bytesValue != null ? _valueAggregators[i].serializeAggregatedValue(bytesValue) : EMPTY_BYTES;
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
          Object longValue = starTreeRecord._metrics[i];
          byteBuffer.putLong(longValue != null ? (long) longValue : 0L);
          break;
        case DOUBLE:
          Object doubleValue = starTreeRecord._metrics[i];
          byteBuffer.putDouble(doubleValue != null ? (double) doubleValue : 0d);
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

  /// Deserializes the record at `docId`, whose metrics start at `offset`.
  ///
  /// A metric marked null in [#_metricNullBitmaps] comes back as `null` without its placeholder being deserialized,
  /// which matters for `BYTES`: the placeholder is an empty array that no aggregator can decode.
  private Record deserializeStarTreeRecord(PinotDataBuffer buffer, long offset, int docId) {
    int[] dimensions = new int[_numDimensions];
    for (int i = 0; i < _numDimensions; i++) {
      dimensions[i] = buffer.getInt(offset);
      offset += Integer.BYTES;
    }
    Object[] metrics = new Object[_numMetrics];
    for (int i = 0; i < _numMetrics; i++) {
      switch (_valueAggregators[i].getAggregatedValueType()) {
        case LONG:
          metrics[i] = hasMetricValue(docId, i) ? buffer.getLong(offset) : null;
          offset += Long.BYTES;
          break;
        case DOUBLE:
          metrics[i] = hasMetricValue(docId, i) ? buffer.getDouble(offset) : null;
          offset += Double.BYTES;
          break;
        case BYTES:
          int numBytes = buffer.getInt(offset);
          offset += Integer.BYTES;
          if (hasMetricValue(docId, i)) {
            byte[] bytes = new byte[numBytes];
            buffer.copyTo(offset, bytes);
            metrics[i] = _valueAggregators[i].deserializeAggregatedValue(bytes);
          } else {
            metrics[i] = null;
          }
          offset += numBytes;
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
    _starTreeRecordOffsets.addRecord(bytes.length);
    if (_metricNullBitmaps != null) {
      // The caller assigns this record _numDocs and increments it afterwards, so it is this record's doc id
      markNullMetrics(record, _numDocs);
    }
  }

  /// Records which of the record's metrics aggregated over no non-null input.
  private void markNullMetrics(Record record, int docId) {
    for (int i = 0; i < _numMetrics; i++) {
      if (record._metrics[i] == null) {
        RoaringBitmap nullBitmap = _metricNullBitmaps[i];
        if (nullBitmap == null) {
          nullBitmap = new RoaringBitmap();
          _metricNullBitmaps[i] = nullBitmap;
        }
        nullBitmap.add(docId);
      }
    }
  }

  /// Returns whether the record holds a value for the metric, as opposed to having aggregated over no non-null input.
  private boolean hasMetricValue(int docId, int metricId) {
    if (_metricNullBitmaps == null) {
      return true;
    }
    RoaringBitmap nullBitmap = _metricNullBitmaps[metricId];
    return nullBitmap == null || !nullBitmap.contains(docId);
  }

  @Override
  Record getStarTreeRecord(int docId)
      throws IOException {
    ensureBufferReadable(docId);
    return deserializeStarTreeRecord(_starTreeRecordBuffer, _starTreeRecordOffsets.getStartOffset(docId), docId);
  }

  @Override
  int getDimensionValue(int docId, int dimensionId)
      throws IOException {
    ensureBufferReadable(docId);
    return _starTreeRecordBuffer.getInt(
        _starTreeRecordOffsets.getStartOffset(docId) + (long) dimensionId * Integer.BYTES);
  }

  private void ensureBufferReadable(int docId)
      throws IOException {
    if (_numReadableStarTreeRecords <= docId) {
      _starTreeRecordOutputStream.flush();
      if (_starTreeRecordBuffer != null) {
        _starTreeRecordBuffer.close();
      }
      _starTreeRecordBuffer =
          PinotDataBuffer.mapFile(_starTreeRecordFile, true, 0, _starTreeRecordOffsets.getEndOffset(),
              PinotDataBuffer.NATIVE_ORDER, "OffHeapSingleTreeBuilder: star-tree record buffer");
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
      dataBuffer = PinotDataBuffer.allocateDirect(bufferSize, PinotDataBuffer.NATIVE_ORDER,
          "OffHeapSingleTreeBuilder: segment record buffer");
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
          int dimension1 = dataBuffer.getInt(offset1 + (long) i * Integer.BYTES);
          int dimension2 = dataBuffer.getInt(offset2 + (long) i * Integer.BYTES);
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
      long offset1 = _starTreeRecordOffsets.getStartOffset(sortedDocIds[i1]);
      long offset2 = _starTreeRecordOffsets.getStartOffset(sortedDocIds[i2]);
      for (int i = dimensionId + 1; i < _numDimensions; i++) {
        int dimension1 = _starTreeRecordBuffer.getInt(offset1 + (long) i * Integer.BYTES);
        int dimension2 = _starTreeRecordBuffer.getInt(offset2 + (long) i * Integer.BYTES);
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
    if (_starTreeRecordBuffer != null) {
      _starTreeRecordBuffer.close();
    }
    _starTreeRecordOutputStream.close();
    FileUtils.forceDelete(_starTreeRecordFile);
  }

  /// Per-record offsets within the star-tree record file. [#addRecord] is invoked once per appended record with the
  /// serialized record length; [#getStartOffset] and [#getEndOffset] return absolute offsets within the file.
  @VisibleForTesting
  interface RecordOffsets {

    void addRecord(int numBytes);

    long getStartOffset(int index);

    long getEndOffset();
  }

  /// [RecordOffsets] for fixed-size records (all metrics serialized with a fixed size), where the offsets are
  /// computed arithmetically without being stored.
  @VisibleForTesting
  static class FixedSizeRecordOffsets implements RecordOffsets {
    private final int _recordSize;
    private int _numRecords;

    FixedSizeRecordOffsets(int recordSize) {
      _recordSize = recordSize;
    }

    @Override
    public void addRecord(int numBytes) {
      assert numBytes == _recordSize;
      _numRecords++;
    }

    @Override
    public long getStartOffset(int index) {
      return (long) index * _recordSize;
    }

    @Override
    public long getEndOffset() {
      return (long) _numRecords * _recordSize;
    }
  }

  /// [RecordOffsets] for variable-size records, tracked as a prefix sum of the appended record lengths. Start offsets
  /// are stored as `int` (4 bytes per record) until the first record starting beyond `Integer.MAX_VALUE`, and as
  /// `long` (8 bytes per record) afterwards. The number of star-tree records can go into the hundreds of millions for
  /// large segments, where a boxed `List<Long>` (~28 bytes per record) would dominate the heap.
  @VisibleForTesting
  static class VariableSizeRecordOffsets implements RecordOffsets {
    private final IntArrayList _intOffsets = new IntArrayList();
    private final LongArrayList _longOffsets = new LongArrayList();
    private long _endOffset;

    @Override
    public void addRecord(int numBytes) {
      if (_endOffset <= Integer.MAX_VALUE) {
        _intOffsets.add((int) _endOffset);
      } else {
        _longOffsets.add(_endOffset);
      }
      _endOffset += numBytes;
    }

    @Override
    public long getStartOffset(int index) {
      int numIntOffsets = _intOffsets.size();
      return index < numIntOffsets ? _intOffsets.getInt(index) : _longOffsets.getLong(index - numIntOffsets);
    }

    @Override
    public long getEndOffset() {
      return _endOffset;
    }
  }
}
