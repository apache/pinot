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
package org.apache.pinot.core.segment.creator.impl.inv;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.query.utils.Pair;
import org.apache.pinot.core.segment.creator.InvertedIndexCreator;
import org.apache.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.pinot.core.segment.creator.impl.V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;


/**
 * Implementation of {@link InvertedIndexCreator} that uses off-heap memory.
 * <p>We use 2 passes to create the range index.
 * <ul>
 *
 *   <li>
 *     A
 *   </li>
 *   <li>
 *     In the first pass (adding values phase), when add() method is called, store the raw values into the
 *     value buffer (for multi-valued column we flatten the values).
 *     We also store the corresponding docId in docIdBuffer which will be sorted in the next phase based on the value in valueBuffer.
 *
 *   </li>
 *   <li>
 *     In the second pass (processing values phase), when seal() method is called, we sort the docIdBuffer based on the value in valueBuffer.
 *     We then iterate over the sorted docIdBuffer and create ranges such that each range comprises of _numDocsPerRange.
 *     While
 *   </li>
 * </ul>
 */
public final class RangeIndexCreator implements InvertedIndexCreator {

  private static final Logger LOGGER = LoggerFactory.getLogger(RangeIndexCreator.class);

  //This will dump the content of temp buffers and ranges
  private static final boolean TRACE = false;

  private static final int RANGE_INDEX_VERSION = 1;

  private static final int DEFAULT_NUM_RANGES = 20;

  private static final String VALUE_BUFFER_SUFFIX = "val.buf";
  private static final String DOC_ID_VALUE_BUFFER_SUFFIX = ".doc.id.buf";

  private final File _rangeIndexFile;

  private final File _tempValueBufferFile;
  private PinotDataBuffer _tempValueBuffer;
  private NumberValueBuffer _numberValueBuffer;

  private final File _tempDocIdBufferFile;
  private PinotDataBuffer _docIdValueBuffer;
  private IntValueBuffer _docIdBuffer;

  private final int _numValues;
  private int _nextDocId;
  private int _nextValueId;
  private int _numDocsPerRange;
  private FieldSpec.DataType _valueType;

  public RangeIndexCreator(File indexDir, FieldSpec fieldSpec, FieldSpec.DataType valueType, int numRanges,
      int numDocsPerRange, int numDocs, int numValues)
      throws IOException {
    _valueType = valueType;
    String columnName = fieldSpec.getName();
    _rangeIndexFile = new File(indexDir, columnName + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    _tempValueBufferFile = new File(indexDir, columnName + VALUE_BUFFER_SUFFIX);
    _tempDocIdBufferFile = new File(indexDir, columnName + DOC_ID_VALUE_BUFFER_SUFFIX);
    _numValues = fieldSpec.isSingleValueField() ? numDocs : numValues;
    int valueSize = valueType.size();
    try {
      //use DEFAULT_NUM_RANGES if numRanges is not set
      if (numRanges < 0) {
        numRanges = DEFAULT_NUM_RANGES;
      }
      if (numDocsPerRange < 0) {
        _numDocsPerRange = (int) Math.ceil(_numValues / numRanges);
      }

      //Value buffer to store the actual values added
      _tempValueBuffer = createTempBuffer((long) _numValues * valueSize, _tempValueBufferFile);

      switch (_valueType) {
        case INT:
          _numberValueBuffer = new IntValueBuffer(_tempValueBuffer);
          break;
        case FLOAT:
          _numberValueBuffer = new FloatValueBuffer(_tempValueBuffer);
          break;
        case LONG:
          _numberValueBuffer = new LongValueBuffer(_tempValueBuffer);
          break;
        case DOUBLE:
          _numberValueBuffer = new DoubleValueBuffer(_tempValueBuffer);
          break;
        default:
          throw new UnsupportedOperationException("Range index is not supported for columns of data type:" + valueType);
      }

      //docId  Buffer
      _docIdValueBuffer = createTempBuffer((long) _numValues * Integer.BYTES, _tempDocIdBufferFile);
      _docIdBuffer = new IntValueBuffer(_docIdValueBuffer);
    } catch (Exception e) {
      destroyBuffer(_tempValueBuffer, _tempValueBufferFile);
      destroyBuffer(_tempValueBuffer, _tempDocIdBufferFile);
      throw e;
    }
  }

  @Override
  public void add(int dictId) {
    _numberValueBuffer.put(_nextDocId, dictId);
    _docIdBuffer.put(_nextDocId, _nextDocId);
    _nextDocId = _nextDocId + 1;
  }

  @Override
  public void add(int[] dictIds, int length) {
    for (int i = 0; i < length; i++) {
      int dictId = dictIds[i];
      _numberValueBuffer.put(_nextValueId, dictId);
      _docIdBuffer.put(_nextValueId, _nextDocId);
      _nextValueId = _nextValueId + 1;
    }
    _nextDocId = _nextDocId + 1;
  }

  @Override
  public void addDoc(Object document, int docIdCounter) {
    throw new IllegalStateException("Bitmap inverted index creator does not support Object type currently");
  }

  @Override
  public void seal()
      throws IOException {
    if (TRACE) {
      LOGGER.info("Before sorting");
      dump();
    }

    //sort the value buffer, change the docId buffer to maintain the mapping
    IntComparator comparator = (i, j) -> {
      Number val1 = _numberValueBuffer.get(i);
      Number val2 = _numberValueBuffer.get(j);
      return _numberValueBuffer.compare(val1, val2);
    };
    Swapper swapper = (i, j) -> {
      Number temp = _docIdBuffer.get(i).intValue();
      _docIdBuffer.put(i, _docIdBuffer.get(j).intValue());
      _docIdBuffer.put(j, temp);

      Number tempValue = _numberValueBuffer.get(i);
      _numberValueBuffer.put(i, _numberValueBuffer.get(j));
      _numberValueBuffer.put(j, tempValue);
    };
    Arrays.quickSort(0, _numValues, comparator, swapper);

    if (TRACE) {
      LOGGER.info("After sorting");
      dump();
    }
    //FIND THE RANGES
    //go over the sorted value to compute ranges
    List<Pair<Integer, Integer>> ranges = new ArrayList<>();

    int boundary = _numDocsPerRange;
    int start = 0;
    for (int i = 0; i < _numValues; i++) {
      if (i > start + boundary) {
        if (comparator.compare(i, i - 1) != 0) {
          ranges.add(new Pair(start, i - 1));
          start = i;
        }
      }
    }
    ranges.add(new Pair(start, _numValues - 1));

    //Dump ranges
    if (TRACE) {
      dumpRanges(ranges);
    }

    // Create bitmaps from inverted index buffers and serialize them to file
    //HEADER
    //   # VERSION (INT)
    //   # DATA_TYPE (String -> INT (length) (ACTUAL BYTES)
    //   # OF RANGES (INT)
    //   <RANGE START VALUE BUFFER> #2 * R & ValueSize
    //   Range Start 0, Range End 0
    //    .........
    //   Range Start R - 1, Range End R - 1
    //   Range MAX VALUE
    //   Bitmap for Range 0 Start Offset
    //      .....
    //   Bitmap for Range R Start Offset
    //BODY
    //   Bitmap for range 0
    //   Bitmap for range 2
    //    ......
    //   Bitmap for range R - 1
    long bytesWritten = 0;
    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(_rangeIndexFile));
        DataOutputStream header = new DataOutputStream(bos);
        FileOutputStream fos = new FileOutputStream(_rangeIndexFile);
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(fos))) {

      //VERSION
      header.writeInt(RANGE_INDEX_VERSION);

      bytesWritten += Integer.BYTES;

      //value data type
      byte[] valueDataTypeBytes = _valueType.name().getBytes(UTF_8);
      header.writeInt(valueDataTypeBytes.length);
      bytesWritten += Integer.BYTES;

      header.write(valueDataTypeBytes);
      bytesWritten += valueDataTypeBytes.length;

      //Write the number of ranges
      header.writeInt(ranges.size());
      bytesWritten += Integer.BYTES;

      //write the range start values
      for (Pair<Integer, Integer> range : ranges) {
        Number rangeStart = _numberValueBuffer.get(range.getFirst());
        writeNumberToHeader(header, rangeStart);
      }
      bytesWritten += ranges.size() * _valueType.size(); // Range start values

      Number lastRangeEnd = _numberValueBuffer.get(ranges.get(ranges.size() - 1).getSecond());
      writeNumberToHeader(header, lastRangeEnd);
      bytesWritten += _valueType.size(); // Last range end value

      //compute the offset where the bitmap for the first range would be written
      //bitmap start offset for each range, one extra to make it easy to get the length for last one.
      long bitmapOffsetHeaderSize = (ranges.size() + 1) * Long.BYTES;

      long bitmapOffset = bytesWritten + bitmapOffsetHeaderSize;
      header.writeLong(bitmapOffset);
      bytesWritten += Long.BYTES;
      fos.getChannel().position(bitmapOffset);

      for (int i = 0; i < ranges.size(); i++) {
        Pair<Integer, Integer> range = ranges.get(i);
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int index = range.getFirst(); index <= range.getSecond(); index++) {
          bitmap.add(_docIdBuffer.get(index).intValue());
        }
        // Write offset and bitmap into file
        int sizeInBytes = bitmap.serializedSizeInBytes();
        bitmapOffset += sizeInBytes;

        // Check for int overflow
        Preconditions.checkState(bitmapOffset > 0, "Inverted index file: %s exceeds 2GB limit", _rangeIndexFile);

        header.writeLong(bitmapOffset);
        bytesWritten += Long.BYTES;

        byte[] bytes = new byte[sizeInBytes];
        bitmap.serialize(ByteBuffer.wrap(bytes));
        dataOutputStream.write(bytes);
        bytesWritten += bytes.length;
      }
    } catch (IOException e) {
      FileUtils.deleteQuietly(_rangeIndexFile);
      throw e;
    }
    Preconditions.checkState(bytesWritten == _rangeIndexFile.length(),
        "Length of inverted index file: " + _rangeIndexFile.length() + " does not match the number of bytes written: "
            + bytesWritten);
  }

  private void writeNumberToHeader(DataOutputStream header, Number number)
      throws IOException {
    switch (_valueType) {
      case INT:
        header.writeInt(number.intValue());
        break;
      case LONG:
        header.writeLong(number.longValue());
        break;
      case FLOAT:
        header.writeFloat(number.floatValue());
        break;
      case DOUBLE:
        header.writeDouble(number.doubleValue());
        break;
      default:
        throw new RuntimeException("Range index not supported for dataType: " + _valueType);
    }
  }

  private void dumpRanges(List<Pair<Integer, Integer>> ranges) {
    StringBuilder rangeOffsets = new StringBuilder("[ ");
    StringBuilder rangeValues = new StringBuilder("[ ");
    for (Pair<Integer, Integer> range : ranges) {
      rangeOffsets.append("(").append(range.getFirst()).append(",").append(range.getSecond()).append(") ,");
      rangeValues.append("(").append(_numberValueBuffer.get(range.getFirst())).append(",")
          .append(_numberValueBuffer.get(range.getSecond())).append(") ,");
    }
    rangeOffsets.append(" ]");
    rangeValues.append(" ]");
    LOGGER.debug("rangeOffsets = " + rangeOffsets);
    LOGGER.debug("rangeValues = " + rangeValues);
  }

  @Override
  public void close()
      throws IOException {
    org.apache.pinot.common.utils.FileUtils.close(new DataBufferAndFile(_tempValueBuffer, _tempValueBufferFile),
        new DataBufferAndFile(_docIdValueBuffer, _tempDocIdBufferFile));
  }

  private class DataBufferAndFile implements Closeable {
    private final PinotDataBuffer _dataBuffer;
    private final File _file;

    DataBufferAndFile(final PinotDataBuffer buffer, final File file) {
      _dataBuffer = buffer;
      _file = file;
    }

    @Override
    public void close()
        throws IOException {
      destroyBuffer(_dataBuffer, _file);
    }
  }

  void dump() {
    System.out.print("DocId  [ ");
    for (int i = 0; i < _numValues; i++) {
      System.out.print(_docIdBuffer.get(i) + ", ");
    }
    System.out.println("]");
    System.out.print("Values [ ");
    for (int i = 0; i < _numValues; i++) {
      System.out.print(_numberValueBuffer.get(i) + ", ");
    }
    System.out.println("] ");
  }

  private PinotDataBuffer createTempBuffer(long size, File mmapFile)
      throws IOException {
    return PinotDataBuffer
        .mapFile(mmapFile, false, 0, size, PinotDataBuffer.NATIVE_ORDER, "RangeIndexCreator: temp buffer");
  }

  private void destroyBuffer(PinotDataBuffer buffer, File mmapFile)
      throws IOException {
    if (buffer != null) {
      buffer.close();
      if (mmapFile.exists()) {
        FileUtils.forceDelete(mmapFile);
      }
    }
  }

  interface NumberValueBuffer {

    void put(int position, Number value);

    Number get(int position);

    int compare(Number val1, Number val2);
  }

  class IntValueBuffer implements NumberValueBuffer {

    private PinotDataBuffer _buffer;

    IntValueBuffer(PinotDataBuffer buffer) {

      _buffer = buffer;
    }

    @Override
    public void put(int position, Number value) {
      _buffer.putInt(position << 2, value.intValue());
    }

    @Override
    public Number get(int position) {
      return _buffer.getInt(position << 2);
    }

    @Override
    public int compare(Number val1, Number val2) {
      return Integer.compare(val1.intValue(), val2.intValue());
    }
  }

  class LongValueBuffer implements NumberValueBuffer {

    private PinotDataBuffer _buffer;

    LongValueBuffer(PinotDataBuffer buffer) {

      _buffer = buffer;
    }

    @Override
    public void put(int position, Number value) {
      _buffer.putInt(position << 3, value.intValue());
    }

    @Override
    public Number get(int position) {
      return _buffer.getInt(position << 3);
    }

    @Override
    public int compare(Number val1, Number val2) {
      return Long.compare(val1.longValue(), val2.longValue());
    }
  }

  class FloatValueBuffer implements NumberValueBuffer {

    private PinotDataBuffer _buffer;

    FloatValueBuffer(PinotDataBuffer buffer) {

      _buffer = buffer;
    }

    @Override
    public void put(int position, Number value) {
      _buffer.putInt(position << 2, value.intValue());
    }

    @Override
    public Number get(int position) {
      return _buffer.getInt(position << 2);
    }

    @Override
    public int compare(Number val1, Number val2) {
      return Long.compare(val1.longValue(), val2.longValue());
    }
  }

  class DoubleValueBuffer implements NumberValueBuffer {

    private PinotDataBuffer _buffer;

    DoubleValueBuffer(PinotDataBuffer buffer) {

      _buffer = buffer;
    }

    @Override
    public void put(int position, Number value) {
      _buffer.putInt(position << 3, value.intValue());
    }

    @Override
    public Number get(int position) {
      return _buffer.getInt(position << 3);
    }

    @Override
    public int compare(Number val1, Number val2) {
      return Long.compare(val1.longValue(), val2.longValue());
    }
  }
}
