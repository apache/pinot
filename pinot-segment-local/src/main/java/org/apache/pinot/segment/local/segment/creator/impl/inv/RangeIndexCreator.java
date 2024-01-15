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
package org.apache.pinot.segment.local.segment.creator.impl.inv;

import com.google.common.base.Charsets;
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
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.spi.index.creator.CombinedInvertedIndexCreator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;
import static org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Range index creator that uses off-heap memory.
 * <p>We use 2 passes to create the range index.
 * <ul>
 *   <li>
 *     In the first pass (adding values phase), when add() method is called, store the raw values into the value buffer
 *     (for multi-valued column we flatten the values). We also store the corresponding docId in docIdBuffer which will
 *     be sorted in the next phase based on the value in valueBuffer.
 *   </li>
 *   <li>
 *     In the second pass (processing values phase), when seal() method is called, we sort the docIdBuffer based on the
 *     value in valueBuffer. We then iterate over the sorted docIdBuffer and create ranges such that each range
 *     comprises of _numDocsPerRange.
 *   </li>
 * </ul>
 */
public final class RangeIndexCreator implements CombinedInvertedIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(RangeIndexCreator.class);

  //This will dump the content of temp buffers and ranges
  private static final boolean TRACE = false;

  public static final int VERSION = 1;

  private static final int DEFAULT_NUM_RANGES = 20;

  private static final String VALUE_BUFFER_SUFFIX = "val.buf";
  private static final String DOC_ID_VALUE_BUFFER_SUFFIX = ".doc.id.buf";

  //output file which will hold the range index
  private final File _rangeIndexFile;

  //File where the input values will be stored. This is a temp file that will be deleted at the end
  private final File _tempValueBufferFile;
  //pinot data buffer MMapped - maps the content of _tempValueBufferFile
  private PinotDataBuffer _tempValueBuffer;
  //a simple wrapper over _tempValueBuffer to make it easy to read/write any Number (INT,LONG, FLOAT, DOUBLE)
  private NumberValueBuffer _numberValueBuffer;

  //File where the docId will be stored. Temp file that will be deleted at the end
  private final File _tempDocIdBufferFile;
  //pinot data buffer MMapped - maps the content of _tempDocIdBufferFile
  private PinotDataBuffer _docIdValueBuffer;
  //a simple wrapper over _docIdValueBuffer to make it easy to read/write any INT
  private IntValueBuffer _docIdBuffer;

  private final int _numValues;
  private int _nextDocId;
  private int _nextValueId;
  private final int _numValuesPerRange;
  private final DataType _valueType;

  /**
   *
   * @param indexDir destination of the range index file
   * @param fieldSpec fieldspec of the column to generate the range index
   * @param valueType DataType of the column, INT if dictionary encoded, or INT, FLOAT, LONG, DOUBLE for raw encoded
   * @param numRanges Number of ranges, use DEFAULT_NUM_RANGES if not configured (<= 0)
   * @param numValuesPerRange Number of values per range, calculate from numRanges if not configured (<= 0)
   * @param numDocs total number of documents
   * @param numValues total number of values, used for Multi value columns (for single value columns numDocs==
   *                  numValues)
   * @throws IOException
   */
  public RangeIndexCreator(File indexDir, FieldSpec fieldSpec, DataType valueType, int numRanges, int numValuesPerRange,
      int numDocs, int numValues)
      throws IOException {
    _valueType = valueType;
    String columnName = fieldSpec.getName();
    _rangeIndexFile = new File(indexDir, columnName + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    _tempValueBufferFile = new File(indexDir, columnName + VALUE_BUFFER_SUFFIX);
    _tempDocIdBufferFile = new File(indexDir, columnName + DOC_ID_VALUE_BUFFER_SUFFIX);
    _numValues = fieldSpec.isSingleValueField() ? numDocs : numValues;
    int valueSize = valueType.size();
    try {
      Preconditions.checkArgument(numRanges <= 0 || numValuesPerRange <= 0,
          "At most one of 'numRanges' and 'numValuesPerRange' should be configured");
      if (numRanges > 0) {
        _numValuesPerRange = (_numValues + numRanges - 1) / numRanges;
      } else if (numValuesPerRange > 0) {
        _numValuesPerRange = numValuesPerRange;
      } else {
        _numValuesPerRange = (_numValues + DEFAULT_NUM_RANGES - 1) / DEFAULT_NUM_RANGES;
      }

      //Value buffer to store the values added via add method
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
  public DataType getDataType() {
    return _valueType;
  }

  @Override
  public void add(int value) {
    _numberValueBuffer.put(_nextDocId, value);
    _docIdBuffer.put(_nextDocId, _nextDocId);
    _nextDocId++;
  }

  @Override
  public void add(int[] values, int length) {
    for (int i = 0; i < length; i++) {
      _numberValueBuffer.put(_nextValueId, values[i]);
      _docIdBuffer.put(_nextValueId, _nextDocId);
      _nextValueId++;
    }
    _nextDocId++;
  }

  @Override
  public void add(long value) {
    _numberValueBuffer.put(_nextDocId, value);
    _docIdBuffer.put(_nextDocId, _nextDocId);
    _nextDocId++;
  }

  @Override
  public void add(long[] values, int length) {
    for (int i = 0; i < length; i++) {
      _numberValueBuffer.put(_nextValueId, values[i]);
      _docIdBuffer.put(_nextValueId, _nextDocId);
      _nextValueId++;
    }
    _nextDocId++;
  }

  @Override
  public void add(float value) {
    _numberValueBuffer.put(_nextDocId, value);
    _docIdBuffer.put(_nextDocId, _nextDocId);
    _nextDocId++;
  }

  @Override
  public void add(float[] values, int length) {
    for (int i = 0; i < length; i++) {
      _numberValueBuffer.put(_nextValueId, values[i]);
      _docIdBuffer.put(_nextValueId, _nextDocId);
      _nextValueId++;
    }
    _nextDocId++;
  }

  @Override
  public void add(double value) {
    _numberValueBuffer.put(_nextDocId, value);
    _docIdBuffer.put(_nextDocId, _nextDocId);
    _nextDocId++;
  }

  @Override
  public void add(double[] values, int length) {
    for (int i = 0; i < length; i++) {
      _numberValueBuffer.put(_nextValueId, values[i]);
      _docIdBuffer.put(_nextValueId, _nextDocId);
      _nextValueId++;
    }
    _nextDocId++;
  }

  /**
   * Generates the range Index file
   * Sample output by running RangeIndexCreatorTest with TRACE=true and change log4.xml in core to info
   * 15:18:47.330 RangeIndexCreator - Before sorting
   * 15:18:47.333 RangeIndexCreator - DocIdBuffer  [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
   * 18, 19, ]
   * 15:18:47.333 RangeIndexCreator - ValueBuffer  [ 3, 0, 0, 0, 3, 1, 3, 0, 2, 4, 4, 2, 4, 3, 2, 1, 0, 2, 0, 3, ]
   * 15:18:47.371 RangeIndexCreator - After sorting
   * 15:18:47.371 RangeIndexCreator - DocIdBuffer  [ 16, 3, 1, 2, 7, 18, 15, 5, 14, 8, 17, 11, 0, 4, 6, 13, 19, 10,
   * 9, 12, ]
   * 15:18:47.371 RangeIndexCreator - ValueBuffer  [ 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 4, 4, ]
   * 15:18:47.372 RangeIndexCreator - rangeOffsets = [ (0,5) ,(6,7) ,(8,11) ,(12,16) ,(17,19) , ]
   * 15:18:47.372 RangeIndexCreator - rangeValues = [ (0,0) ,(1,1) ,(2,2) ,(3,3) ,(4,4) , ]
   *
   * @throws IOException
   */
  @Override
  public void seal()
      throws IOException {
    if (TRACE) {
      LOGGER.info("Before sorting");
      dump();
    }

    //Sorts the value buffer while maintaining the mapping with the docId.
    //The mapping is needed  in the subsequent phase where we generate the bitmap for each range.
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

    int boundary = _numValuesPerRange;
    int start = 0;
    for (int i = 0; i < _numValues; i++) {
      if (i > start + boundary) {
        if (comparator.compare(i, i - 1) != 0) {
          ranges.add(Pair.of(start, i - 1));
          start = i;
        }
      }
    }
    ranges.add(Pair.of(start, _numValues - 1));

    //Dump ranges
    if (TRACE) {
      dumpRanges(ranges);
    }
    // RANGE INDEX FILE LAYOUT
    //HEADER
    //   # VERSION (INT)
    //   # DATA_TYPE (String -> INT (length) (ACTUAL BYTES)
    //   # Number OF RANGES (INT)
    //   <RANGE START VALUE BUFFER> # (R + 1 )* ValueSize
    //   Range Start 0,
    //    .........
    //   Range Start R - 1
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
        FileChannel channel = fos.getChannel();
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(fos))) {

      //VERSION
      header.writeInt(VERSION);

      bytesWritten += Integer.BYTES;

      //value data type
      byte[] valueDataTypeBytes = _valueType.name().getBytes(Charsets.UTF_8);
      header.writeInt(valueDataTypeBytes.length);
      bytesWritten += Integer.BYTES;

      header.write(valueDataTypeBytes);
      bytesWritten += valueDataTypeBytes.length;

      //Write the number of ranges
      header.writeInt(ranges.size());
      bytesWritten += Integer.BYTES;

      //write the range start values
      for (Pair<Integer, Integer> range : ranges) {
        Number rangeStart = _numberValueBuffer.get(range.getLeft());
        writeNumberToHeader(header, rangeStart);
      }
      bytesWritten += ranges.size() * _valueType.size(); // Range start values

      Number lastRangeEnd = _numberValueBuffer.get(ranges.get(ranges.size() - 1).getRight());
      writeNumberToHeader(header, lastRangeEnd);
      bytesWritten += _valueType.size(); // Last range end value

      //compute the offset where the bitmap for the first range would be written
      //bitmap start offset for each range, one extra to make it easy to get the length for last one.
      long bitmapOffsetHeaderSize = (ranges.size() + 1) * Long.BYTES;

      long bitmapOffset = bytesWritten + bitmapOffsetHeaderSize;
      header.writeLong(bitmapOffset);
      bytesWritten += Long.BYTES;

      channel.position(bitmapOffset);

      for (int i = 0; i < ranges.size(); i++) {
        Pair<Integer, Integer> range = ranges.get(i);
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int index = range.getLeft(); index <= range.getRight(); index++) {
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
      rangeOffsets.append("(").append(range.getLeft()).append(",").append(range.getRight()).append(") ,");
      rangeValues.append("(").append(_numberValueBuffer.get(range.getLeft())).append(",")
          .append(_numberValueBuffer.get(range.getRight())).append(") ,");
    }
    rangeOffsets.append(" ]");
    rangeValues.append(" ]");
    LOGGER.info("rangeOffsets = " + rangeOffsets);
    LOGGER.info("rangeValues = " + rangeValues);
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

  public int getNumValuesPerRange() {
    return _numValuesPerRange;
  }

  void dump() {
    StringBuilder docIdAsString = new StringBuilder("DocIdBuffer  [ ");
    for (int i = 0; i < _numValues; i++) {
      docIdAsString.append(_docIdBuffer.get(i) + ", ");
    }
    docIdAsString.append("]");
    LOGGER.info(docIdAsString.toString());

    StringBuilder valuesAsString = new StringBuilder("ValueBuffer  [ ");
    for (int i = 0; i < _numValues; i++) {
      valuesAsString.append(_numberValueBuffer.get(i) + ", ");
    }
    valuesAsString.append("] ");
    LOGGER.info(valuesAsString.toString());
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

  private interface NumberValueBuffer {

    void put(int position, Number value);

    Number get(int position);

    int compare(Number val1, Number val2);
  }

  private static class IntValueBuffer implements NumberValueBuffer {
    private final PinotDataBuffer _dataBuffer;

    IntValueBuffer(PinotDataBuffer dataBuffer) {
      _dataBuffer = dataBuffer;
    }

    @Override
    public void put(int position, Number value) {
      _dataBuffer.putInt(position << 2, value.intValue());
    }

    @Override
    public Number get(int position) {
      return _dataBuffer.getInt(position << 2);
    }

    @Override
    public int compare(Number val1, Number val2) {
      return Integer.compare(val1.intValue(), val2.intValue());
    }
  }

  private static class LongValueBuffer implements NumberValueBuffer {
    private final PinotDataBuffer _dataBuffer;

    LongValueBuffer(PinotDataBuffer dataBuffer) {
      _dataBuffer = dataBuffer;
    }

    @Override
    public void put(int position, Number value) {
      _dataBuffer.putLong(position << 3, value.longValue());
    }

    @Override
    public Number get(int position) {
      return _dataBuffer.getLong(position << 3);
    }

    @Override
    public int compare(Number val1, Number val2) {
      return Long.compare(val1.longValue(), val2.longValue());
    }
  }

  private static class FloatValueBuffer implements NumberValueBuffer {
    private final PinotDataBuffer _dataBuffer;

    FloatValueBuffer(PinotDataBuffer dataBuffer) {
      _dataBuffer = dataBuffer;
    }

    @Override
    public void put(int position, Number value) {
      _dataBuffer.putFloat(position << 2, value.floatValue());
    }

    @Override
    public Number get(int position) {
      return _dataBuffer.getFloat(position << 2);
    }

    @Override
    public int compare(Number val1, Number val2) {
      return Float.compare(val1.floatValue(), val2.floatValue());
    }
  }

  private static class DoubleValueBuffer implements NumberValueBuffer {
    private final PinotDataBuffer _dataBuffer;

    DoubleValueBuffer(PinotDataBuffer dataBuffer) {
      _dataBuffer = dataBuffer;
    }

    @Override
    public void put(int position, Number value) {
      _dataBuffer.putDouble(position << 3, value.doubleValue());
    }

    @Override
    public Number get(int position) {
      return _dataBuffer.getDouble(position << 3);
    }

    @Override
    public int compare(Number val1, Number val2) {
      return Double.compare(val1.doubleValue(), val2.doubleValue());
    }
  }
}
