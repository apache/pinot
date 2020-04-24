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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.Constants;
import org.apache.pinot.core.query.utils.Pair;
import org.apache.pinot.core.segment.creator.InvertedIndexCreator;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

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
 *     In the first pass (adding values phase), when add() method is called, store the raw values into the forward index
 *     value buffer (for multi-valued column also store number of values for each docId into forward index length
 *     buffer). We also compute the inverted index length for each dictId while adding values.
 *   </li>
 *   <li>
 *     In the second pass (processing values phase), when seal() method is called, all the dictIds should already been
 *     added. We first reorder the values into the inverted index buffers by going over the dictIds in forward index
 *     value buffer (for multi-valued column we also need forward index length buffer to get the docId for each dictId).
 *     <p>Once we have the inverted index buffers, we simply go over them and create the bitmap for each dictId and
 *     serialize them into a file.
 *   </li>
 * </ul>
 * <p>Based on the number of values we need to store, we use direct memory or MMap file to allocate the buffer.
 */
public final class RangeIndexCreator implements InvertedIndexCreator {

  private static final int RANGE_INDEX_VERSION = 1;

  // Use MMapBuffer if the value buffer size is larger than 2G
  private static final int NUM_VALUES_THRESHOLD_FOR_MMAP_BUFFER = 500_000_000;
  private static final int DEFAULT_NUM_RANGES = 20;

  private static final String FORWARD_INDEX_VALUE_BUFFER_SUFFIX = ".fwd.idx.val.buf";
  private static final String DOC_ID_VALUE_BUFFER_SUFFIX = ".doc.id.val.buf";

  private final File _invertedIndexFile;
  private final File _forwardIndexValueBufferFile;
  private final File _docIdBufferFileForSorting;
  private final int _numValues;
  private final boolean _useMMapBuffer;

  // Forward index buffers (from docId to dictId)
  private int _nextDocId;
  private PinotDataBuffer _forwardIndexValueBuffer;
  private NumberValueBuffer _valueBuffer;
  //for sorting
  private PinotDataBuffer _docIdValueBuffer;
  private IntValueBuffer _docIdBuffer;
  // For multi-valued column only because each docId can have multiple dictIds
  private int _nextValueId;
  private int _numDocsPerRange;
  private FieldSpec.DataType _valueType;

  public RangeIndexCreator(File indexDir, FieldSpec fieldSpec, FieldSpec.DataType valueType, int numRanges, int numDocsPerRange, int numDocs, int numValues)
      throws IOException {
    _valueType = valueType;
    String columnName = fieldSpec.getName();
    _invertedIndexFile = new File(indexDir, columnName + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    _forwardIndexValueBufferFile = new File(indexDir, columnName + FORWARD_INDEX_VALUE_BUFFER_SUFFIX);
    _docIdBufferFileForSorting = new File(indexDir, columnName + DOC_ID_VALUE_BUFFER_SUFFIX);
    _numValues = fieldSpec.isSingleValueField() ? numDocs : numValues;
    _useMMapBuffer = _numValues > NUM_VALUES_THRESHOLD_FOR_MMAP_BUFFER;
    int valueSize = valueType.size();
    try {
      //use DEFAULT_NUM_RANGES if numRanges is not set
      if(numRanges < 0) {
        numRanges = DEFAULT_NUM_RANGES;
      }
      if(numDocsPerRange < 0) {
        _numDocsPerRange = (int) Math.ceil(_numValues / numRanges);
      }

      //FORWARD INDEX - stores the actual values added
      _forwardIndexValueBuffer = createTempBuffer((long) _numValues * valueSize, _forwardIndexValueBufferFile);

      switch (_valueType) {
        case INT:
          _valueBuffer = new IntValueBuffer(_forwardIndexValueBuffer);
          break;
        case FLOAT:
          _valueBuffer = new FloatValueBuffer(_forwardIndexValueBuffer);
          break;
        case LONG:
          _valueBuffer = new LongValueBuffer(_forwardIndexValueBuffer);
          break;
        case DOUBLE:
          _valueBuffer = new DoubleValueBuffer(_forwardIndexValueBuffer);
          break;
        default:
          throw new UnsupportedOperationException("Range index is not supported for columns of data type:" + valueType);
      }

      //Stores the docId - this will be sorted based on the values in FORWARD INDEX in the end
      _docIdValueBuffer = createTempBuffer((long) _numValues * Integer.BYTES, _forwardIndexValueBufferFile);
      _docIdBuffer = new IntValueBuffer(_docIdValueBuffer);
    } catch (Exception e) {
      destroyBuffer(_forwardIndexValueBuffer, _forwardIndexValueBufferFile);
      destroyBuffer(_forwardIndexValueBuffer, _docIdBufferFileForSorting);
      throw e;
    }
  }

  @Override
  public void add(int dictId) {
    _valueBuffer.put(_nextDocId, dictId);
    _docIdBuffer.put(_nextDocId, _nextDocId);
    _nextDocId = _nextDocId + 1;
  }

  @Override
  public void add(int[] dictIds, int length) {
    for (int i = 0; i < length; i++) {
      int dictId = dictIds[i];
      _valueBuffer.put(_nextDocId, dictId);
      _docIdBuffer.put(_nextValueId, _nextDocId);
      _nextValueId = _nextValueId + 1;
    }
  }

  @Override
  public void addDoc(Object document, int docIdCounter) {
    throw new IllegalStateException("Bitmap inverted index creator does not support Object type currently");
  }

  @Override
  public void seal()
      throws IOException {
    //sort the forward index copy
    //go over the sorted value to compute ranges
    IntComparator comparator = (i, j) -> {
      Number val1 = _valueBuffer.get(_docIdBuffer.get(i).intValue());
      Number val2 = _valueBuffer.get(_docIdBuffer.get(j).intValue());
      return _valueBuffer.compare(val1, val2);
    };
    Swapper swapper = (i, j) -> {
      Number temp = _docIdBuffer.get(i).intValue();
      _docIdBuffer.put(i, _docIdBuffer.get(j).intValue());
      _docIdBuffer.put(j, temp);
    };
    Arrays.quickSort(0, _numValues, comparator, swapper);

//    dump();
    //FIND THE RANGES
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

    // Create bitmaps from inverted index buffers and serialize them to file
    //HEADER
    //   # VERSION (INT)
    //   # DATA_TYPE (String -> INT (length) (ACTUAL BYTES)
    //   # OF RANGES (INT)
    //   <RANGE START VALUE BUFFER> #R & ValueSize
    //   Range Start 0
    //    .........
    //   Range Start R - 1
    //   Bitmap for Range 0 Start Offset
    //      .....
    //   Bitmap for Range R Start Offset
    //BODY
    //   Bitmap for range 0
    //   Bitmap for range 2
    //    ......
    //   Bitmap for range R - 1
    long bytesWritten = 0;
    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(_invertedIndexFile));
        DataOutputStream header = new DataOutputStream(bos);
        FileOutputStream fos = new FileOutputStream(_invertedIndexFile);
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

      //Write the Range values
      header.writeInt(ranges.size());
      bytesWritten += Integer.BYTES;

      for (Pair<Integer, Integer> range : ranges) {
        Number value = _valueBuffer.get(_docIdBuffer.get(range.getFirst()).intValue());
        switch (_valueType) {
          case INT:
            header.writeInt(value.intValue());
            break;
          case LONG:
            header.writeLong(value.longValue());
            break;
          case FLOAT:
            header.writeFloat(value.floatValue());
            break;
          case DOUBLE:
            header.writeDouble(value.doubleValue());
            break;
          default:
            throw new RuntimeException("Range index not supported for dataType: " + _valueType);
        }
      }

      bytesWritten += (ranges.size()) * _valueType.size(); // Range start values

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
        Preconditions.checkState(bitmapOffset > 0, "Inverted index file: %s exceeds 2GB limit", _invertedIndexFile);

        header.writeLong(bitmapOffset);
        bytesWritten += Long.BYTES;

        byte[] bytes = new byte[sizeInBytes];
        bitmap.serialize(ByteBuffer.wrap(bytes));
        dataOutputStream.write(bytes);
        bytesWritten += bytes.length;
      }
    } catch (IOException e) {
      FileUtils.deleteQuietly(_invertedIndexFile);
      throw e;
    }
    Preconditions.checkState(bytesWritten == _invertedIndexFile.length(),
        "Length of inverted index file: " + _invertedIndexFile.length()
            + " does not match the number of bytes written: " + bytesWritten);
  }

  @Override
  public void close()
      throws IOException {
    org.apache.pinot.common.utils.FileUtils
        .close(new DataBufferAndFile(_forwardIndexValueBuffer, _forwardIndexValueBufferFile),
            new DataBufferAndFile(_docIdValueBuffer, _docIdBufferFileForSorting));
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
    System.out.print("[ ");
    for (int i = 0; i < _numValues; i++) {
      System.out.print(_valueBuffer.get(_docIdBuffer.get(i).intValue()) + ", ");
    }
    System.out.println("]");
  }

  private PinotDataBuffer createTempBuffer(long size, File mmapFile)
      throws IOException {
    if (_useMMapBuffer) {
      return PinotDataBuffer.mapFile(mmapFile, false, 0, size, PinotDataBuffer.NATIVE_ORDER,
          "OffHeapBitmapInvertedIndexCreator: temp buffer");
    } else {
      return PinotDataBuffer.allocateDirect(size, PinotDataBuffer.NATIVE_ORDER,
          "OffHeapBitmapInvertedIndexCreator: temp buffer for " + mmapFile.getName());
    }
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
