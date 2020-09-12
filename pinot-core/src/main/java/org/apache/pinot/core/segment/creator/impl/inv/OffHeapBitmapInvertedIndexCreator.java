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
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Implementation of {@link DictionaryBasedInvertedIndexCreator} that uses off-heap memory.
 * <p>We use 2 passes to create the inverted index.
 * <ul>
 *   <li>
 *     In the first pass (adding values phase), when add() method is called, store the dictIds into the forward index
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
public final class OffHeapBitmapInvertedIndexCreator implements DictionaryBasedInvertedIndexCreator {
  // Use MMapBuffer if the value buffer size is larger than 2G
  private static final int NUM_VALUES_THRESHOLD_FOR_MMAP_BUFFER = 500_000_000;

  private static final String FORWARD_INDEX_VALUE_BUFFER_SUFFIX = ".fwd.idx.val.buf";
  private static final String FORWARD_INDEX_LENGTH_BUFFER_SUFFIX = ".fwd.idx.len.buf";
  private static final String INVERTED_INDEX_VALUE_BUFFER_SUFFIX = ".inv.idx.val.buf";
  private static final String INVERTED_INDEX_LENGTH_BUFFER_SUFFIX = ".inv.idx.len.buf";

  private final File _invertedIndexFile;
  private final File _forwardIndexValueBufferFile;
  private final File _forwardIndexLengthBufferFile;
  private final File _invertedIndexValueBufferFile;
  private final File _invertedIndexLengthBufferFile;
  private final boolean _singleValue;
  private final int _cardinality;
  private final int _numDocs;
  private final int _numValues;
  private final boolean _useMMapBuffer;

  // Forward index buffers (from docId to dictId)
  private int _nextDocId;
  private PinotDataBuffer _forwardIndexValueBuffer;
  // For multi-valued column only because each docId can have multiple dictIds
  private int _nextValueId;
  private PinotDataBuffer _forwardIndexLengthBuffer;

  // Inverted index buffers (from dictId to docId)
  private PinotDataBuffer _invertedIndexValueBuffer;
  private PinotDataBuffer _invertedIndexLengthBuffer;

  public OffHeapBitmapInvertedIndexCreator(File indexDir, FieldSpec fieldSpec, int cardinality, int numDocs,
      int numValues)
      throws IOException {
    String columnName = fieldSpec.getName();
    _invertedIndexFile = new File(indexDir, columnName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    _forwardIndexValueBufferFile = new File(indexDir, columnName + FORWARD_INDEX_VALUE_BUFFER_SUFFIX);
    _forwardIndexLengthBufferFile = new File(indexDir, columnName + FORWARD_INDEX_LENGTH_BUFFER_SUFFIX);
    _invertedIndexValueBufferFile = new File(indexDir, columnName + INVERTED_INDEX_VALUE_BUFFER_SUFFIX);
    _invertedIndexLengthBufferFile = new File(indexDir, columnName + INVERTED_INDEX_LENGTH_BUFFER_SUFFIX);
    _singleValue = fieldSpec.isSingleValueField();
    _cardinality = cardinality;
    _numDocs = numDocs;
    _numValues = _singleValue ? numDocs : numValues;
    _useMMapBuffer = _numValues > NUM_VALUES_THRESHOLD_FOR_MMAP_BUFFER;

    try {
      _forwardIndexValueBuffer = createTempBuffer((long) _numValues * Integer.BYTES, _forwardIndexValueBufferFile);
      if (!_singleValue) {
        _forwardIndexLengthBuffer = createTempBuffer((long) _numDocs * Integer.BYTES, _forwardIndexLengthBufferFile);
      }

      // We need to clear the inverted index length buffer because we rely on the initial value of 0, and keep updating
      // the value instead of directly setting the value
      _invertedIndexLengthBuffer =
          createTempBuffer((long) _cardinality * Integer.BYTES, _invertedIndexLengthBufferFile);
      for (int i = 0; i < _cardinality; i++) {
        _invertedIndexLengthBuffer.putInt((long) i * Integer.BYTES, 0);
      }
    } catch (Exception e) {
      destroyBuffer(_forwardIndexValueBuffer, _forwardIndexValueBufferFile);
      destroyBuffer(_forwardIndexLengthBuffer, _forwardIndexLengthBufferFile);
      destroyBuffer(_invertedIndexLengthBuffer, _invertedIndexLengthBufferFile);
      throw e;
    }
  }

  @Override
  public void add(int dictId) {
    putInt(_forwardIndexValueBuffer, _nextDocId++, dictId);
    putInt(_invertedIndexLengthBuffer, dictId, getInt(_invertedIndexLengthBuffer, dictId) + 1);
  }

  @Override
  public void add(int[] dictIds, int length) {
    for (int i = 0; i < length; i++) {
      int dictId = dictIds[i];
      putInt(_forwardIndexValueBuffer, _nextValueId++, dictId);
      putInt(_invertedIndexLengthBuffer, dictId, getInt(_invertedIndexLengthBuffer, dictId) + 1);
    }
    putInt(_forwardIndexLengthBuffer, _nextDocId++, length);
  }

  @Override
  public void seal()
      throws IOException {
    // Calculate value index for each dictId in the inverted index value buffer
    // Re-use inverted index length buffer to store the value index for each dictId, where value index is the index in
    // the inverted index value buffer where we should put next docId for the dictId
    int invertedValueIndex = 0;
    for (int dictId = 0; dictId < _cardinality; dictId++) {
      int length = getInt(_invertedIndexLengthBuffer, dictId);
      putInt(_invertedIndexLengthBuffer, dictId, invertedValueIndex);
      invertedValueIndex += length;
    }

    // Put values into inverted index value buffer
    _invertedIndexValueBuffer = createTempBuffer((long) _numValues * Integer.BYTES, _invertedIndexValueBufferFile);
    if (_singleValue) {
      for (int docId = 0; docId < _numDocs; docId++) {
        int dictId = getInt(_forwardIndexValueBuffer, docId);
        int index = getInt(_invertedIndexLengthBuffer, dictId);
        putInt(_invertedIndexValueBuffer, index, docId);
        putInt(_invertedIndexLengthBuffer, dictId, index + 1);
      }

      // Destroy buffer no longer needed
      destroyBuffer(_forwardIndexValueBuffer, _forwardIndexValueBufferFile);
      _forwardIndexValueBuffer = null;
    } else {
      int valueId = 0;
      for (int docId = 0; docId < _numDocs; docId++) {
        int length = getInt(_forwardIndexLengthBuffer, docId);
        for (int i = 0; i < length; i++) {
          int dictId = getInt(_forwardIndexValueBuffer, valueId++);
          int index = getInt(_invertedIndexLengthBuffer, dictId);
          putInt(_invertedIndexValueBuffer, index, docId);
          putInt(_invertedIndexLengthBuffer, dictId, index + 1);
        }
      }

      // Destroy buffers no longer needed
      destroyBuffer(_forwardIndexValueBuffer, _forwardIndexValueBufferFile);
      _forwardIndexValueBuffer = null;
      destroyBuffer(_forwardIndexLengthBuffer, _forwardIndexLengthBufferFile);
      _forwardIndexLengthBuffer = null;
    }

    // Create bitmaps from inverted index buffers and serialize them to file
    try (DataOutputStream offsetDataStream = new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(_invertedIndexFile)));
        FileOutputStream bitmapFileStream = new FileOutputStream(_invertedIndexFile);
        DataOutputStream bitmapDataStream = new DataOutputStream(new BufferedOutputStream(bitmapFileStream))) {
      int bitmapOffset = (_cardinality + 1) * Integer.BYTES;
      offsetDataStream.writeInt(bitmapOffset);
      bitmapFileStream.getChannel().position(bitmapOffset);

      int startIndex = 0;
      for (int dictId = 0; dictId < _cardinality; dictId++) {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        int endIndex = getInt(_invertedIndexLengthBuffer, dictId);
        for (int i = startIndex; i < endIndex; i++) {
          bitmap.add(getInt(_invertedIndexValueBuffer, i));
        }
        startIndex = endIndex;

        // Write offset and bitmap into file
        bitmapOffset += bitmap.serializedSizeInBytes();
        // Check for int overflow
        Preconditions.checkState(bitmapOffset > 0, "Inverted index file: %s exceeds 2GB limit", _invertedIndexFile);
        offsetDataStream.writeInt(bitmapOffset);
        bitmap.serialize(bitmapDataStream);
      }
    } catch (Exception e) {
      FileUtils.deleteQuietly(_invertedIndexFile);
      throw e;
    }
  }

  @Override
  public void close()
      throws IOException {
    org.apache.pinot.common.utils.FileUtils
        .close(new DataBufferAndFile(_forwardIndexValueBuffer, _forwardIndexValueBufferFile),
            new DataBufferAndFile(_forwardIndexLengthBuffer, _forwardIndexLengthBufferFile),
            new DataBufferAndFile(_invertedIndexValueBuffer, _invertedIndexValueBufferFile),
            new DataBufferAndFile(_invertedIndexLengthBuffer, _invertedIndexLengthBufferFile));
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

  private static void putInt(PinotDataBuffer buffer, long index, int value) {
    buffer.putInt(index << 2, value);
  }

  private static int getInt(PinotDataBuffer buffer, long index) {
    return buffer.getInt(index << 2);
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
}
