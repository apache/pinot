/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.creator.impl.inv;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.utils.MmapUtils;
import com.linkedin.pinot.core.segment.creator.InvertedIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import org.apache.commons.io.FileUtils;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Implementation of {@link InvertedIndexCreator} that uses off-heap memory.
 * <ul>
 *   High level idea:
 *   <li>
 *     We use 2 passes to create the inverted index.
 *   </li>
 *   <li>
 *     In the first pass (adding values phase), when addSV/addMV method is called, store the values (dictIds) into
 *     value buffer (for multi-value column also store number of values for each docId into numValues buffer). We also
 *     compute the posting list length for each value while processing values.
 *   </li>
 *   <li>
 *     We create inverted index in the second pass when seal method is called. By this time, all the values should
 *     already been added. We first construct the posting list by going over the values in value buffer (for multi-value
 *     column we also need numValues buffer to get the docId for each value).
 *     <p>Once we have the posting list for each value (dictId), we simply go over the posting list and create the
 *     bitmap for each value and serialize them into a file.
 *   </li>
 *   <li>
 *     When serializing bitmaps, we first create two files, one for bitmap offsets and one for serialized data. After
 *     serializing all bitmaps, append the serialized data file to the offsets file to get the final inverted index
 *     file.
 *   </li>
 * </ul>
 */
public final class OffHeapBitmapInvertedIndexCreator implements InvertedIndexCreator {
  // Maximum number of values due to 2GB file size limit
  public static final int MAX_NUM_VALUES = Integer.MAX_VALUE / V1Constants.Numbers.INTEGER_SIZE;

  private final String _columnName;
  private final boolean _singleValue;
  private final int _cardinality;
  private final int _numDocs;
  private final int _numValues;
  private final File _indexDir;
  private final File _invertedIndexFile;

  private ByteBuffer _valueByteBuffer;
  private IntBuffer _valueIntBuffer;

  // For multi-value column only
  private int _valueBufferOffset;
  private ByteBuffer _numValuesByteBuffer;
  private IntBuffer _numValuesIntBuffer;

  // Posting list related buffers
  // Constructed in first pass
  private ByteBuffer _postingListLengthByteBuffer;
  private IntBuffer _postingListLengthIntBuffer;
  // Constructed in second pass
  private ByteBuffer _postingListValueByteBuffer;
  private ByteBuffer _postingListCurrentOffsetByteBuffer;

  public OffHeapBitmapInvertedIndexCreator(File indexDir, FieldSpec fieldSpec, int cardinality, int numDocs,
      int numValues) {
    _columnName = fieldSpec.getName();
    _singleValue = fieldSpec.isSingleValueField();
    _cardinality = cardinality;
    _numDocs = numDocs;
    if (_singleValue) {
      Preconditions.checkArgument(numDocs <= MAX_NUM_VALUES,
          "For single-value column: %s, numDocs: %s is too large to create inverted index", _columnName, numDocs);
      _numValues = numDocs;
    } else {
      Preconditions.checkArgument(numValues <= MAX_NUM_VALUES,
          "For multi-value column: %s, numValues: %s is too large to create inverted index", _columnName, numValues);
      _numValues = numValues;
    }
    _indexDir = indexDir;
    _invertedIndexFile = new File(indexDir, _columnName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);

    try {
      // Create value buffer
      _valueByteBuffer = MmapUtils.allocateDirectByteBuffer(_numValues * V1Constants.Numbers.INTEGER_SIZE, null,
          "bitmap value buffer for: " + _columnName);
      _valueIntBuffer = _valueByteBuffer.asIntBuffer();

      // Create numValues buffer for multi-value column
      if (!_singleValue) {
        _numValuesByteBuffer = MmapUtils.allocateDirectByteBuffer(_numDocs * V1Constants.Numbers.INTEGER_SIZE, null,
            "bitmap numValues buffer for: " + _columnName);
        _numValuesIntBuffer = _numValuesByteBuffer.asIntBuffer();
      }

      // Create length buffer for posting list
      _postingListLengthByteBuffer =
          MmapUtils.allocateDirectByteBuffer(_cardinality * V1Constants.Numbers.INTEGER_SIZE, null,
              "bitmap posting list length buffer for: " + _columnName);
      _postingListLengthIntBuffer = _postingListLengthByteBuffer.asIntBuffer();
    } catch (Exception e) {
      MmapUtils.unloadByteBuffer(_valueByteBuffer);
      MmapUtils.unloadByteBuffer(_numValuesByteBuffer);
      MmapUtils.unloadByteBuffer(_postingListLengthByteBuffer);
      throw e;
    }
  }

  @Override
  public void addSV(int docId, int dictId) {
    _valueIntBuffer.put(docId, dictId);
    _postingListLengthIntBuffer.put(dictId, _postingListLengthIntBuffer.get(dictId) + 1);
  }

  @Override
  public void addMV(int docId, int[] dictIds) {
    addMV(docId, dictIds, dictIds.length);
  }

  @Override
  public void addMV(int docId, int[] dictIds, int numDictIds) {
    _numValuesIntBuffer.put(docId, numDictIds);
    for (int i = 0; i < numDictIds; i++) {
      int dictId = dictIds[i];
      _valueIntBuffer.put(_valueBufferOffset++, dictId);
      _postingListLengthIntBuffer.put(dictId, _postingListLengthIntBuffer.get(dictId) + 1);
    }
  }

  @Override
  public void seal() throws IOException {
    // Construct the posting list

    // Create posting list value buffer
    _postingListValueByteBuffer =
        MmapUtils.allocateDirectByteBuffer(_numValues * V1Constants.Numbers.INTEGER_SIZE, null,
            "bitmap posting list value buffer for: " + _columnName);
    IntBuffer postingListValueIntBuffer = _postingListValueByteBuffer.asIntBuffer();

    // Create posting list current offset buffer and initialize it using posting list length buffer
    _postingListCurrentOffsetByteBuffer =
        MmapUtils.allocateDirectByteBuffer(_cardinality * V1Constants.Numbers.INTEGER_SIZE, null,
            "bitmap posting list current offset buffer for: " + _columnName);
    IntBuffer postingListCurrentOffsetIntBuffer = _postingListCurrentOffsetByteBuffer.asIntBuffer();
    int postingListStartOffset = 0;
    for (int dictId = 0; dictId < _cardinality; dictId++) {
      postingListCurrentOffsetIntBuffer.put(dictId, postingListStartOffset);
      postingListStartOffset += _postingListLengthIntBuffer.get(dictId);
    }

    // Dump values into posting list
    if (_singleValue) {
      for (int docId = 0; docId < _numDocs; docId++) {
        int dictId = _valueIntBuffer.get(docId);
        int offset = postingListCurrentOffsetIntBuffer.get(dictId);
        postingListValueIntBuffer.put(offset++, docId);
        postingListCurrentOffsetIntBuffer.put(dictId, offset);
      }
    } else {
      int valueOffset = 0;
      for (int docId = 0; docId < _numDocs; docId++) {
        int numValues = _numValuesIntBuffer.get(docId);
        for (int i = 0; i < numValues; i++) {
          int dictId = _valueIntBuffer.get(valueOffset++);
          int offset = postingListCurrentOffsetIntBuffer.get(dictId);
          postingListValueIntBuffer.put(offset++, docId);
          postingListCurrentOffsetIntBuffer.put(dictId, offset);
        }
      }
      // Release numValues buffer
      MmapUtils.unloadByteBuffer(_numValuesByteBuffer);
      _numValuesByteBuffer = null;
    }

    // Release value buffer and posting list current offset buffer
    MmapUtils.unloadByteBuffer(_valueByteBuffer);
    _valueByteBuffer = null;
    MmapUtils.unloadByteBuffer(_postingListCurrentOffsetByteBuffer);
    _postingListCurrentOffsetByteBuffer = null;

    File tempBitmapDataFile = new File(_indexDir, _invertedIndexFile.getName() + ".tmp");
    try {
      // First write offsets and serialized bitmaps into two files
      try (DataOutputStream offsetOut = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(_invertedIndexFile)));
          DataOutputStream bitmapOut = new DataOutputStream(
              new BufferedOutputStream(new FileOutputStream(tempBitmapDataFile)))) {
        // Write the offset for the first bitmap
        int bitmapDataStartOffset = (_cardinality + 1) * V1Constants.Numbers.INTEGER_SIZE;
        offsetOut.writeInt(bitmapDataStartOffset);

        // Write offsets and serialized bitmaps
        int postingListOffset = 0;
        for (int dictId = 0; dictId < _cardinality; dictId++) {
          MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
          int postingListLength = _postingListLengthIntBuffer.get(dictId);
          for (int i = 0; i < postingListLength; i++) {
            bitmap.add(postingListValueIntBuffer.get(postingListOffset++));
          }
          bitmapDataStartOffset += bitmap.serializedSizeInBytes();
          offsetOut.writeInt(bitmapDataStartOffset);
          bitmap.serialize(bitmapOut);
        }
      }

      // Append the bitmap data file to the inverted index file
      try (FileChannel out = new FileOutputStream(_invertedIndexFile, true).getChannel();
          FileChannel in = new FileInputStream(tempBitmapDataFile).getChannel()) {
        // jfim: For some reason, it seems like the second argument of transferFrom is relative on Linux while it is
        // an absolute position on MacOS X. As such, we reposition the stream to 0 on both platforms to make it an
        // absolute position call.
        out.position(0).transferFrom(in, out.size(), in.size());
      }
    } catch (Exception e) {
      FileUtils.deleteQuietly(_invertedIndexFile);
      FileUtils.deleteQuietly(tempBitmapDataFile);
      throw e;
    }
  }

  @Override
  public void close() {
    MmapUtils.unloadByteBuffer(_valueByteBuffer);
    MmapUtils.unloadByteBuffer(_numValuesByteBuffer);
    MmapUtils.unloadByteBuffer(_postingListLengthByteBuffer);
    MmapUtils.unloadByteBuffer(_postingListValueByteBuffer);
    MmapUtils.unloadByteBuffer(_postingListCurrentOffsetByteBuffer);
  }
}
