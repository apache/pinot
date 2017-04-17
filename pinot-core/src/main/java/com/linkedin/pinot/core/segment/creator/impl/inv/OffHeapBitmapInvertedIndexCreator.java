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
import org.apache.commons.io.IOUtils;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.utils.MmapUtils;
import com.linkedin.pinot.core.segment.creator.InvertedIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;

/**
 * This version of Index Creator uses off heap memory to create the posting lists.
 * Typical usage
 * <code>
 * creator = new OffHeapBitmapInvertedIndexCreator(.....);
 * creator.add(int docId, int dictId) //single value
 * creator.add(int docId, int[] dictIds) //multi value
 * creator.seal() //generates the actual files
 * </code>
 * High level idea
 * <p>
 * When add method is invoked, simply store the raw values in valueBuffer (for multi values even
 * lengths are stored for each docId in lengthsBuffer). We also compute the posting list length for
 * each dictId.
 * </p>
 * <p>
 * The actual construction is delayed until seal is called. In seal() method, we first construct the
 * posting list by going over the raw values in value buffer.
 * Once we have the posting list for each dictionary Id, we simply create the mutable roaring bitmap
 * and serialize the output.
 * </p>
 * We first create two files, offsets file and bitmap serialized data and then merge(append) the two
 * files. This file can be read using BitmapInvertedIndexReader.
 * <p>
 * OUTPUT FILE FORMAT
 * </p>
 * <code>
 * [BITMAP OFFSET INDEXES] -- cardinality + 1, each entry is of type INT. We need one extra to know the size of the last bitmap.(we don't really need this but had to do this to maintain backward compatibility)
 * [BITMAP SERIALIZED DATA]-- cardinality, each entry is serialized roaring bitmap data. Length of each entry varies based on content of the bitmap
 * </code>
 * @see HeapBitmapInvertedIndexCreator for heap based implementation
 */
public class OffHeapBitmapInvertedIndexCreator implements InvertedIndexCreator {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(OffHeapBitmapInvertedIndexCreator.class);

  // Number of bytes required to store an integer
  private static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
  // Maximum number of entries for each buffer
  public static final int MAX_NUM_ENTRIES = Integer.MAX_VALUE / INT_SIZE;

  private final File invertedIndexFile;
  private final FieldSpec spec;
  long start = 0;
  private ByteBuffer origValueBuffer;
  private ByteBuffer origLengths;
  IntBuffer valueBuffer;
  IntBuffer lengths;// used in multi value
  int currentVacantPos = 0;
  private int cardinality;
  private int capacity;
  private ByteBuffer origPostingListBuffer;
  private ByteBuffer origPostingListLengths;
  private ByteBuffer origPostingListStartOffsets;
  private ByteBuffer origPostingListCurrentOffsets;
  IntBuffer postingListBuffer; // entire posting list
  IntBuffer postingListLengths; // length of each posting list
  IntBuffer postingListStartOffsets; // start offset in posting List Buffer
  IntBuffer postingListCurrentOffsets; // start offset in posting List Buffer
  private int numDocs;

  public OffHeapBitmapInvertedIndexCreator(File indexDir, int cardinality, int numDocs, int totalNumberOfEntries,
      FieldSpec spec) {
    String columnName = spec.getName();
    Preconditions.checkArgument((cardinality > 0) && (cardinality <= MAX_NUM_ENTRIES),
        "For column: %s, cardinality: %s must > 0 and <= %s", columnName, cardinality, MAX_NUM_ENTRIES);
    Preconditions.checkArgument((numDocs > 0) && (numDocs <= MAX_NUM_ENTRIES),
        "For column: %s, numDocs: %s must > 0 and <= %s", columnName, numDocs, MAX_NUM_ENTRIES);
    if (!spec.isSingleValueField()) {
      Preconditions.checkArgument((totalNumberOfEntries > 0) && (totalNumberOfEntries <= MAX_NUM_ENTRIES),
          "For column: %s, totalNumberOfEntries: %s must > 0 and <= %s for multi-value column", columnName,
          totalNumberOfEntries, MAX_NUM_ENTRIES);
    }

    this.cardinality = cardinality;
    this.numDocs = numDocs;
    this.capacity = spec.isSingleValueField() ? numDocs : totalNumberOfEntries;
    this.spec = spec;
    invertedIndexFile = new File(indexDir, columnName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);

    start = System.currentTimeMillis();

    // create buffers to store raw values
    origValueBuffer = MmapUtils.allocateDirectByteBuffer(this.capacity * INT_SIZE, null,
        "value buffer create bitmap index for " + columnName);
    valueBuffer = origValueBuffer.asIntBuffer();
    if (!spec.isSingleValueField()) {
      origLengths = MmapUtils.allocateDirectByteBuffer(numDocs * INT_SIZE, null,
          "lengths buffer to create bitmap index for " + columnName);
      lengths = origLengths.asIntBuffer();
    } else {
      origLengths = null;
    }

    // create buffer to store posting lists
    origPostingListBuffer = MmapUtils.allocateDirectByteBuffer(this.capacity * INT_SIZE, null,
        "posting list buffer to bitmap index for " + columnName);
    postingListBuffer = origPostingListBuffer.asIntBuffer();
    origPostingListLengths = MmapUtils.allocateDirectByteBuffer(cardinality * INT_SIZE, null,
        "posting list lengths buffer to bitmap index for " + columnName);
    postingListLengths = origPostingListLengths.asIntBuffer();
    origPostingListStartOffsets = MmapUtils.allocateDirectByteBuffer(cardinality * INT_SIZE, null,
        "posting list start offsets buffer to bitmap index for " + columnName);
    postingListStartOffsets = origPostingListStartOffsets.asIntBuffer();
    origPostingListCurrentOffsets = MmapUtils.allocateDirectByteBuffer(cardinality * INT_SIZE, null,
        "posting list current offsets buffer to bitmap index for " + columnName);
    postingListCurrentOffsets = origPostingListCurrentOffsets.asIntBuffer();
  }

  @Override
  public void add(int docId, int dictionaryId) {
    Preconditions.checkArgument(dictionaryId >= 0, "dictionary Id %s must >=0", dictionaryId);
    Preconditions.checkArgument(docId >= 0 && docId < numDocs, "docId Id %s must >=0 and < %s",
        docId, numDocs);
    indexSingleValue(docId, dictionaryId);

  }

  @Override
  public void add(int docId, int[] dictionaryIds) {
    add(docId, dictionaryIds, dictionaryIds.length);
  }

  @Override
  public void add(int docId, int[] dictionaryIds, int length) {
    if (spec.isSingleValueField()) {
      throw new RuntimeException("Method not applicable to single value fields");
    }
    indexMultiValue(docId, dictionaryIds, length);
  }

  @Override
  public long totalTimeTakeSoFar() {
    return (System.currentTimeMillis() - start);
  }

  @Override
  public void seal() throws IOException {
    FileOutputStream fos = null;
    FileInputStream fisOffsets = null;
    FileInputStream fisBitmaps = null;
    final DataOutputStream bitmapsOut;
    final DataOutputStream offsetsOut;
    String tempOffsetsFile = invertedIndexFile + ".offsets";
    String tempBitmapsFile = invertedIndexFile + ".binary";

    try {
      // build the posting list
      constructPostingLists();

      // we need two separate streams, one to write the offsets and another to write the serialized
      // bitmap data. We need two because we dont the serialized length of each bitmap without
      // constructing.
      offsetsOut =
          new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempOffsetsFile)));
      bitmapsOut =
          new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempBitmapsFile)));

      // write out offsets of bitmaps. The information can be used to access a certain bitmap
      // directly.
      // Totally (invertedIndex.length+1) offsets will be written out; the last offset is used to
      // calculate the length of
      // the last bitmap, which might be needed when accessing bitmaps randomly.
      // If a bitmap's offset is k, then k bytes need to be skipped to reach the bitmap.
      int startOffset = 4 * (cardinality + 1);
      offsetsOut.writeInt(startOffset);// The first bitmap's offset
      MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
      for (int i = 0; i < cardinality; i++) {
        bitmap.clear();
        int length = postingListLengths.get(i);
        for (int j = 0; j < length; j++) {
          int bufferOffset = postingListStartOffsets.get(i) + j;
          int value = postingListBuffer.get(bufferOffset);
          bitmap.add(value);
        }
        // serialize bitmap to bitmapsOut stream
        bitmap.serialize(bitmapsOut);
        startOffset += bitmap.serializedSizeInBytes();
        // write offset
        offsetsOut.writeInt(startOffset);
      }
      offsetsOut.close();
      bitmapsOut.close();
      // merge the two files by simply writing offsets data first and then bitmap serialized data
      fos = new FileOutputStream(invertedIndexFile);
      fisOffsets = new FileInputStream(tempOffsetsFile);
      fisBitmaps = new FileInputStream(tempBitmapsFile);
      FileChannel channelOffsets = fisOffsets.getChannel();
      channelOffsets.transferTo(0, channelOffsets.size(), fos.getChannel());

      FileChannel channelBitmaps = fisBitmaps.getChannel();
      channelBitmaps.transferTo(0, channelBitmaps.size(), fos.getChannel());

      LOGGER.debug("persisted bitmap inverted index for column : " + spec.getName() + " in "
          + invertedIndexFile.getAbsolutePath());
    } catch (Exception e) {
      LOGGER.error("Exception while creating bitmap index for column:" + spec.getName(), e);
    } finally {
      IOUtils.closeQuietly(fos);
      IOUtils.closeQuietly(fisOffsets);
      IOUtils.closeQuietly(fisOffsets);
      IOUtils.closeQuietly(fos);
      IOUtils.closeQuietly(fos);
      // MMaputils handles the null checks for buffer
      MmapUtils.unloadByteBuffer(origValueBuffer);
      origValueBuffer = null; valueBuffer = null;
      if (origLengths != null) {
        MmapUtils.unloadByteBuffer(origLengths);
        origLengths = null; lengths = null;
      }
      MmapUtils.unloadByteBuffer(origPostingListBuffer);
      origPostingListBuffer = null; postingListBuffer = null;
      MmapUtils.unloadByteBuffer(origPostingListCurrentOffsets);
      origPostingListCurrentOffsets = null; postingListCurrentOffsets = null;
      MmapUtils.unloadByteBuffer(origPostingListLengths);
      origPostingListLengths = null; postingListLengths = null;
      MmapUtils.unloadByteBuffer(origPostingListStartOffsets);
      origPostingListStartOffsets = null; postingListStartOffsets = null;
      FileUtils.deleteQuietly(new File(tempOffsetsFile));
      FileUtils.deleteQuietly(new File(tempBitmapsFile));
    }
  }

  /**
   * Construct the posting list for each dictId in the original data
   */
  private void constructPostingLists() {
    int offset = 0;
    for (int i = 0; i < cardinality; i++) {
      postingListStartOffsets.put(i, offset);
      postingListCurrentOffsets.put(i, offset);
      offset = offset + postingListLengths.get(i);
    }
    if (spec.isSingleValueField()) {
      for (int i = 0; i < capacity; i++) {
        // read the value
        int dictId = valueBuffer.get(i);
        int dictIdOffset = postingListCurrentOffsets.get(dictId);
        postingListBuffer.put(dictIdOffset, i);
        postingListCurrentOffsets.put(dictId, dictIdOffset + 1);
      }
    } else {
      int startOffset = 0;
      for (int i = 0; i < numDocs; i++) {
        int length = lengths.get(i);
        for (int j = 0; j < length; j++) {
          int dictId = valueBuffer.get((startOffset + j));
          int dictIdOffset = postingListCurrentOffsets.get(dictId);
          postingListBuffer.put(dictIdOffset, i);
          postingListCurrentOffsets.put(dictId, dictIdOffset + 1);

        }
        startOffset = startOffset + length;
      }
    }
  }

  private void indexSingleValue(int docId, int dictId) {
    valueBuffer.put(docId, dictId);
    int length = postingListLengths.get(dictId);
    postingListLengths.put(dictId, length + 1);
  }

  private void indexMultiValue(int docId, int[] entries, int length) {
    for (int i = 0; i < length; i++) {
      final int entry = entries[i];
      Preconditions.checkArgument(entry >= 0, "dictionary Id %s must >=0", entry);
      valueBuffer.put(currentVacantPos + i, entry);
      int postingListLength = postingListLengths.get(entry);
      postingListLengths.put(entry, postingListLength + 1);
    }
    currentVacantPos += length;
    lengths.put(docId, length);
  }

  public File getInvertedIndexFile() {
    return invertedIndexFile;
  }

}
