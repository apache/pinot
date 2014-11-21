package com.linkedin.pinot.core.segment.creator.impl;


import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.creator.InvertedIndexCreator;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 12, 2014
 */

public class SegmentInvertedIndexCreatorImpl implements InvertedIndexCreator{
  private static final Logger logger = Logger.getLogger(SegmentInvertedIndexCreatorImpl.class);

  private final File invertedIndexFile;
  private final FieldSpec spec;
  private final MutableRoaringBitmap[] invertedIndex;
  long start = 0;

  public SegmentInvertedIndexCreatorImpl(File indexDir, int cardinality , FieldSpec spec) {
    this.spec = spec;
    invertedIndexFile = new File(indexDir, spec.getName() + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    invertedIndex = new MutableRoaringBitmap[cardinality];
    for (int i = 0; i < invertedIndex.length; ++i) {
      invertedIndex[i] = new MutableRoaringBitmap();
    }
    start = System.currentTimeMillis();
  }

  @Override
  public void add(int dictionaryId, int docId) {
    invertedIndex[dictionaryId].add(docId);
  }

  @Override
  public long totalTimeTakeSoFar() {
    return (System.currentTimeMillis() - start);
  }

  @Override
  public void seal() throws IOException {
    final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(invertedIndexFile)));
    // First, write out offsets of bitmaps. The information can be used to access a certain bitmap directly.
    // Totally (invertedIndex.length+1) offsets will be written out; the last offset is used to calculate the length of
    // the last bitmap, which might be needed when accessing bitmaps randomly.
    // If a bitmap's offset is k, then k bytes need to be skipped to reach the bitmap.
    int offset = 4 * (invertedIndex.length + 1); // The first bitmap's offset
    out.writeInt(offset);
    for (final MutableRoaringBitmap element : invertedIndex) { // the other bitmap's offset
      offset += element.serializedSizeInBytes();
      out.writeInt(offset);
    }
    // write out bitmaps one by one
    for (final MutableRoaringBitmap element : invertedIndex) {
      element.serialize(out);
    }
    out.close();
    logger.info("persisted bitmap inverted index for column : " + spec.getName() + " in "
        + invertedIndexFile.getAbsolutePath());
  }

  @Override
  public void add(Object e, int docId) {
    if (spec.isSingleValueField()) {
      final int entry = ((Integer)e).intValue();
      indexSingleValue(entry, docId);
      return;
    }

    final Object [] entryArr = ((Object[])e);
    Arrays.sort(entryArr);

    final int[] entries = new int[entryArr.length];

    for (int i = 0; i < entryArr.length; i++) {
      entries[i] = ((Integer)entryArr[i]).intValue();
    }

    indexMultiValue(entries, docId);
  }

  private void indexSingleValue(int entry, int docId) {
    if (entry == -1) {
      return;
    }
    invertedIndex[entry].add(docId);
  }

  private void indexMultiValue(int[] entries, int docId) {
    for (final int entrie : entries) {
      if (entrie != -1) {
        invertedIndex[entrie].add(docId);
      }
    }
  }
}
