package com.linkedin.pinot.core.indexsegment.columnar.creator;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.common.data.FieldSpec;


public class BitmapInvertedIndexCreator implements InvertedIndexCreator {

  private static final Logger logger = Logger.getLogger(BitmapInvertedIndexCreator.class);

  private final File invertedIndexFile;
  private final FieldSpec spec;
  private final MutableRoaringBitmap[] invertedIndex;
  long start = 0;

  public BitmapInvertedIndexCreator(File indexDir, DictionaryCreator dictionaryCreator, FieldSpec spec) {
    this.spec = spec;
    invertedIndexFile = new File(indexDir, spec.getName() + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    invertedIndex = new MutableRoaringBitmap[dictionaryCreator.getDictionarySize()];
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
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(invertedIndexFile)));
    // First, write out offsets of bitmaps. The information can be used to access a certain bitmap directly.
    // Totally (invertedIndex.length+1) offsets will be written out; the last offset is used to calculate the length of
    // the last bitmap, which might be needed when accessing bitmaps randomly.
    // If a bitmap's offset is k, then k bytes need to be skipped to reach the bitmap.
    int offset = 4 * (invertedIndex.length + 1); // The first bitmap's offset
    out.writeInt(offset);
    for (int k = 0; k < invertedIndex.length; ++k) { // the other bitmap's offset
      offset += invertedIndex[k].serializedSizeInBytes();
      out.writeInt(offset);
    }
    // write out bitmaps one by one
    for (int k = 0; k < invertedIndex.length; ++k) {
      invertedIndex[k].serialize(out);
    }
    out.close();
    logger.info("persisted bitmap inverted index for column : " + spec.getName() + " in "
        + invertedIndexFile.getAbsolutePath());
  }

}
