package com.linkedin.pinot.core.indexsegment.columnar.creator;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.differential.IntegratedBinaryPacking;
import me.lemire.integercompression.differential.IntegratedComposition;
import me.lemire.integercompression.differential.IntegratedIntegerCODEC;
import me.lemire.integercompression.differential.IntegratedVariableByte;

import org.apache.log4j.Logger;

import com.linkedin.pinot.common.data.FieldSpec;

public class IntArrayInvertedIndexCreator implements InvertedIndexCreator {
  private static final Logger logger = Logger.getLogger(IntArrayInvertedIndexCreator.class);

  private static final int LIST_DEFAULT_CAPACITY = 1024;
  private final File invertedIndexFile;
  private final FieldSpec spec;
  final int[][] noCompressList;
  final int[] listSizes;
  final IntegratedIntegerCODEC codec;

  public IntArrayInvertedIndexCreator(File indexDir, DictionaryCreator dictionaryCreator, FieldSpec spec) {
    this.spec = spec;
    invertedIndexFile = new File(indexDir, spec.getName() + V1Constants.Indexes.INTARRAY_INVERTED_INDEX_FILE_EXTENSION);
    int size = dictionaryCreator.getDictionarySize();
    listSizes = new int[size];
    noCompressList = new int[size][];
    for (int i = 0; i < size; ++i) {
      noCompressList[i] = new int[LIST_DEFAULT_CAPACITY];
    }
    codec = new IntegratedComposition(new IntegratedBinaryPacking(), new IntegratedVariableByte());
  }

  @Override
  public void add(int dictionaryId, int docId) {
    if (listSizes[dictionaryId] >= noCompressList[dictionaryId].length) { // if the list is full, increase its capacity
      noCompressList[dictionaryId] = resize(noCompressList[dictionaryId]);
    }
    noCompressList[dictionaryId][listSizes[dictionaryId]] = docId;
    ++listSizes[dictionaryId];
  }

  /**
   * Returns a new array with doubled capacity and the original data in the specified array.
   * @param array the array to expand.
   * @return a new array with doubled capacity and the original data in the specified array.
   */
  protected int[] resize(int[] array) {
    return Arrays.copyOf(array, array.length << 1);
  }

  @Override
  public void seal() throws IOException {
    if (noCompressList.length == 0) { // we don't need to do anything if this is an empty inverted index.
      return;
    }
    for (int i = 0; i < noCompressList.length; ++i) {
      Arrays.sort(noCompressList[i], 0, listSizes[i]);
    }
    final int[][] compressedLists = compressLists();
    writeToFile(compressedLists);
  }

  private int[][] compressLists() {
    final int[][] compressedLists = new int[noCompressList.length][];
    int maxCapacity = 0;
    for (int listSize : listSizes) {
      maxCapacity = Math.max(maxCapacity, listSize);
    }

    int[] compressionBuffer = new int[maxCapacity + 1024];
    boolean triedOnce = false;
    for (int i = 0; i < noCompressList.length; ++i) {
      try {
        compressedLists[i] = compressList(noCompressList[i], listSizes[i], compressionBuffer);
        triedOnce = false;
      } catch (ArrayIndexOutOfBoundsException e) {
        if (!triedOnce) {
          // buffer is not large enough; expand buffer and try again.
          compressionBuffer = new int[compressionBuffer.length * 2];
          --i;
          triedOnce = true;
        } else {
          throw (e);
        }
      }
    }
    return compressedLists;
  }

  private int[] compressList(int[] input, int inputSize, int[] buffer)
  {
    final IntWrapper inputoffset = new IntWrapper(0);
    final IntWrapper outputoffset = new IntWrapper(0);
    codec.compress(input, inputoffset, inputSize, buffer, outputoffset);
    return Arrays.copyOf(buffer, outputoffset.get());
  }

  private void writeToFile(int[][] compressedLists) throws IOException {
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(invertedIndexFile)));
    // The field of offsets and list sizes (i.e., number of integers after decompression) looks list this:
    //   start offset of compressed list 1
    //   start offset of compressed list 2
    //   ...
    //   start offset of compressed list n
    //    END  offset of compressed list n
    //   data size of uncompressed list 1
    //   data size of uncompressed list 2
    //   ...
    //   data size of uncompressed list n
    int offset = (4 * (noCompressList.length+1)) + (4 * noCompressList.length);
    out.writeInt(offset);
    for (int i = 0; i < noCompressList.length; ++i) {
      offset += compressedLists[i].length * 4; // note that the offset is in byte
      out.writeInt(offset);
    }
    for (int i = 0; i < noCompressList.length; ++i) {
      out.writeInt(listSizes[i]); // write out data size of uncompressed list
    }
    // write out lists one by one
    for (int i = 0; i < compressedLists.length; ++i) {
      for (int j = 0; j < compressedLists[i].length; ++j) {
        out.writeInt(compressedLists[i][j]);
      }
    }
    out.close();
    logger.info("persisted CompressedIntArray inverted index for column : " + spec.getName() + " in " +
        invertedIndexFile.getAbsolutePath());
  }

  @Override
  public long totalTimeTakeSoFar() {
    // TODO Auto-generated method stub
    return 0;
  }
}
