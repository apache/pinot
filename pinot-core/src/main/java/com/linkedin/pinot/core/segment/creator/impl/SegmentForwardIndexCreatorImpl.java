package com.linkedin.pinot.core.segment.creator.impl;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.index.writer.impl.FixedBitWidthRowColDataFileWriter;
import com.linkedin.pinot.core.index.writer.impl.FixedBitWidthSingleColumnMultiValueWriter;

public class SegmentForwardIndexCreatorImpl {

  private final File forwardIndexFile;
  private final FieldSpec spec;
  private int maxNumberOfBits = 0;
  private FixedBitWidthRowColDataFileWriter sVWriter;
  private FixedBitWidthSingleColumnMultiValueWriter mVWriter;
  private int numDocs = 0;
  private final int totalNumberOfValues;
  private int rowIndex = 0;

  public SegmentForwardIndexCreatorImpl(FieldSpec spec, File baseIndexDir, int cardinality, int numDocs, int totalNumberOfValues) throws Exception {
    forwardIndexFile = new File(baseIndexDir, spec.getName() + V1Constants.Indexes.UN_SORTED_FWD_IDX_FILE_EXTENTION);
    this.spec = spec;
    FileUtils.touch(forwardIndexFile);
    maxNumberOfBits = getNumOfBits(cardinality);
    this.numDocs = numDocs;
    this.totalNumberOfValues = totalNumberOfValues;
    if (spec.isSingleValueField()) {
      sVWriter = new FixedBitWidthRowColDataFileWriter(forwardIndexFile, numDocs, 1, new int[] { maxNumberOfBits });
    } else {
      mVWriter = new FixedBitWidthSingleColumnMultiValueWriter(forwardIndexFile, numDocs, totalNumberOfValues, maxNumberOfBits);
    }
  }

  public static int getNumOfBits(int dictionarySize) {
    int ret = (int) Math.ceil(Math.log(dictionarySize) / Math.log(2));
    if (ret == 0) {
      ret = 1;
    }
    return ret;
  }

  public void index(Object e) {
    if (spec.isSingleValueField()) {
      final int entry = ((Integer)e).intValue();
      indexSingleValue(entry);
      return;
    }

    final Object [] entryArr = ((Object[])e);
    Arrays.sort(entryArr);

    final int[] entries = new int[entryArr.length];

    for (int i = 0; i < entryArr.length; i++) {
      entries[i] = ((Integer)entryArr[i]).intValue();
    }

    indexMultiValue(entries);
  }

  private void indexSingleValue(int entry) {
    sVWriter.setInt(rowIndex++, 0, entry);
  }

  private void indexMultiValue(int[] entries) {
    mVWriter.setIntArray(rowIndex++, entries);
  }

  public void close() {
    if (spec.isSingleValueField()) {
      sVWriter.saveAndClose();
    }else {
      mVWriter.close();
    }
  }
}
