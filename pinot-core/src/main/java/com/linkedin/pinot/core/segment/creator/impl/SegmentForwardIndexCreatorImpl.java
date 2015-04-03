/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.creator.impl;

import java.io.Closeable;
import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.index.writer.impl.FixedBitWidthRowColDataFileWriter;
import com.linkedin.pinot.core.index.writer.impl.FixedBitWidthSingleColumnMultiValueWriter;

public class SegmentForwardIndexCreatorImpl implements Closeable {

  private final File forwardIndexFile;
  private final FieldSpec spec;
  private int maxNumberOfBits = 0;
  private FixedBitWidthRowColDataFileWriter sVWriter;
  private FixedBitWidthSingleColumnMultiValueWriter mVWriter;
  private int numDocs = 0;
  private final int totalNumberOfValues;
  private int rowIndex = 0;

  public SegmentForwardIndexCreatorImpl(FieldSpec spec, File baseIndexDir, int cardinality, int numDocs, int totalNumberOfValues, boolean hasNulls) throws Exception {
    forwardIndexFile = new File(baseIndexDir, spec.getName() + V1Constants.Indexes.UN_SORTED_FWD_IDX_FILE_EXTENTION);
    this.spec = spec;
    FileUtils.touch(forwardIndexFile);
    maxNumberOfBits = getNumOfBits(cardinality);
    this.numDocs = numDocs;
    this.totalNumberOfValues = totalNumberOfValues;
    if (spec.isSingleValueField()) {
      sVWriter = new FixedBitWidthRowColDataFileWriter(forwardIndexFile, numDocs, 1, new int[] { maxNumberOfBits }, new boolean[] { hasNulls });
    } else {
      mVWriter = new FixedBitWidthSingleColumnMultiValueWriter(forwardIndexFile, numDocs, totalNumberOfValues, maxNumberOfBits);
    }
  }

  public static int getNumOfBits(int dictionarySize) {
    if (dictionarySize < 2)
      return 1;
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
      sVWriter.close();
    }else {
      mVWriter.close();
    }
  }
}
