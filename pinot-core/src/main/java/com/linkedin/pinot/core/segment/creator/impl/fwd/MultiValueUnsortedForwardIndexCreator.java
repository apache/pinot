package com.linkedin.pinot.core.segment.creator.impl.fwd;

import java.io.Closeable;
import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.index.writer.impl.FixedBitWidthSingleColumnMultiValueWriter;
import com.linkedin.pinot.core.segment.creator.ForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


public class MultiValueUnsortedForwardIndexCreator implements ForwardIndexCreator, Closeable {

  private final File forwardIndexFile;
  private final FieldSpec spec;
  private int maxNumberOfBits = 0;
  private FixedBitWidthSingleColumnMultiValueWriter mVWriter;

  public MultiValueUnsortedForwardIndexCreator(FieldSpec spec, File baseIndexDir, int cardinality, int numDocs,
      int totalNumberOfValues, boolean hasNulls) throws Exception {

    forwardIndexFile = new File(baseIndexDir, spec.getName() + V1Constants.Indexes.UN_SORTED_MV_FWD_IDX_FILE_EXTENTION);
    this.spec = spec;
    FileUtils.touch(forwardIndexFile);
    maxNumberOfBits = SingleValueUnsortedForwardIndexCreator.getNumOfBits(cardinality);
    mVWriter =
        new FixedBitWidthSingleColumnMultiValueWriter(forwardIndexFile, numDocs, totalNumberOfValues, maxNumberOfBits);
  }

  @Override
  public void index(int docId, Object e) {
    final Object[] entryArr = ((Object[]) e);
    Arrays.sort(entryArr);

    final int[] entries = new int[entryArr.length];

    for (int i = 0; i < entryArr.length; i++) {
      entries[i] = ((Integer) entryArr[i]).intValue();
    }
    mVWriter.setIntArray(docId, entries);
  }

  @Override
  public void close() {
    mVWriter.close();
  }

}
