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
package com.linkedin.pinot.core.segment.creator.impl.fwd;

import java.io.Closeable;
import java.io.File;

import org.apache.commons.io.FileUtils;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.index.writer.impl.FixedBitWidthRowColDataFileWriter;
import com.linkedin.pinot.core.segment.creator.SingleValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


public class SingleValueUnsortedForwardIndexCreator implements Closeable, SingleValueForwardIndexCreator {

  private final File forwardIndexFile;
  private final FieldSpec spec;
  private int maxNumberOfBits = 0;
  private FixedBitWidthRowColDataFileWriter sVWriter;

  public SingleValueUnsortedForwardIndexCreator(FieldSpec spec, File baseIndexDir, int cardinality, int numDocs,
      int totalNumberOfValues, boolean hasNulls) throws Exception {
    forwardIndexFile = new File(baseIndexDir, spec.getName() + V1Constants.Indexes.UN_SORTED_SV_FWD_IDX_FILE_EXTENTION);
    this.spec = spec;
    FileUtils.touch(forwardIndexFile);
    maxNumberOfBits = getNumOfBits(cardinality);
    sVWriter =
        new FixedBitWidthRowColDataFileWriter(forwardIndexFile, numDocs, 1, new int[] { maxNumberOfBits },
            new boolean[] { hasNulls });
  }

  public static int getNumOfBits(int dictionarySize) {
    if (dictionarySize < 2) {
      return 1;
    }
    int ret = (int) Math.ceil(Math.log(dictionarySize) / Math.log(2));
    if (ret == 0) {
      ret = 1;
    }
    return ret;
  }

  @Override
  public void index(int docId, int dictionaryIndex) {
    sVWriter.setInt(docId, 0, dictionaryIndex);
  }

  @Override
  public void close() {
    sVWriter.close();
  }
}
