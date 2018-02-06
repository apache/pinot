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
package com.linkedin.pinot.core.segment.creator.impl.fwd;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.io.writer.SingleColumnSingleValueWriter;
import com.linkedin.pinot.core.io.writer.impl.v1.FixedBitSingleValueWriter;
import com.linkedin.pinot.core.segment.creator.SingleValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;


public class SingleValueUnsortedForwardIndexCreator implements SingleValueForwardIndexCreator {
  private final File forwardIndexFile;
  private final FieldSpec spec;
  private int maxNumberOfBits = 0;
  private SingleColumnSingleValueWriter sVWriter;

  public SingleValueUnsortedForwardIndexCreator(FieldSpec spec, File baseIndexDir, int cardinality, int numDocs,
      int totalNumberOfValues, boolean hasNulls) throws Exception {
    forwardIndexFile =
        new File(baseIndexDir, spec.getName() + V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
    this.spec = spec;
    FileUtils.touch(forwardIndexFile);
    maxNumberOfBits = getNumOfBits(cardinality);
    sVWriter = new FixedBitSingleValueWriter(forwardIndexFile, numDocs, maxNumberOfBits);
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
    sVWriter.setInt(docId, dictionaryIndex);
  }

  @Override
  public void close() throws IOException {
    sVWriter.close();
  }
}
