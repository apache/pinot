/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.io.util.PinotDataBitSet;
import com.linkedin.pinot.core.io.writer.SingleColumnMultiValueWriter;
import com.linkedin.pinot.core.io.writer.impl.v1.FixedBitMultiValueWriter;
import com.linkedin.pinot.core.segment.creator.MultiValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.File;
import java.io.IOException;


public class MultiValueUnsortedForwardIndexCreator implements MultiValueForwardIndexCreator {
  private final SingleColumnMultiValueWriter _writer;

  public MultiValueUnsortedForwardIndexCreator(File outputDir, String column, int cardinality, int numDocs,
      int totalNumValues) throws Exception {
    File indexFile = new File(outputDir, column + V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
    _writer = new FixedBitMultiValueWriter(indexFile, numDocs, totalNumValues,
        PinotDataBitSet.getNumBitsPerValue(cardinality - 1));
  }

  @Override
  public void index(int docId, int[] dictIds) {
    _writer.setIntArray(docId, dictIds);
  }

  @Override
  public void close() throws IOException {
    _writer.close();
  }
}
