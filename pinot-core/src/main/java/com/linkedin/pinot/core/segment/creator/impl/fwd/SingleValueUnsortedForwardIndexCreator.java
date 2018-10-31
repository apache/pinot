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
import com.linkedin.pinot.core.io.writer.SingleColumnSingleValueWriter;
import com.linkedin.pinot.core.io.writer.impl.v1.FixedBitSingleValueWriter;
import com.linkedin.pinot.core.segment.creator.SingleValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.File;
import java.io.IOException;


public class SingleValueUnsortedForwardIndexCreator implements SingleValueForwardIndexCreator {
  private final SingleColumnSingleValueWriter _writer;

  public SingleValueUnsortedForwardIndexCreator(File outputDir, String column, int cardinality, int numDocs)
      throws Exception {
    File indexFile = new File(outputDir, column + V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
    _writer = new FixedBitSingleValueWriter(indexFile, numDocs, PinotDataBitSet.getNumBitsPerValue(cardinality - 1));
  }

  @Override
  public void index(int docId, int dictId) {
    _writer.setInt(docId, dictId);
  }

  @Override
  public void close() throws IOException {
    _writer.close();
  }
}
