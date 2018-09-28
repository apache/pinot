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

import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.creator.SingleValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;


public class SingleValueSortedForwardIndexCreator implements SingleValueForwardIndexCreator {
  private final FixedByteSingleValueMultiColWriter _writer;
  private final int[] _minDocIds;
  private final int[] _maxDocIds;

  public SingleValueSortedForwardIndexCreator(File outputDir, String column, int cardinality) throws Exception {
    File indexFile = new File(outputDir, column + V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
    _writer =
        new FixedByteSingleValueMultiColWriter(indexFile, cardinality, 2, new int[]{Integer.BYTES, Integer.BYTES});
    _minDocIds = new int[cardinality];
    _maxDocIds = new int[cardinality];
    Arrays.fill(_minDocIds, Integer.MAX_VALUE);
    Arrays.fill(_maxDocIds, Integer.MIN_VALUE);
  }

  @Override
  public void index(int docId, int dictId) {
    if (_minDocIds[dictId] > docId) {
      _minDocIds[dictId] = docId;
    }
    if (_maxDocIds[dictId] < docId) {
      _maxDocIds[dictId] = docId;
    }
  }

  @Override
  public void close() throws IOException {
    int cardinality = _maxDocIds.length;
    for (int i = 0; i < cardinality; i++) {
      _writer.setInt(i, 0, _minDocIds[i]);
      _writer.setInt(i, 1, _maxDocIds[i]);
    }
    _writer.close();
  }
}
