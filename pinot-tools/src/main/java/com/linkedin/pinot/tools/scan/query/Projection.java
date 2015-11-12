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
package com.linkedin.pinot.tools.scan.query;

import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;


public class Projection {
  private final IndexSegmentImpl _indexSegment;
  private final SegmentMetadataImpl _metadata;
  private final List<Integer> _filteredDocIds;
  private final List<String> _columns;

  Projection(IndexSegmentImpl indexSegment, SegmentMetadataImpl metadata, List<Integer> filteredDocIds,
      List<String> columns) {

    _indexSegment = indexSegment;
    _metadata = metadata;
    _filteredDocIds = filteredDocIds;
    _columns = columns;
  }

  public ResultTable run() {
    ResultTable resultTable = new ResultTable(_columns, _filteredDocIds.size());

    for (String column : _columns) {
      if (_metadata.getColumnMetadataFor(column).isSingleValue()) {
        BlockSingleValIterator bvIter =
            (BlockSingleValIterator) _indexSegment.getDataSource(column).getNextBlock().getBlockValueSet().iterator();

        int rowId = 0;
        for (Integer docId : _filteredDocIds) {
          bvIter.skipTo(docId);
          resultTable.add(rowId++, bvIter.nextIntVal());
        }
      } else {
        BlockMultiValIterator bvIter =
            (BlockMultiValIterator) _indexSegment.getDataSource(column).getNextBlock().getBlockValueSet().iterator();

        int rowId = 0;
        for (Integer docId : _filteredDocIds) {
          bvIter.skipTo(docId);
          int maxNumMultiValues = _metadata.getColumnMetadataFor(column).getMaxNumberOfMultiValues();
          int[] dictIds = new int[maxNumMultiValues];

          int numMVValues = bvIter.nextIntVal(dictIds);
          dictIds = Arrays.copyOf(dictIds, numMVValues);

          resultTable.add(rowId++, ArrayUtils.toObject(dictIds));
        }
      }
    }

    return resultTable;
  }
}
