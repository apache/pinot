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
package com.linkedin.pinot.tools.scan.query;

import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.ArrayUtils;


public class Projection {
  private final IndexSegmentImpl _indexSegment;
  private final SegmentMetadataImpl _metadata;
  private final List<Integer> _filteredDocIds;
  private final List<Pair> _columnList;
  private final Set<String> _mvColumns;
  private final Map<String, int[]> _mvColumnArrayMap;
  private Map<String, Dictionary> _dictionaryMap;
  private boolean _addCountStar;
  private int _limit = 10;

  Projection(IndexSegmentImpl indexSegment, SegmentMetadataImpl metadata, List<Integer> filteredDocIds,
      List<Pair> columns, Map<String, Dictionary> dictionaryMap, boolean addCountStar) {

    _indexSegment = indexSegment;
    _metadata = metadata;
    _filteredDocIds = filteredDocIds;
    _dictionaryMap = dictionaryMap;
    _addCountStar = addCountStar;

    _columnList = new ArrayList<>();
    for (Pair pair : columns) {
      _columnList.add(pair);
    }

    _mvColumns = new HashSet<>();
    _mvColumnArrayMap = new HashMap<>();

    for (ColumnMetadata columnMetadata : _metadata.getColumnMetadataMap().values()) {
      String column = columnMetadata.getColumnName();

      if (!columnMetadata.isSingleValue()) {
        _mvColumns.add(column);
      }
      _mvColumnArrayMap.put(column, new int[columnMetadata.getMaxNumberOfMultiValues()]);
    }
  }

  public ResultTable run() {
    ResultTable resultTable = new ResultTable(_columnList, _filteredDocIds.size());
    resultTable.setResultType(ResultTable.ResultType.Selection);

    for (Pair pair : _columnList) {
      String column = (String) pair.getFirst();
      if (!_mvColumns.contains(column)) {
        BlockSingleValIterator bvIter =
            (BlockSingleValIterator) _indexSegment.getDataSource(column).nextBlock().getBlockValueSet().iterator();

        int rowId = 0;
        for (Integer docId : _filteredDocIds) {
          bvIter.skipTo(docId);
          resultTable.add(rowId++, bvIter.nextIntVal());
        }
      } else {
        BlockMultiValIterator bvIter =
            (BlockMultiValIterator) _indexSegment.getDataSource(column).nextBlock().getBlockValueSet().iterator();

        int rowId = 0;
        for (Integer docId : _filteredDocIds) {
          bvIter.skipTo(docId);
          int[] dictIds = _mvColumnArrayMap.get(column);
          int numMVValues = bvIter.nextIntVal(dictIds);

          dictIds = Arrays.copyOf(dictIds, numMVValues);
          resultTable.add(rowId++, ArrayUtils.toObject(dictIds));
        }
      }
    }

    return transformFromIdToValues(resultTable, _dictionaryMap, _addCountStar);
  }

  public ResultTable transformFromIdToValues(ResultTable resultTable, Map<String, Dictionary> dictionaryMap,
      boolean addCountStar) {
    List<Pair> columnList = resultTable.getColumnList();

    for (ResultTable.Row row : resultTable) {
      int colId = 0;
      for (Object object : row) {
        String column = (String) columnList.get(colId).getFirst();
        Dictionary dictionary = dictionaryMap.get(column);

        if (object instanceof Object[]) {
          Object[] objArray = (Object[]) object;
          Object[] valArray = new Object[objArray.length];

          for (int i = 0; i < objArray.length; ++i) {
            int dictId = (int) objArray[i];
            valArray[i] = dictionary.get(dictId);
          }
          row.set(colId, valArray);
        } else {
          int dictId = (int) object;
          row.set(colId, dictionary.get(dictId));
        }
        ++colId;
      }
    }

    // Add additional column for count(*)
    if (addCountStar) {
      for (ResultTable.Row row : resultTable) {
        row.add(1);
      }
      resultTable.addCountStarColumn();
    }

    return resultTable;
  }
}
