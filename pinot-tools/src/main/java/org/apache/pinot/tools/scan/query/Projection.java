/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.scan.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.utils.Pair;


@SuppressWarnings({"rawtypes", "unchecked"})
public class Projection {
  private final ImmutableSegment _immutableSegment;
  private final SegmentMetadataImpl _metadata;
  private final List<Integer> _filteredDocIds;
  private final List<Pair> _columnList;
  private final Set<String> _mvColumns;
  private final Map<String, int[]> _mvColumnArrayMap;
  private Map<String, Dictionary> _dictionaryMap;
  private boolean _addCountStar;
  private int _limit = 10;

  Projection(ImmutableSegment immutableSegment, SegmentMetadataImpl metadata, List<Integer> filteredDocIds,
      List<Pair> columns, Map<String, Dictionary> dictionaryMap, boolean addCountStar) {
    _immutableSegment = immutableSegment;
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
      ForwardIndexReader reader = _immutableSegment.getDataSource(column).getForwardIndex();
      try (ForwardIndexReaderContext readerContext = reader.createContext()) {
        if (!_mvColumns.contains(column)) {
          int rowId = 0;
          for (int docId : _filteredDocIds) {
            resultTable.add(rowId++, reader.getDictId(docId, readerContext));
          }
        } else {
          int rowId = 0;
          for (int docId : _filteredDocIds) {
            int[] dictIdBuffer = _mvColumnArrayMap.get(column);
            int numMVValues = reader.getDictIdMV(docId, dictIdBuffer, readerContext);
            int[] dictIds = Arrays.copyOf(dictIdBuffer, numMVValues);
            resultTable.add(rowId++, dictIds);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
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

        if (object instanceof int[]) {
          int[] dictIds = (int[]) object;
          Object[] values = new Object[dictIds.length];
          for (int i = 0; i < dictIds.length; ++i) {
            values[i] = dictionary.get(dictIds[i]);
          }
          row.set(colId, values);
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
