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

import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Selection {
  private final IndexSegmentImpl _indexSegment;
  private final SegmentMetadataImpl _metadata;
  private final List<Integer> _filteredDocIds;
  private final List<Pair> _selectionColumns;

  public Selection(IndexSegmentImpl indexSegment, SegmentMetadataImpl metadata, List<Integer> filteredDocIds,
      List<Pair> selectionColumns) {

    _indexSegment = indexSegment;
    _metadata = metadata;
    _filteredDocIds = filteredDocIds;
    _selectionColumns = selectionColumns;
  }

  public ResultTable run() {
    boolean addCountStar = false;
    Map<String, Dictionary> dictionaryMap = new HashMap<>();

    for (Pair pair : _selectionColumns) {
      String column = (String) pair.getFirst();
      if (column.equals("*")) {
        addCountStar = true;
      }
      dictionaryMap.put(column, _indexSegment.getDictionaryFor(column));
    }

    Projection projection =
        new Projection(_indexSegment, _metadata, _filteredDocIds, _selectionColumns, dictionaryMap, addCountStar);

    return projection.run();
  }
}
