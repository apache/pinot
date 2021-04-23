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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.utils.Pair;


public class Selection {
  private final ImmutableSegment _immutableSegment;
  private final SegmentMetadataImpl _metadata;
  private final List<Integer> _filteredDocIds;
  private final List<Pair> _selectionColumns;

  public Selection(ImmutableSegment immutableSegment, SegmentMetadataImpl metadata, List<Integer> filteredDocIds,
      List<Pair> selectionColumns) {

    _immutableSegment = immutableSegment;
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
      dictionaryMap.put(column, _immutableSegment.getDictionary(column));
    }

    Projection projection =
        new Projection(_immutableSegment, _metadata, _filteredDocIds, _selectionColumns, dictionaryMap, addCountStar);

    return projection.run();
  }
}
