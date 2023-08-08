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
package org.apache.pinot.segment.spi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.segment.spi.index.IndexType;


/**
 * The context for fetching buffers of a segment during query.
 */
public class FetchContext {
  private final UUID _fetchId;
  private final String _segmentName;
  private final Map<String, List<IndexType<?, ?, ?>>> _columnToIndexList;

  /**
   * Create a new FetchRequest for this segment, to fetch all buffers of the given columns
   * @param fetchId unique fetch id
   * @param segmentName segment name
   * @param columns set of columns to fetch
   */
  public FetchContext(UUID fetchId, String segmentName, Set<String> columns) {
    _fetchId = fetchId;
    _segmentName = segmentName;
    _columnToIndexList = new HashMap<>();
    for (String column : columns) {
      _columnToIndexList.put(column, null);
    }
  }

  /**
   * Create a new FetchRequest for this segment, to fetch those indexes as mentioned in the column to indexes list map
   * @param fetchId unique fetch id
   * @param segmentName segment name
   * @param columnToIndexList map of column names as key, and list of indexes to fetch as values
   */
  public FetchContext(UUID fetchId, String segmentName, Map<String, List<IndexType<?, ?, ?>>> columnToIndexList) {
    _fetchId = fetchId;
    _segmentName = segmentName;
    _columnToIndexList = columnToIndexList;
  }

  /**
   * An id to uniquely identify the fetch request
   * @return unique uuid
   */
  public UUID getFetchId() {
    return _fetchId;
  }

  /**
   * Segment name associated with this fetch context
   */
  public String getSegmentName() {
    return _segmentName;
  }

  /**
   * Map of columns to fetch as key, and the list of indexes to fetch for the column as value
   * The list of indexes can be null, which indicates that every index for this column should be fetched
   */
  public Map<String, List<IndexType<?, ?, ?>>> getColumnToIndexList() {
    return _columnToIndexList;
  }

  public boolean isEmpty() {
    return _columnToIndexList.isEmpty();
  }
}
