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

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.util.List;


public class AggregationGroupBy {
  private final IndexSegmentImpl _indexSegment;
  private final SegmentMetadataImpl _metadata;
  private final List<Integer> _filteredDocIds;
  private final List<AggregationInfo> _aggregationsInfo;

  public AggregationGroupBy(IndexSegmentImpl indexSegment, SegmentMetadataImpl metadata, List<Integer> filteredDocIds,
      List<AggregationInfo> aggregationsInfo) {

    _indexSegment = indexSegment;
    _metadata = metadata;
    _filteredDocIds = filteredDocIds;
    _aggregationsInfo = aggregationsInfo;
  }

  public ResultTable run() {
    return null;
  }
}
