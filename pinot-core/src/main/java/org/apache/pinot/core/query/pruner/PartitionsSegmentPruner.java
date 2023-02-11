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
package org.apache.pinot.core.query.pruner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * The {@code PartitionSegmentPruner} is the segment pruner that prunes segments based on the partition info from the
 * QueryContext.
 */
public class PartitionsSegmentPruner implements SegmentPruner {

  @Override
  public void init(PinotConfiguration config) {
  }

  @Override
  public boolean isApplicableTo(QueryContext query) {
    return query.getColumnPartitionMap() != null && !query.getColumnPartitionMap().isEmpty();
  }

  @Override
  public List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query) {
    if (segments.isEmpty() || !isApplicableTo(query)) {
      return segments;
    }
    Map<String, Set<Integer>> queryColumnPartitionMap = query.getColumnPartitionMap();
    int numSegments = segments.size();
    List<IndexSegment> selectedSegments = new ArrayList<>(numSegments);
    for (IndexSegment segment : segments) {
      SegmentMetadata segmentMetadata = segment.getSegmentMetadata();
      boolean selected = false;
      for (String column : queryColumnPartitionMap.keySet()) {
        ColumnMetadata columnMetadataFor = segmentMetadata.getColumnMetadataFor(column);
        if (columnMetadataFor == null) {
          continue;
        }
        Set<Integer> segmentPartitions = columnMetadataFor.getPartitions();
        if (segmentPartitions != null && !segmentPartitions.isEmpty()) {
          for (int queryPartition : queryColumnPartitionMap.get(column)) {
            if (segmentPartitions.contains(queryPartition)) {
              selected = true;
              break;
            }
          }
          if (selected) {
            break;
          }
        }
      }
      if (selected) {
        selectedSegments.add(segment);
      }
    }
    return selectedSegments;
  }
}
