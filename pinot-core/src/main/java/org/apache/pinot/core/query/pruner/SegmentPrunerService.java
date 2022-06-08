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
import org.apache.pinot.core.query.config.SegmentPrunerConfig;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.trace.InvocationScope;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>SegmentPrunerService</code> class contains multiple segment pruners and provides service to prune segments
 * against all pruners.
 */
public class SegmentPrunerService {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPrunerService.class);

  private final List<SegmentPruner> _segmentPruners;

  public SegmentPrunerService(SegmentPrunerConfig config) {
    int numPruners = config.numSegmentPruners();
    _segmentPruners = new ArrayList<>(numPruners);

    for (int i = 0; i < numPruners; i++) {
      String segmentPrunerName = config.getSegmentPrunerName(i);
      LOGGER.info("Adding segment pruner: {}", segmentPrunerName);
      SegmentPruner pruner = SegmentPrunerProvider.getSegmentPruner(segmentPrunerName,
          config.getSegmentPrunerConfig(i));
      if (pruner != null) {
        _segmentPruners.add(pruner);
      } else {
        LOGGER.warn("could not create segment pruner: {}", segmentPrunerName);
      }
    }
  }

  /**
   * Prunes the segments based on the query request, returns the segments that are not pruned.
   */
  public List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query) {
    try (InvocationScope scope = Tracing.getTracer().createScope(SegmentPrunerService.class)) {
      segments = removeInvalidSegments(segments, query);
      int invokedPrunersCount = 0;
      for (SegmentPruner segmentPruner : _segmentPruners) {
        if (segmentPruner.isApplicableTo(query)) {
          invokedPrunersCount++;
          try (InvocationScope prunerScope = Tracing.getTracer().createScope(segmentPruner.getClass())) {
            prunerScope.setNumSegments(segments.size());
            segments = segmentPruner.prune(segments, query);
          }
        }
      }
      scope.setNumChildren(invokedPrunersCount);
    }
    return segments;
  }

  private static List<IndexSegment> removeInvalidSegments(List<IndexSegment> segments, QueryContext query) {
    int selected = 0;
    for (IndexSegment segment : segments) {
      if (!isInvalidSegment(segment, query)) {
        segments.set(selected++, segment);
      }
    }
    return segments.subList(0, selected);
  }

  private static boolean isInvalidSegment(IndexSegment segment, QueryContext query) {
    return segment.getSegmentMetadata().getTotalDocs() == 0
        || !segment.getColumnNames().containsAll(query.getColumns());
  }
}
