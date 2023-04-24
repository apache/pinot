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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
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
  private final Map<SegmentPruner, BiConsumer<SegmentPrunerStatistics, Integer>> _prunerStatsUpdaters;

  public SegmentPrunerService(SegmentPrunerConfig config) {
    int numPruners = config.numSegmentPruners();
    _prunerStatsUpdaters = new HashMap<>();
    _segmentPruners = new ArrayList<>(numPruners);

    for (int i = 0; i < numPruners; i++) {
      String segmentPrunerName = config.getSegmentPrunerName(i);
      LOGGER.info("Adding segment pruner: {}", segmentPrunerName);
      SegmentPruner pruner = SegmentPrunerProvider.getSegmentPruner(segmentPrunerName,
          config.getSegmentPrunerConfig(i));
      if (pruner != null) {
        _segmentPruners.add(pruner);
        switch (segmentPrunerName.toLowerCase()) {
          case SegmentPrunerProvider.SELECTION_QUERY_SEGMENT_PRUNER_NAME:
            _prunerStatsUpdaters.put(pruner, SegmentPrunerStatistics::setLimitPruned);
            break;
          case SegmentPrunerProvider.COLUMN_VALUE_SEGMENT_PRUNER_NAME:
          case SegmentPrunerProvider.BLOOM_FILTER_SEGMENT_PRUNER_NAME:
            _prunerStatsUpdaters.put(pruner, SegmentPrunerStatistics::setValuePruned);
            break;
          default:
            _prunerStatsUpdaters.put(pruner, (stats, value) -> { });
            break;
        }
      } else {
        LOGGER.warn("could not create segment pruner: {}", segmentPrunerName);
      }
    }
    assert _segmentPruners.stream()
        .allMatch(_prunerStatsUpdaters::containsKey)
        : "No defined stats updater for pruner " + _segmentPruners.stream()
        .filter(p -> !_prunerStatsUpdaters.containsKey(p))
        .findAny().orElseThrow(IllegalStateException::new);
  }

  /**
   * Prunes the segments based on the query request, returns the segments that are not pruned.
   *
   * @deprecated this method is here for compatibility reasons and may be removed soon.
   * Call {@link #prune(List, QueryContext, SegmentPrunerStatistics)} instead
   * @param segments the list of segments to be pruned. This is a destructive operation that may modify this list in an
   *                 undefined way. Therefore, this list should not be used after calling this method.
   */
  @Deprecated
  public List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query) {
    return prune(segments, query, new SegmentPrunerStatistics());
  }

  /**
   * Prunes the segments based on the query request, returns the segments that are not pruned.
   *
   * @param segments the list of segments to be pruned. This is a destructive operation that may modify this list in an
   *                 undefined way. Therefore, this list should not be used after calling this method.
   */
  public List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query, SegmentPrunerStatistics stats) {
    return prune(segments, query, stats, null);
  }

  public List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query, SegmentPrunerStatistics stats,
      @Nullable ExecutorService executorService) {
    try (InvocationScope scope = Tracing.getTracer().createScope(SegmentPrunerService.class)) {
      segments = removeInvalidSegments(segments, query, stats);
      int invokedPrunersCount = 0;
      for (SegmentPruner segmentPruner : _segmentPruners) {
        if (segmentPruner.isApplicableTo(query)) {
          invokedPrunersCount++;
          try (InvocationScope prunerScope = Tracing.getTracer().createScope(segmentPruner.getClass())) {
            int originalSegmentsSize = segments.size();
            prunerScope.setNumSegments(originalSegmentsSize);
            segments = segmentPruner.prune(segments, query, executorService);
            _prunerStatsUpdaters.get(segmentPruner).accept(stats, originalSegmentsSize - segments.size());
          }
        }
      }
      scope.setNumChildren(invokedPrunersCount);
    }
    return segments;
  }

  /**
   * Filters the given list, returning a list that only contains the valid segments, modifying the list received as
   * argument.
   *
   * <p>
   * This is a destructive operation. The list received as arguments may be modified, so only the returned list should
   * be used.
   * </p>
   *
   * @param segments the list of segments to be pruned. This is a destructive operation that may modify this list in an
   *                 undefined way. Therefore, this list should not be used after calling this method.
   * @return the new list with filtered elements. This is the list that have to be used.
   */
  private static List<IndexSegment> removeInvalidSegments(List<IndexSegment> segments, QueryContext query,
      SegmentPrunerStatistics stats) {
    int selected = 0;
    int invalid = 0;
    for (IndexSegment segment : segments) {
      if (!isEmptySegment(segment)) {
        if (isInvalidSegment(segment, query)) {
          invalid++;
        } else {
          segments.set(selected++, segment);
        }
      }
    }
    stats.setInvalidSegments(invalid);
    return segments.subList(0, selected);
  }

  private static boolean isEmptySegment(IndexSegment segment) {
    return segment.getSegmentMetadata().getTotalDocs() == 0;
  }

  private static boolean isInvalidSegment(IndexSegment segment, QueryContext query) {
    return !segment.getColumnNames().containsAll(query.getColumns());
  }
}
