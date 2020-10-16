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
package org.apache.pinot.controller.helix.core.minion.mergestrategy;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.operator.transform.transformer.timeunit.CustomTimeUnitTransformer;
import org.apache.pinot.spi.utils.TimeUtils;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Time based merge strategy.
 *
 * When the segments are correctly time aligned (e.g. each segment contains exactly 1 day's data), the time based merge
 * strategy allows to merge segments based on the segment granularity.
 *
 * For example, the merge strategy will try to merge segments based on the daily bucket if the segment granularity is
 * set to 'DAILY' (i.e. only segments from the same day can be considered to be merged).
 *
 * Default is 'NONE' where the strategy would try to merge everything into a single time bucket.
 */
public class TimeBasedMergeStrategy implements MergeStrategy {

  public enum SegmentGranularity {
    NONE, DAYS, WEEKS, MONTHS
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TimeBasedMergeStrategy.class);

  private SegmentGranularity _segmentGranularity;

  public TimeBasedMergeStrategy(Map<String, String> taskConfigs) {
    String segmentGranularityStr = taskConfigs.get(MinionConstants.MergeRollupTask.SEGMENT_GRANULARITY);
    try {
      _segmentGranularity = SegmentGranularity.valueOf(segmentGranularityStr);
    } catch (Exception e) {
      _segmentGranularity = SegmentGranularity.NONE;
    }
    LOGGER.info("Time based merge strategy is initialized with the segment granularity : {}", _segmentGranularity);
  }

  @Override
  public List<List<SegmentZKMetadata>> generateMergeTaskCandidates(List<SegmentZKMetadata> segmentsToMerge,
      int maxNumSegmentsPerTask) {
    Map<Long, List<SegmentZKMetadata>> bucketedSegments = new TreeMap<>();

    for (SegmentZKMetadata segmentZKMetadata : segmentsToMerge) {
      TimeUnit timeUnit = segmentZKMetadata.getTimeUnit();
      long startTimeInMillis = timeUnit.toMillis(segmentZKMetadata.getStartTime());
      long endTimeInMillis = timeUnit.toMillis(segmentZKMetadata.getEndTime());

      if (!TimeUtils.isValidTimeInterval(new Interval(startTimeInMillis, endTimeInMillis))) {
        LOGGER.warn("Time interval is not valid. segmentName = {}, startTime = {}, endTime= {}",
            segmentZKMetadata.getSegmentName(), startTimeInMillis, endTimeInMillis);
        continue;
      }

      switch (_segmentGranularity) {
        case DAYS:
          startTimeInMillis = TimeUnit.DAYS.convert(startTimeInMillis, TimeUnit.MILLISECONDS);
          endTimeInMillis = TimeUnit.DAYS.convert(endTimeInMillis, TimeUnit.MILLISECONDS);
          break;
        case WEEKS:
          CustomTimeUnitTransformer weeksTimeUnitTransformer =
              new CustomTimeUnitTransformer(TimeUnit.MILLISECONDS, "WEEKS");
          long[] weeksInput = new long[]{startTimeInMillis, endTimeInMillis};
          long[] weeksOutput = new long[2];
          weeksTimeUnitTransformer.transform(weeksInput, weeksOutput, weeksInput.length);
          startTimeInMillis = weeksOutput[0];
          endTimeInMillis = weeksOutput[1];
          break;
        case MONTHS:
          CustomTimeUnitTransformer monthsTimeUnitTransformer =
              new CustomTimeUnitTransformer(TimeUnit.MILLISECONDS, "MONTHS");
          long[] monthsInput = new long[]{startTimeInMillis, endTimeInMillis};
          long[] monthsOutput = new long[2];
          monthsTimeUnitTransformer.transform(monthsInput, monthsOutput, monthsInput.length);
          startTimeInMillis = monthsOutput[0];
          endTimeInMillis = monthsOutput[1];
          break;
        default:
          startTimeInMillis = 0;
          endTimeInMillis = 0;
          break;
      }

      if (startTimeInMillis != endTimeInMillis) {
        LOGGER.warn(
            "The segment is not time aligned which means that the segment falls into multiple time buckets based on "
                + "the segment granularity. segmentName = {}, startTime = {}, outputTime = {}, segmentGranularity = {}",
            segmentZKMetadata.getSegmentName(), startTimeInMillis, endTimeInMillis, _segmentGranularity);
        continue;
      }
      List<SegmentZKMetadata> segments = bucketedSegments.computeIfAbsent(startTimeInMillis, k -> new ArrayList<>());
      segments.add(segmentZKMetadata);
    }

    // Compute the final list of segments to be merged together
    List<List<SegmentZKMetadata>> result = new ArrayList<>();
    for (List<SegmentZKMetadata> segmentsPerTimeBucket : bucketedSegments.values()) {
      List<List<SegmentZKMetadata>> segmentListsToMerge = Lists.partition(segmentsPerTimeBucket, maxNumSegmentsPerTask);
      for (List<SegmentZKMetadata> segmentListToMerge : segmentListsToMerge) {
        result.add(segmentListToMerge);
      }
    }
    return result;
  }
}
