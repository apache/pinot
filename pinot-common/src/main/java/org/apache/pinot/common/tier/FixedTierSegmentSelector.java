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
package org.apache.pinot.common.tier;

import java.util.Set;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;


/**
 * A {@link TierSegmentSelector} strategy which selects segments for a tier based on a fixed list
 */
public class FixedTierSegmentSelector implements TierSegmentSelector {
  private final Set<String> _segmentsToSelect;

  public FixedTierSegmentSelector(Set<String> segmentsToSelect) {
    _segmentsToSelect = segmentsToSelect;
  }

  @Override
  public String getType() {
    return TierFactory.FIXED_SEGMENT_SELECTOR_TYPE;
  }

  @Override
  public boolean selectSegment(String tableNameWithType, SegmentZKMetadata segmentZKMetadata) {
    return _segmentsToSelect.contains(segmentZKMetadata.getSegmentName()) && segmentZKMetadata.getStatus()
        .isCompleted();
  }

  public Set<String> getSegmentsToSelect() {
    return _segmentsToSelect;
  }

  @Override
  public String toString() {
    return "FixedTierSegmentSelector{" + "_segmentsToSelect=" + _segmentsToSelect + '}';
  }
}
