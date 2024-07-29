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

import com.google.common.base.Preconditions;
import java.util.Set;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * A {@link TierSegmentSelector} strategy which selects segments for a tier based on a fixed list
 */
public class FixedTierSegmentSelector implements TierSegmentSelector {
  private final Set<String> _segmentsToSelect;
  private final HelixManager _helixManager;

  public FixedTierSegmentSelector(HelixManager helixManager, Set<String> segmentsToSelect) {
    _segmentsToSelect = segmentsToSelect;
    _helixManager = helixManager;
  }

  @Override
  public String getType() {
    return TierFactory.FIXED_SEGMENT_SELECTOR_TYPE;
  }

  /**
   * Checks if a segment is eligible for the tier based on fixed list
   * @param tableNameWithType Name of the table
   * @param segmentName Name of the segment
   * @return true if eligible
   */
  @Override
  public boolean selectSegment(String tableNameWithType, String segmentName) {
    if (CollectionUtils.isNotEmpty(_segmentsToSelect) && _segmentsToSelect.contains(segmentName)) {
      if (TableType.OFFLINE == TableNameBuilder.getTableTypeFromTableName(tableNameWithType)) {
        return true;
      }

      SegmentZKMetadata segmentZKMetadata =
          ZKMetadataProvider.getSegmentZKMetadata(_helixManager.getHelixPropertyStore(), tableNameWithType,
              segmentName);
      Preconditions.checkNotNull(segmentZKMetadata, "Could not find zk metadata for segment: %s of table: %s",
          segmentName, tableNameWithType);
      return segmentZKMetadata.getStatus().isCompleted();
    }
    return false;
  }

  public Set<String> getSegmentsToSelect() {
    return _segmentsToSelect;
  }

  @Override
  public String toString() {
    return "FixedTierSegmentSelector{" + "_segmentsToSelect=" + _segmentsToSelect + '}';
  }
}
