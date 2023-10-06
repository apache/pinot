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
package org.apache.pinot.broker.routing.segmentpreselector;

import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.lineage.SegmentLineageUtils;


/**
 * Segment lineage based segment pre-selector
 *
 * This pre-selector reads the segment lineage metadata and filters out either merged segments or original segments
 * to make sure that the final segments contain no duplicate data.
 */
@AllArgsConstructor
public class SegmentLineageBasedSegmentPreSelector implements SegmentPreSelector {
  private final String _tableNameWithType;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @Override
  public Set<String> preSelect(Set<String> onlineSegments) {
    SegmentLineage segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, _tableNameWithType);
    SegmentLineageUtils.filterSegmentsBasedOnLineageInPlace(onlineSegments, segmentLineage);
    return onlineSegments;
  }
}
