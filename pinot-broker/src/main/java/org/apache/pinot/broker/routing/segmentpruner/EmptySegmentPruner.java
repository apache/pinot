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
package org.apache.pinot.broker.routing.segmentpruner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code EmptySegmentPruner} prunes segments if they have 0 totalDocs.
 * Does not prune segments with -1 total docs (that can be either error case or CONSUMING segment)
 */
public class EmptySegmentPruner implements SegmentPruner {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmptySegmentPruner.class);

  private final String _tableNameWithType;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _segmentZKMetadataPathPrefix;

  private final Map<String, Long> _segmentTotalDocsMap = new HashMap<>();
  private final Set<String> _emptySegments = new HashSet<>();

  public EmptySegmentPruner(TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableConfig.getTableName();
    _propertyStore = propertyStore;
    _segmentZKMetadataPathPrefix = ZKMetadataProvider.constructPropertyStorePathForResource(_tableNameWithType) + "/";
  }

  @Override
  public void init(ExternalView externalView, IdealState idealState, Set<String> onlineSegments) {
    // Bulk load info for all online segments
    int numSegments = onlineSegments.size();
    List<String> segments = new ArrayList<>(numSegments);
    List<String> segmentZKMetadataPaths = new ArrayList<>(numSegments);
    for (String segment : onlineSegments) {
      segments.add(segment);
      segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
    }
    List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
    for (int i = 0; i < numSegments; i++) {
      String segment = segments.get(i);
      long totalDocs = extractTotalDocsFromSegmentZKMetaZNRecord(segment, znRecords.get(i));
      _segmentTotalDocsMap.put(segment, totalDocs);
      if (totalDocs == 0) {
        _emptySegments.add(segment);
      }
    }
  }

  private long extractTotalDocsFromSegmentZKMetaZNRecord(String segment, @Nullable ZNRecord znRecord) {
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, _tableNameWithType);
      return -1;
    }
    return znRecord.getLongField(CommonConstants.Segment.TOTAL_DOCS, -1);
  }

  @Override
  public synchronized void onExternalViewChange(ExternalView externalView, IdealState idealState,
      Set<String> onlineSegments) {
    // NOTE: We don't update all the segment ZK metadata for every external view change, but only the new added/removed
    //       ones. The refreshed segment ZK metadata change won't be picked up.
    for (String segment : onlineSegments) {
      _segmentTotalDocsMap.computeIfAbsent(segment, k -> {
        long totalDocs = extractTotalDocsFromSegmentZKMetaZNRecord(k,
            _propertyStore.get(_segmentZKMetadataPathPrefix + k, null, AccessOption.PERSISTENT));
        if (totalDocs == 0) {
          _emptySegments.add(segment);
        }
        return totalDocs;
      });
    }
    _segmentTotalDocsMap.keySet().retainAll(onlineSegments);
    _emptySegments.retainAll(onlineSegments);
  }

  @Override
  public synchronized void refreshSegment(String segment) {
    long totalDocs = extractTotalDocsFromSegmentZKMetaZNRecord(segment,
        _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT));
    _segmentTotalDocsMap.put(segment, totalDocs);
    if (totalDocs == 0) {
      _emptySegments.add(segment);
    } else {
      _emptySegments.remove(segment);
    }
  }

  /**
   * Prune out segments which are empty
   */
  @Override
  public Set<String> prune(BrokerRequest brokerRequest, Set<String> segments) {
    segments.removeAll(_emptySegments);
    return segments;
  }
}
