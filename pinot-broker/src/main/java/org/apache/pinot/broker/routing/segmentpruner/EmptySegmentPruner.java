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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentBrokerView;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.request.BrokerRequest;
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
  private Set<SegmentBrokerView> _emptySegments = new HashSet<>();

  public EmptySegmentPruner(TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableConfig.getTableName();
    _propertyStore = propertyStore;
    _segmentZKMetadataPathPrefix = ZKMetadataProvider.constructPropertyStorePathForResource(_tableNameWithType) + "/";
  }

  @Override
  public void init(ExternalView externalView, IdealState idealState, Set<SegmentBrokerView> onlineSegments) {
    onExternalViewChange(externalView, idealState, onlineSegments);
  }

  @Override
  public synchronized void onExternalViewChange(ExternalView externalView, IdealState idealState,
      Set<SegmentBrokerView> onlineSegments) {
    _emptySegments = onlineSegments.stream().filter(segmentMetadata -> segmentMetadata.getTotalDocs() == 0)
        .collect(Collectors.toSet());
  }

  @Override
  public synchronized void refreshSegment(SegmentBrokerView segment) {
    if (segment.getTotalDocs() == 0) {
      _emptySegments.add(segment);
    } else {
      _emptySegments.remove(segment);
    }
  }

  /**
   * Prune out segments which are empty
   */
  @Override
  public Set<SegmentBrokerView> prune(BrokerRequest brokerRequest, Set<SegmentBrokerView> segments) {
    if (_emptySegments.isEmpty()) {
      return segments;
    }
    Set<SegmentBrokerView> selectedSegments = new HashSet<>(segments);
    selectedSegments.removeAll(_emptySegments);
    return selectedSegments;
  }
}
