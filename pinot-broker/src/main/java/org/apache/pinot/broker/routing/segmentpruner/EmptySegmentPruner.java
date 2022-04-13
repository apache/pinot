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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code EmptySegmentPruner} prunes segments if they have 0 total docs.
 * It does not prune segments with -1 total docs (that can be either error case or CONSUMING segment).
 */
public class EmptySegmentPruner implements SegmentPruner {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmptySegmentPruner.class);

  private final String _tableNameWithType;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _segmentZKMetadataPathPrefix;

  private final Set<String> _segmentsLoaded = new HashSet<>();
  private final Set<String> _emptySegments = ConcurrentHashMap.newKeySet();

  private volatile ResultCache _resultCache;

  public EmptySegmentPruner(TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableConfig.getTableName();
    _propertyStore = propertyStore;
    _segmentZKMetadataPathPrefix = ZKMetadataProvider.constructPropertyStorePathForResource(_tableNameWithType) + "/";
  }

  @Override
  public void init(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
    // Bulk load info for all online segments
    int numSegments = onlineSegments.size();
    List<String> segments = new ArrayList<>(numSegments);
    List<String> segmentZKMetadataPaths = new ArrayList<>(numSegments);
    for (String segment : onlineSegments) {
      segments.add(segment);
      segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
    }
    _segmentsLoaded.addAll(segments);
    List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
    for (int i = 0; i < numSegments; i++) {
      String segment = segments.get(i);
      if (isEmpty(segment, znRecords.get(i))) {
        _emptySegments.add(segment);
      }
    }
  }

  @Override
  public synchronized void onAssignmentChange(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    // NOTE: We don't update all the segment ZK metadata for every external view change, but only the new added/removed
    //       ones. The refreshed segment ZK metadata change won't be picked up.
    boolean emptySegmentsChanged = false;
    for (String segment : onlineSegments) {
      if (_segmentsLoaded.add(segment)) {
        if (isEmpty(segment,
            _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT))) {
          emptySegmentsChanged |= _emptySegments.add(segment);
        }
      }
    }
    _segmentsLoaded.retainAll(onlineSegments);
    emptySegmentsChanged |= _emptySegments.retainAll(onlineSegments);

    if (emptySegmentsChanged) {
      // Reset the result cache when empty segments changed
      _resultCache = null;
    }
  }

  @Override
  public synchronized void refreshSegment(String segment) {
    _segmentsLoaded.add(segment);
    if (isEmpty(segment, _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT))) {
      if (_emptySegments.add(segment)) {
        // Reset the result cache when empty segments changed
        _resultCache = null;
      }
    } else {
      if (_emptySegments.remove(segment)) {
        // Reset the result cache when empty segments changed
        _resultCache = null;
      }
    }
  }

  private boolean isEmpty(String segment, @Nullable ZNRecord segmentZKMetadataZNRecord) {
    if (segmentZKMetadataZNRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, _tableNameWithType);
      return false;
    }
    return segmentZKMetadataZNRecord.getLongField(CommonConstants.Segment.TOTAL_DOCS, -1) == 0;
  }

  @Override
  public Set<String> prune(BrokerRequest brokerRequest, Set<String> segments) {
    if (_emptySegments.isEmpty()) {
      return segments;
    }

    // Return the cached result when the input is the same reference
    ResultCache resultCache = _resultCache;
    if (resultCache != null && resultCache._inputSegments == segments) {
      return resultCache._outputSegments;
    }

    Set<String> selectedSegments = new HashSet<>(segments);
    selectedSegments.removeAll(_emptySegments);
    _resultCache = new ResultCache(segments, selectedSegments);
    return selectedSegments;
  }

  private static class ResultCache {
    final Set<String> _inputSegments;
    final Set<String> _outputSegments;

    ResultCache(Set<String> inputSegments, Set<String> outputSegments) {
      _inputSegments = inputSegments;
      _outputSegments = outputSegments;
    }
  }
}
