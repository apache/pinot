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

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.routing.segmentselector.SelectedSegments;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.CommonConstants.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code PartitionSegmentPruner} prunes segments based on the their partition metadata stored in ZK. The pruner
 * supports queries with filter (or nested filter) of EQUALITY and IN predicates.
 */
public class PartitionSegmentPruner implements SegmentPruner {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionSegmentPruner.class);
  private static final PartitionInfo INVALID_PARTITION_INFO = new PartitionInfo(null, null);

  private final String _tableNameWithType;
  private final String _partitionColumn;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _segmentZKMetadataPathPrefix;
  private final Map<String, PartitionInfo> _partitionInfoMap = new ConcurrentHashMap<>();
  private final Map<PartitionInfo, Set<String>> _partitionInfoSegments = new ConcurrentHashMap<>();
  private SelectedSegments _allSegments = new SelectedSegments(Collections.emptySet(), true);

  public PartitionSegmentPruner(String tableNameWithType, String partitionColumn,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableNameWithType;
    _partitionColumn = partitionColumn;
    _propertyStore = propertyStore;
    _segmentZKMetadataPathPrefix = ZKMetadataProvider.constructPropertyStorePathForResource(tableNameWithType) + "/";
  }

  @Override
  public void init(ExternalView externalView, IdealState idealState, Set<String> onlineSegments) {
    // Bulk load partition info for all online segments
    int numSegments = onlineSegments.size();
    List<String> segments = new ArrayList<>(numSegments);
    List<String> segmentZKMetadataPaths = new ArrayList<>(numSegments);
    for (String segment : onlineSegments) {
      segments.add(segment);
      segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
    }
    List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT);
    for (int i = 0; i < numSegments; i++) {
      String segment = segments.get(i);
      PartitionInfo partitionInfo = extractPartitionInfoFromSegmentZKMetadataZNRecord(segment, znRecords.get(i));
      if (partitionInfo != null) {
        _partitionInfoMap.put(segment, partitionInfo);
      }
    }
    rebuildReverseMap();
  }

  private void rebuildReverseMap() {
    _partitionInfoSegments.clear();
    Set<String> segmentsFound = new HashSet<>();
    _partitionInfoMap.forEach((segmentName, partitionInfo) -> {
      segmentsFound.add(segmentName);
      Set<String> segments = _partitionInfoSegments.computeIfAbsent(partitionInfo, (info) -> new HashSet<>());
      segments.add(segmentName);
    });
    _allSegments = new SelectedSegments(segmentsFound, true);
  }
  /**
   * NOTE: Returns {@code null} when the ZNRecord is missing (could be transient Helix issue). Returns
   *       {@link #INVALID_PARTITION_INFO} when the segment does not have valid partition metadata in its ZK metadata,
   *       in which case we won't retry later.
   */
  @Nullable
  private PartitionInfo extractPartitionInfoFromSegmentZKMetadataZNRecord(String segment, @Nullable ZNRecord znRecord) {
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, _tableNameWithType);
      return null;
    }

    String partitionMetadataJson = znRecord.getSimpleField(Segment.PARTITION_METADATA);
    if (partitionMetadataJson == null) {
      LOGGER.warn("Failed to find segment partition metadata for segment: {}, table: {}", segment, _tableNameWithType);
      return INVALID_PARTITION_INFO;
    }

    SegmentPartitionMetadata segmentPartitionMetadata;
    try {
      segmentPartitionMetadata = SegmentPartitionMetadata.fromJsonString(partitionMetadataJson);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while extracting segment partition metadata for segment: {}, table: {}", segment,
          _tableNameWithType, e);
      return INVALID_PARTITION_INFO;
    }

    ColumnPartitionMetadata columnPartitionMetadata =
        segmentPartitionMetadata.getColumnPartitionMap().get(_partitionColumn);
    if (columnPartitionMetadata == null) {
      LOGGER.warn("Failed to find column partition metadata for column: {}, segment: {}, table: {}", _partitionColumn,
          segment, _tableNameWithType);
      return INVALID_PARTITION_INFO;
    }

    return new PartitionInfo(PartitionFunctionFactory
        .getPartitionFunction(columnPartitionMetadata.getFunctionName(), columnPartitionMetadata.getNumPartitions()),
        columnPartitionMetadata.getPartitions());
  }

  @Override
  public synchronized void onExternalViewChange(ExternalView externalView, IdealState idealState,
      Set<String> onlineSegments) {
    // NOTE: We don't update all the segment ZK metadata for every external view change, but only the new added/removed
    //       ones. The refreshed segment ZK metadata change won't be picked up.
    for (String segment : onlineSegments) {
      _partitionInfoMap.computeIfAbsent(segment, k -> extractPartitionInfoFromSegmentZKMetadataZNRecord(k,
          _propertyStore.get(_segmentZKMetadataPathPrefix + k, null, AccessOption.PERSISTENT)));
    }
    _partitionInfoMap.keySet().retainAll(onlineSegments);
    rebuildReverseMap();
  }

  @Override
  public synchronized void refreshSegment(String segment) {
    PartitionInfo partitionInfo = extractPartitionInfoFromSegmentZKMetadataZNRecord(segment,
        _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT));
    if (partitionInfo != null) {
      _partitionInfoMap.put(segment, partitionInfo);
    } else {
      _partitionInfoMap.remove(segment);
    }
    rebuildReverseMap();
  }

//  /**
//   * Package-level function to make previous tests happy
//   */
//  @VisibleForTesting
//  SelectedSegments prune(BrokerRequest brokerRequest, Set<String> segments) {
//    return prune(brokerRequest, new SelectedSegments(segments, true));
//  }
  @Override
  public SelectedSegments prune(BrokerRequest brokerRequest, SelectedSegments segments) {
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();
    if (pinotQuery != null) {
      // SQL

      Expression filterExpression = pinotQuery.getFilterExpression();
      if (filterExpression == null) {
        return segments;
      }
      return pruneSegments(segments, (partitionInfo, cachedPartitionFunction) -> isPartitionMatch(filterExpression,
        partitionInfo, cachedPartitionFunction));
    } else {
      // PQL
      FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
      if (filterQueryTree == null) {
        return segments;
      }
      return pruneSegments(segments, (partitionInfo, cachedPartitionFunction) -> isPartitionMatch(filterQueryTree, partitionInfo, cachedPartitionFunction));
    }
  }

  private SelectedSegments pruneSegments(SelectedSegments segments, java.util.function.BiFunction<PartitionInfo, CachedPartitionFunction,  Boolean> partitionMatchLambda) {
    // Some segments may not be refreshed/notified via Zookeeper yet, but broker may have found it
    // in this case we need to include the new segments.
    Set<String> selectedSegments = new HashSet<>();
    CachedPartitionFunction cachedPartitionFunction = new CachedPartitionFunction();
    if (segments.hasHash() && segments.getSegmentHash().equals(_allSegments.getSegmentHash())) {
      for (PartitionInfo partitionInfo : _partitionInfoSegments.keySet()) {
        if (partitionInfo
            == INVALID_PARTITION_INFO) { // segments without partitions not pruned if they are
                                         // passed in
          selectedSegments.addAll(
              Sets.intersection(_partitionInfoSegments.get(partitionInfo), segments.getSegments()));
        } else if (partitionMatchLambda.apply(partitionInfo, cachedPartitionFunction)) {
          selectedSegments.addAll(_partitionInfoSegments.get(partitionInfo));
        }
      }
    } else {
      // The segments are not exact match of our pre-built segments from onExternalView() notifications
      // so they need to be pruned properly one by one
      for (String segment : segments.getSegments()) {
        PartitionInfo partitionInfo = _partitionInfoMap.get(segment);
        if (partitionInfo == null
            || partitionInfo == INVALID_PARTITION_INFO
            || partitionMatchLambda.apply(partitionInfo, cachedPartitionFunction)) {
          selectedSegments.add(segment);
        }
      }
    }
    return new SelectedSegments(selectedSegments, false); // avoid hash to save CPU.
  }

  private boolean isPartitionMatch(Expression filterExpression, PartitionInfo partitionInfo, CachedPartitionFunction cachedPartitionFunction) {
    Function function = filterExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());
    List<Expression> operands = function.getOperands();
    switch (filterKind) {
      case AND:
        for (Expression child : operands) {
          if (!isPartitionMatch(child, partitionInfo, cachedPartitionFunction)) {
            return false;
          }
        }
        return true;
      case OR:
        for (Expression child : operands) {
          if (isPartitionMatch(child, partitionInfo, cachedPartitionFunction)) {
            return true;
          }
        }
        return false;
      case EQUALS: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          return partitionInfo._partitions.contains(
            cachedPartitionFunction.getPartition(partitionInfo._partitionFunction, operands.get(1).getLiteral().getFieldValue().toString()));
        } else {
          return true;
        }
      }
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName().equals(_partitionColumn)) {
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            if (partitionInfo._partitions.contains(cachedPartitionFunction.getPartition(partitionInfo._partitionFunction,
              operands.get(i).getLiteral().getFieldValue().toString()))) {
              return true;
            }
          }
          return false;
        } else {
          return true;
        }
      }
      default:
        return true;
    }
  }

  @Deprecated
  private boolean isPartitionMatch(FilterQueryTree filterQueryTree, PartitionInfo partitionInfo, CachedPartitionFunction cachedPartitionFunction) {
    switch (filterQueryTree.getOperator()) {
      case AND:
        for (FilterQueryTree child : filterQueryTree.getChildren()) {
          if (!isPartitionMatch(child, partitionInfo, cachedPartitionFunction)) {
            return false;
          }
        }
        return true;
      case OR:
        for (FilterQueryTree child : filterQueryTree.getChildren()) {
          if (isPartitionMatch(child, partitionInfo, cachedPartitionFunction)) {
            return true;
          }
        }
        return false;
      case EQUALITY:
      case IN:
        if (filterQueryTree.getColumn().equals(_partitionColumn)) {
          for (String value : filterQueryTree.getValue()) {
            if (partitionInfo._partitions.contains(cachedPartitionFunction.getPartition(partitionInfo._partitionFunction, value))) {
              return true;
            }
          }
          return false;
        }
      default:
        return true;
    }
  }

  private static class PartitionInfo {
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;

    PartitionInfo(PartitionFunction partitionFunction, Set<Integer> partitions) {
      _partitionFunction = partitionFunction;
      _partitions = partitions;
    }
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PartitionInfo that = (PartitionInfo) o;
      return Objects.equals(_partitionFunction, that._partitionFunction) &&
        Objects.equals(_partitions, that._partitions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_partitionFunction, _partitions);
    }
  }

  /**
   * This class is created to speed up partition number calls. Sometimes with lots of segments (like 40k+)
   * Computing partition values may be CPU intensive, caching the computation result may help
   *
   * For each partition function, we have one TTL Fixed Sized Cache that caches the computation results of all
   * the inputs seen. For example, if PartitionFunction is Murmur5, and Murmur5.getPartition("my_segment_1") = 9,
   * after first computation, the "my_segment_1" => 9 value mapping is stored in the cache.
   * The next time we can skip the computation of Murmur5.getPartition("my_segment_1") and
   * directly lookup and find that the partition of "my_segment_1" is 9.
   *
   * This is helpful in situations where we have very high QPS, and Brokers are trying to prune 40k+ segments
   * over and over again, such mapping/lookup can save CPU resource.
   */
  private static class CachedPartitionFunction {
    final Map<PartitionFunction, Map<String, Integer>> cache = new HashMap<>();

    public int getPartition(final PartitionFunction func, String partitionKey) {
      Map<String, Integer> valueToPartition = cache.computeIfAbsent(func, (ignored) -> new HashMap<>());
      if (!valueToPartition.containsKey(partitionKey)) {
        valueToPartition.put(partitionKey, func.getPartition(partitionKey));
      }
      return valueToPartition.get(partitionKey);
    }
  }
}
