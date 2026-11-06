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
package org.apache.pinot.broker.routing.instanceselector;

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.ToIntFunction;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Builds the [SegmentGroups] of one rebuild of the instance selector state.
///
/// Segments with the same ideal state and external view instance state maps, and of the same kind (old or new), have
/// the same candidates. In a large table, most segments share their maps with many others (e.g. all the segments of a
/// stream partition), so the builder computes the candidates once per distinct pair of maps, and allocates nothing per
/// segment: the only per-segment state is the segment's entry in the segment to group id map.
///
/// Building takes two steps, because strict replica-group routing needs to see every pair before computing any
/// candidate (an instance unavailable for an old segment is excluded for every segment with the same ideal state
/// instances):
/// - [#addSegment] records each segment with the id of its pair and whether it is new;
/// - [#build] computes the candidates of each pair, canonicalizes them into [CandidateGroup]s, and rewrites the pair id
///   recorded for each segment into its group id.
///
/// Not thread safe. Only used by the rebuild thread, for the duration of one rebuild.
final class SegmentGroupsBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentGroupsBuilder.class);

  // Preserve Set equality regardless of instance iteration order.
  // Mixed hashes reduce collisions without copying or sorting keys.
  // Backing assignment maps must remain unchanged during the rebuild.
  static final Hash.Strategy<Set<String>> INSTANCE_SET_STRATEGY =
      new Hash.Strategy<>() {
        @Override
        public int hashCode(Set<String> instances) {
          if (instances == null) {
            return 0;
          }

          int first = 0;
          int second = 0;
          for (String instance : instances) {
            int hash = instance.hashCode();
            first += HashCommon.mix(hash);
            second += HashCommon.mix(hash ^ 0x9e3779b9);
          }

          return HashCommon.mix(first ^ Integer.rotateLeft(second, 16) ^ instances.size());
        }

        @Override
        public boolean equals(Set<String> left, Set<String> right) {
          return left == right || (left != null && right != null && left.equals(right));
        }
      };

  private static final int UNKNOWN_POOL = Integer.MIN_VALUE;
  private static final int NO_GROUP = -1;

  private final String _tableNameWithType;
  private final Object2LongOpenHashMap<String> _newSegmentCreationTimeMap;
  private final boolean _strictReplicaGroup;
  private final ToIntFunction<String> _poolFunction;
  // Segment to (pair id << 1 | new) until build() rewrites it into the group id
  private final Object2IntOpenHashMap<String> _segmentToGroupId;
  private final InstanceStateMapsCache _instanceStateMapsCache = new InstanceStateMapsCache();
  private final Object2IntOpenHashMap<String> _instanceToPool = new Object2IntOpenHashMap<>();

  private final Object2IntOpenHashMap<String> _instanceToSlot = new Object2IntOpenHashMap<>();
  // First candidate created for each slot
  private final List<SegmentInstanceCandidate> _slotCandidates = new ArrayList<>();
  // Candidates interned by (slot, ideal state replica id, online flag), so that equal candidate lists hold the same
  // candidate objects and compare by identity. The pool is a function of the slot.
  private final Long2ObjectOpenHashMap<SegmentInstanceCandidate> _internedCandidates = new Long2ObjectOpenHashMap<>();
  private final List<SegmentInstanceCandidate> _scratchCandidates = new ArrayList<>();
  private final IntArrayList _scratchSlots = new IntArrayList();

  private final Object2IntOpenHashMap<GroupKey> _groupKeyToId = new Object2IntOpenHashMap<>();
  private final List<CandidateGroup> _groups = new ArrayList<>();
  private final IntArrayList _groupSizes = new IntArrayList();

  /// @param newSegmentCreationTimeMap creation time of the new segments; any other segment is an old segment
  /// @param numSegments number of segments to be added, to presize the segment map
  /// @param strictReplicaGroup whether to exclude an instance unavailable for any old segment from the candidates of
  ///                           every segment with the same instances in the ideal state
  /// @param poolFunction returns the pool of an instance; called once per instance
  SegmentGroupsBuilder(String tableNameWithType, Object2LongOpenHashMap<String> newSegmentCreationTimeMap,
      int numSegments, boolean strictReplicaGroup, ToIntFunction<String> poolFunction) {
    _tableNameWithType = tableNameWithType;
    _newSegmentCreationTimeMap = newSegmentCreationTimeMap;
    _strictReplicaGroup = strictReplicaGroup;
    _poolFunction = poolFunction;
    _segmentToGroupId = new Object2IntOpenHashMap<>(numSegments);
    _segmentToGroupId.defaultReturnValue(SegmentStates.UNKNOWN_SEGMENT);
    _instanceToPool.defaultReturnValue(UNKNOWN_POOL);
    _instanceToSlot.defaultReturnValue(SegmentGroups.NO_SLOT);
    _groupKeyToId.defaultReturnValue(NO_GROUP);
  }

  /// Returns whether the instance state is online for routing purpose (ONLINE/CONSUMING).
  static boolean isOnlineForRouting(@Nullable String state) {
    return SegmentStateModel.ONLINE.equals(state) || SegmentStateModel.CONSUMING.equals(state);
  }

  /// Returns whether the given instance is online for routing purpose in both the ideal state and the external view.
  private static boolean isOnlineForRouting(@Nullable String idealStateState,
      @Nullable Map<String, String> externalViewInstanceStateMap, String instance) {
    // NOTE: DO NOT check if EV matches IS because it is a valid state when EV is CONSUMING while IS is ONLINE
    return isOnlineForRouting(idealStateState)
        && externalViewInstanceStateMap != null
        && isOnlineForRouting(externalViewInstanceStateMap.get(instance));
  }

  /// Returns a map that iterates the entries of the given map in key order: the map itself when it is a
  /// [SortedMap] or when its iteration order is already sorted, otherwise a sorted copy.
  ///
  /// The instance state maps read from ZooKeeper are insertion-ordered, and are usually written in key order, so
  /// the linear check avoids the copy in the common case.
  static Map<String, String> sortByKeyIfNeeded(Map<String, String> map) {
    if (map instanceof SortedMap) {
      return map;
    }
    String previousKey = null;
    for (String key : map.keySet()) {
      if (previousKey != null && previousKey.compareTo(key) > 0) {
        return new TreeMap<>(map);
      }
      previousKey = key;
    }
    return map;
  }

  /// Records the given segment with its ideal state and external view instance state maps. The segment is new when
  /// it has a creation time.
  void addSegment(String segment, Map<String, String> idealStateInstanceStateMap,
      @Nullable Map<String, String> externalViewInstanceStateMap) {
    InstanceStateMaps instanceStateMaps =
        _instanceStateMapsCache.canonicalize(idealStateInstanceStateMap, externalViewInstanceStateMap);
    boolean isNew = _newSegmentCreationTimeMap.containsKey(segment);
    if (isNew) {
      if (instanceStateMaps._numNewSegments++ == 0) {
        instanceStateMaps._sampleNewSegment = segment;
      }
    } else {
      if (instanceStateMaps._numOldSegments++ == 0) {
        instanceStateMaps._sampleOldSegment = segment;
      }
    }
    _segmentToGroupId.put(segment, instanceStateMaps._id << 1 | (isNew ? 1 : 0));
  }

  /// Returns the number of added segments whose ideal state instances do not span at least 2 pools.
  int getNumSinglePoolSegments() {
    int numSinglePoolSegments = 0;
    for (InstanceStateMaps instanceStateMaps : _instanceStateMapsCache._instanceStateMapsList) {
      if (isSinglePool(instanceStateMaps._idealStateInstanceStateMap)) {
        numSinglePoolSegments += instanceStateMaps._numOldSegments + instanceStateMaps._numNewSegments;
      }
    }
    return numSinglePoolSegments;
  }

  private boolean isSinglePool(Map<String, String> idealStateInstanceStateMap) {
    boolean firstInstance = true;
    int pool = 0;
    for (String instance : idealStateInstanceStateMap.keySet()) {
      int instancePool = getPool(instance);
      if (firstInstance) {
        pool = instancePool;
        firstInstance = false;
      } else if (instancePool != pool) {
        return false;
      }
    }
    return true;
  }

  /// Returns the groups of the added segments. The builder must not be used afterwards.
  SegmentGroups build() {
    List<InstanceStateMaps> instanceStateMapsList = _instanceStateMapsCache._instanceStateMapsList;
    Map<Set<String>, Set<String>> unavailableInstancesMap =
        _strictReplicaGroup ? computeUnavailableInstancesMap(instanceStateMapsList) : null;
    int numInstanceStateMaps = instanceStateMapsList.size();
    // Indexed by the value recorded for the segments: pair id << 1 | new
    int[] groupIds = new int[numInstanceStateMaps << 1];
    for (int i = 0; i < numInstanceStateMaps; i++) {
      InstanceStateMaps instanceStateMaps = instanceStateMapsList.get(i);
      Map<String, String> idealStateInstanceStateMap = instanceStateMaps._idealStateInstanceStateMap;
      if (instanceStateMaps._numOldSegments > 0) {
        int expectedReplicas = addOldSegmentCandidates(instanceStateMaps, unavailableInstancesMap != null
            ? unavailableInstancesMap.get(idealStateInstanceStateMap.keySet())
            : Set.of());
        int groupId = addGroup(false, expectedReplicas, instanceStateMaps._sampleOldSegment);
        _groupSizes.set(groupId, _groupSizes.getInt(groupId) + instanceStateMaps._numOldSegments);
        groupIds[i << 1] = groupId;
      }
      if (instanceStateMaps._numNewSegments > 0) {
        if (unavailableInstancesMap != null) {
          addStrictReplicaGroupNewSegmentCandidates(instanceStateMaps,
              unavailableInstancesMap.getOrDefault(idealStateInstanceStateMap.keySet(), Set.of()));
        } else {
          addNewSegmentCandidates(instanceStateMaps);
        }
        int groupId = addGroup(true, 0, instanceStateMaps._sampleNewSegment);
        _groupSizes.set(groupId, _groupSizes.getInt(groupId) + instanceStateMaps._numNewSegments);
        groupIds[i << 1 | 1] = groupId;
      }
    }
    _segmentToGroupId.object2IntEntrySet().fastForEach(entry -> entry.setValue(groupIds[entry.getIntValue()]));

    int numSlots = _slotCandidates.size();
    String[] slotInstances = new String[numSlots];
    int[] slotPools = new int[numSlots];
    for (int slot = 0; slot < numSlots; slot++) {
      SegmentInstanceCandidate candidate = _slotCandidates.get(slot);
      slotInstances[slot] = candidate.getInstance();
      slotPools[slot] = candidate.getPool();
    }
    return new SegmentGroups(_segmentToGroupId, _newSegmentCreationTimeMap, _groups.toArray(new CandidateGroup[0]),
        _groupSizes.toIntArray(), slotInstances, slotPools, _instanceToSlot);
  }

  /// Returns the instances unavailable for any old segment, keyed by the instances in the ideal state of the segment:
  /// the instances that are not online for routing in both the ideal state and the external view.
  private Map<Set<String>, Set<String>> computeUnavailableInstancesMap(List<InstanceStateMaps> instanceStateMapsList) {
    Map<Set<String>, Set<String>> unavailableInstancesMap =
        new Object2ObjectOpenCustomHashMap<>(INSTANCE_SET_STRATEGY);
    for (InstanceStateMaps instanceStateMaps : instanceStateMapsList) {
      if (instanceStateMaps._numOldSegments == 0) {
        continue;
      }
      Map<String, String> idealStateInstanceStateMap = instanceStateMaps._idealStateInstanceStateMap;
      Map<String, String> externalViewInstanceStateMap = instanceStateMaps._externalViewInstanceStateMap;
      Set<String> instancesInIdealState = idealStateInstanceStateMap.keySet();
      Set<String> unavailableInstances =
          unavailableInstancesMap.computeIfAbsent(instancesInIdealState, k -> new HashSet<>());
      for (Map.Entry<String, String> entry : idealStateInstanceStateMap.entrySet()) {
        String instance = entry.getKey();
        if (!isOnlineForRouting(entry.getValue(), externalViewInstanceStateMap, instance)
            && unavailableInstances.add(instance)) {
          LOGGER.warn(
              "Found unavailable instance: {} in instance group: {} for segment: {}, table: {} (IS: {}, EV: {})",
              instance, instancesInIdealState, instanceStateMaps._sampleOldSegment, _tableNameWithType,
              idealStateInstanceStateMap, externalViewInstanceStateMap);
        }
      }
    }
    return unavailableInstancesMap;
  }

  /// Adds the candidates of an old segment: the instances that are online for routing in both the ideal state and the
  /// external view, except the given excluded instances. Returns the expected number of replicas: the number of
  /// instances that are ONLINE/CONSUMING in the ideal state, and at least 1.
  private int addOldSegmentCandidates(InstanceStateMaps instanceStateMaps, Set<String> excludedInstances) {
    Map<String, String> idealStateInstanceStateMap = sortByKeyIfNeeded(instanceStateMaps._idealStateInstanceStateMap);
    Map<String, String> externalViewInstanceStateMap = instanceStateMaps._externalViewInstanceStateMap;
    int numInstancesOnlineForRouting = 0;
    int idealStateReplicaId = 0;
    for (Map.Entry<String, String> entry : idealStateInstanceStateMap.entrySet()) {
      if (isOnlineForRouting(entry.getValue())) {
        numInstancesOnlineForRouting++;
        String instance = entry.getKey();
        if (externalViewInstanceStateMap != null
            && isOnlineForRouting(externalViewInstanceStateMap.get(instance))
            && !excludedInstances.contains(instance)) {
          addCandidate(instance, true, idealStateReplicaId);
        }
      }
      idealStateReplicaId++;
    }
    return Math.max(1, numInstancesOnlineForRouting);
  }

  /// Adds the candidates of a new segment: the instances that are ONLINE/CONSUMING in the ideal state. A candidate is
  /// online when the instance is ONLINE/CONSUMING in the external view.
  private void addNewSegmentCandidates(InstanceStateMaps instanceStateMaps) {
    Map<String, String> idealStateInstanceStateMap = sortByKeyIfNeeded(instanceStateMaps._idealStateInstanceStateMap);
    Map<String, String> externalViewInstanceStateMap = instanceStateMaps._externalViewInstanceStateMap;
    int idealStateReplicaId = 0;
    for (Map.Entry<String, String> entry : idealStateInstanceStateMap.entrySet()) {
      if (isOnlineForRouting(entry.getValue())) {
        String instance = entry.getKey();
        boolean online =
            externalViewInstanceStateMap != null && isOnlineForRouting(externalViewInstanceStateMap.get(instance));
        addCandidate(instance, online, idealStateReplicaId);
      }
      idealStateReplicaId++;
    }
  }

  /// Adds the candidates of a new segment for strict replica-group routing: the instances in the ideal state, except
  /// the given excluded instances. A candidate is online when the instance is online for routing in both the ideal
  /// state and the external view.
  private void addStrictReplicaGroupNewSegmentCandidates(InstanceStateMaps instanceStateMaps,
      Set<String> excludedInstances) {
    Map<String, String> idealStateInstanceStateMap = sortByKeyIfNeeded(instanceStateMaps._idealStateInstanceStateMap);
    Map<String, String> externalViewInstanceStateMap = instanceStateMaps._externalViewInstanceStateMap;
    int idealStateReplicaId = 0;
    for (Map.Entry<String, String> entry : idealStateInstanceStateMap.entrySet()) {
      String instance = entry.getKey();
      if (!excludedInstances.contains(instance)) {
        addCandidate(instance, isOnlineForRouting(entry.getValue(), externalViewInstanceStateMap, instance),
            idealStateReplicaId);
      }
      idealStateReplicaId++;
    }
  }

  /// Adds the given candidate to the scratch candidates, creating its slot when the instance does not have one yet.
  private void addCandidate(String instance, boolean online, int idealStateReplicaId) {
    int slot = _instanceToSlot.getInt(instance);
    SegmentInstanceCandidate candidate;
    if (slot == SegmentGroups.NO_SLOT) {
      slot = _slotCandidates.size();
      candidate = new SegmentInstanceCandidate(instance, online, getPool(instance), idealStateReplicaId);
      _instanceToSlot.put(instance, slot);
      _slotCandidates.add(candidate);
      _internedCandidates.put(getCandidateKey(slot, online, idealStateReplicaId), candidate);
    } else {
      long candidateKey = getCandidateKey(slot, online, idealStateReplicaId);
      candidate = _internedCandidates.get(candidateKey);
      if (candidate == null) {
        candidate =
            new SegmentInstanceCandidate(instance, online, _slotCandidates.get(slot).getPool(), idealStateReplicaId);
        _internedCandidates.put(candidateKey, candidate);
      }
    }
    _scratchCandidates.add(candidate);
    _scratchSlots.add(slot);
  }

  private static long getCandidateKey(int slot, boolean online, int idealStateReplicaId) {
    return (long) slot << 32 | (long) idealStateReplicaId << 1 | (online ? 1 : 0);
  }

  /// Returns the id of the group of the scratch candidates, adding the group if it does not exist yet, and clears the
  /// scratch candidates.
  private int addGroup(boolean isNew, int expectedReplicas, String sampleSegment) {
    SegmentInstanceCandidate[] candidates = _scratchCandidates.toArray(new SegmentInstanceCandidate[0]);
    GroupKey groupKey = new GroupKey(candidates, isNew, expectedReplicas);
    int groupId = _groupKeyToId.getInt(groupKey);
    if (groupId == NO_GROUP) {
      groupId = _groups.size();
      _groups.add(new CandidateGroup(new GroupRouting(List.of(candidates), _scratchSlots.toIntArray()), isNew,
          expectedReplicas, sampleSegment));
      _groupSizes.add(0);
      _groupKeyToId.put(groupKey, groupId);
    }
    _scratchCandidates.clear();
    _scratchSlots.clear();
    return groupId;
  }

  private int getPool(String instance) {
    int pool = _instanceToPool.getInt(instance);
    if (pool == UNKNOWN_POOL) {
      pool = _poolFunction.applyAsInt(instance);
      _instanceToPool.put(instance, pool);
    }
    return pool;
  }

  /// The content of a [CandidateGroup]. The candidates are interned, so they compare by identity.
  private static final class GroupKey {
    final SegmentInstanceCandidate[] _candidates;
    final boolean _new;
    final int _expectedReplicas;
    final int _hashCode;

    GroupKey(SegmentInstanceCandidate[] candidates, boolean isNew, int expectedReplicas) {
      _candidates = candidates;
      _new = isNew;
      _expectedReplicas = expectedReplicas;
      int hashCode = 31 * Boolean.hashCode(isNew) + expectedReplicas;
      for (SegmentInstanceCandidate candidate : candidates) {
        hashCode = 31 * (31 * hashCode + candidate.getInstance().hashCode())
            + (candidate.getIdealStateReplicaId() << 1 | (candidate.isOnline() ? 1 : 0));
      }
      _hashCode = hashCode;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof GroupKey)) {
        return false;
      }
      GroupKey that = (GroupKey) o;
      if (_hashCode != that._hashCode
          || _new != that._new
          || _expectedReplicas != that._expectedReplicas
          || _candidates.length != that._candidates.length) {
        return false;
      }
      for (int i = 0; i < _candidates.length; i++) {
        if (_candidates[i] != that._candidates[i]) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return _hashCode;
    }
  }

  /// The ideal state and external view instance state maps of a segment, compared by content. Segments with equal maps
  /// have the same candidates, so the canonical instance (see [InstanceStateMapsCache]) counts them, and the
  /// candidates are computed once per canonical instance.
  private static final class InstanceStateMaps {
    final ContentComparator _contentComparator;
    Map<String, String> _idealStateInstanceStateMap;
    @Nullable
    Map<String, String> _externalViewInstanceStateMap;
    int _hashCode;

    // Set on the canonical instance only
    int _id;
    int _numOldSegments;
    int _numNewSegments;
    @Nullable
    String _sampleOldSegment;
    @Nullable
    String _sampleNewSegment;

    InstanceStateMaps(ContentComparator contentComparator) {
      _contentComparator = contentComparator;
    }

    void set(Map<String, String> idealStateInstanceStateMap,
        @Nullable Map<String, String> externalViewInstanceStateMap, int hashCode) {
      _idealStateInstanceStateMap = idealStateInstanceStateMap;
      _externalViewInstanceStateMap = externalViewInstanceStateMap;
      _hashCode = hashCode;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof InstanceStateMaps)) {
        return false;
      }
      InstanceStateMaps that = (InstanceStateMaps) o;
      // NOTE: The hash code depends on the iteration order of the maps, so maps with the same entries in a different
      //       order make separate pairs. This is correct because every candidate list is built in the order of
      //       sortByKeyIfNeeded(), so such pairs have the same candidates and end up in the same group.
      if (_hashCode != that._hashCode
          || !_contentComparator.isEqual(_idealStateInstanceStateMap, that._idealStateInstanceStateMap)) {
        return false;
      }
      if (_externalViewInstanceStateMap == null || that._externalViewInstanceStateMap == null) {
        return _externalViewInstanceStateMap == that._externalViewInstanceStateMap;
      }
      return _contentComparator.isEqual(_externalViewInstanceStateMap, that._externalViewInstanceStateMap);
    }

    @Override
    public int hashCode() {
      return _hashCode;
    }
  }

  /// Returns one canonical [InstanceStateMaps] per distinct pair of instance state maps.
  ///
  /// Looks the maps up through a probe, and hashes and compares them without iterators, so that looking up the maps of
  /// a segment allocates nothing unless the pair is new. The probe of a new pair becomes its canonical instance.
  private static final class InstanceStateMapsCache {
    final ObjectOpenHashSet<InstanceStateMaps> _cache = new ObjectOpenHashSet<>();
    final List<InstanceStateMaps> _instanceStateMapsList = new ArrayList<>();
    final ContentHasher _contentHasher = new ContentHasher();
    final ContentComparator _contentComparator = new ContentComparator();
    InstanceStateMaps _probe = new InstanceStateMaps(_contentComparator);

    InstanceStateMaps canonicalize(Map<String, String> idealStateInstanceStateMap,
        @Nullable Map<String, String> externalViewInstanceStateMap) {
      int hashCode = 31 * _contentHasher.hash(idealStateInstanceStateMap)
          + (externalViewInstanceStateMap != null ? _contentHasher.hash(externalViewInstanceStateMap) : 0);
      _probe.set(idealStateInstanceStateMap, externalViewInstanceStateMap, hashCode);
      InstanceStateMaps canonical = _cache.addOrGet(_probe);
      if (canonical == _probe) {
        canonical._id = _instanceStateMapsList.size();
        _instanceStateMapsList.add(canonical);
        _probe = new InstanceStateMaps(_contentComparator);
      }
      return canonical;
    }
  }

  /// Computes a hash code of an instance state map from its entries in iteration order, without an entry iterator.
  /// [Map#hashCode()] sums the hash codes of the entries, so maps of instance names that differ in only a few
  /// characters (e.g. `Server_1` to `Server_99`) often collide. This hash code depends on the position of each entry,
  /// so such maps rarely collide.
  @VisibleForTesting
  static final class ContentHasher implements BiConsumer<String, String> {
    int _hashCode;

    int hash(Map<String, String> instanceStateMap) {
      _hashCode = 0;
      instanceStateMap.forEach(this);
      return _hashCode;
    }

    @Override
    public void accept(String instance, @Nullable String state) {
      _hashCode = 31 * (31 * _hashCode + instance.hashCode()) + Objects.hashCode(state);
    }
  }

  /// Compares instance state maps by entries, as [Map#equals(Object)] does, without an entry iterator.
  private static final class ContentComparator implements BiConsumer<String, String> {
    Map<String, String> _other;
    boolean _equal;

    boolean isEqual(Map<String, String> instanceStateMap1, Map<String, String> instanceStateMap2) {
      if (instanceStateMap1 == instanceStateMap2) {
        return true;
      }
      if (instanceStateMap1.size() != instanceStateMap2.size()) {
        return false;
      }
      _other = instanceStateMap2;
      _equal = true;
      instanceStateMap1.forEach(this);
      _other = null;
      return _equal;
    }

    @Override
    public void accept(String instance, @Nullable String state) {
      if (_equal) {
        _equal = state != null
            ? state.equals(_other.get(instance))
            : _other.get(instance) == null && _other.containsKey(instance);
      }
    }
  }
}
