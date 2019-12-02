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
package org.apache.pinot.common.lineage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to represent segment merge lineage information.
 *
 * Segment merge lineage information is serialized into a znode and stored in a helix property store (zookeeper). This
 * information will be used by the broker, segment merge task generator, and retention manager.
 *
 * For each segment group, we are storing the following information:
 * 1. group id
 *    - group identifier (will be stored in time based uuid format)
 * 2. group level
 *    - segment level allows us to have a hierarchical representation of the segment lineage. When we assign the merge
 *      task, we will only merge/roll-up segments with the same level.
 *      (e.g. If hourly segment groups are in level 0, daily segment groups will belong to level 1)
 * 3. segments
 *    - segments that belong to a particular segment group
 * 4. lineage information
 *    - If a segment group is created by merging multiple children segment groups, we write the lineage information
 *      (e.g. segment group C is merged from segment group A, B)
 *
 *
 * Example)
 * Let's say that we have 3 segments to begin with (S1, S2, S3). For original segments, we treat them specially by
 * regarding each segment as a group. So, our lineage info will have the following data.
 *
 * _parentGroupToChildrenGroupsMap = empty
 * _levelToGroupToSegmentsMap = { level_0 -> { G1 -> S1, G2 -> S2, G3 -> S3} }
 *
 * Let's say that a merge task merges S1, S2, S3 into S4, S5. Then, we now have a new group G4 (S4,S5). The lineage
 * now would be the following:
 *
 * _parentGroupToChildrenGroupsMap = { G4 -> { G1, G2, G3} }
 * _levelToGroupToSegmentsMap = { level_0 -> { G1 -> S1, G2 -> S2, G3 -> S3}
 *                                level_1 -> { G4 -> S4, S5} }
 *
 */
public class SegmentMergeLineage {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMergeLineage.class);

  private static final String LEVEL_KEY_PREFIX = "level_";
  private static final String ROOT_NODE_GROUP_ID = "root";
  private static final String SEGMENT_DELIMITER = ",";
  private static final int DEFAULT_GROUP_LEVEL = 0;

  private String _tableNameWithType;

  // Mapping of group id to children group ids
  private Map<String, List<String>> _parentGroupToChildrenGroupsMap;

  // Mapping of group level to group id to segments that belong to a group
  private Map<Integer, Map<String, List<String>>> _levelToGroupToSegmentsMap;

  public SegmentMergeLineage(String tableNameWithType) {
    _tableNameWithType = tableNameWithType;
    _parentGroupToChildrenGroupsMap = new HashMap<>();
    _levelToGroupToSegmentsMap = new HashMap<>();
  }

  public SegmentMergeLineage(String tableNameWithType, Map<String, List<String>> segmentGroupLineageMap,
      Map<Integer, Map<String, List<String>>> levelToGroupToSegmentMap) {
    _tableNameWithType = tableNameWithType;
    _parentGroupToChildrenGroupsMap = segmentGroupLineageMap;
    _levelToGroupToSegmentsMap = levelToGroupToSegmentMap;
  }

  public String getTableName() {
    return _tableNameWithType;
  }

  public static SegmentMergeLineage fromZNRecord(ZNRecord record) {
    String tableNameWithType = record.getId();
    Map<String, List<String>> segmentGroupLineageMap = record.getListFields();

    Map<Integer, Map<String, List<String>>> groupToSegmentsMap = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : record.getMapFields().entrySet()) {
      String levelKey = entry.getKey();
      Integer level = Integer.parseInt(levelKey.substring(LEVEL_KEY_PREFIX.length()));
      Map<String, List<String>> groupToSegmentsForLevel = new HashMap<>();
      for (Map.Entry<String, String> groupEntry : entry.getValue().entrySet()) {
        String groupId = groupEntry.getKey();
        String segmentsString = groupEntry.getValue();
        List<String> segments = Arrays.asList(segmentsString.split(SEGMENT_DELIMITER));
        groupToSegmentsForLevel.put(groupId, new ArrayList<>(segments));
      }
      groupToSegmentsMap.put(level, groupToSegmentsForLevel);
    }
    return new SegmentMergeLineage(tableNameWithType, segmentGroupLineageMap, groupToSegmentsMap);
  }

  public ZNRecord toZNRecord() {
    ZNRecord record = new ZNRecord(_tableNameWithType);
    record.setListFields(_parentGroupToChildrenGroupsMap);
    Map<String, Map<String, String>> groupToSegmentsMap = new HashMap<>();

    for (Map.Entry<Integer, Map<String, List<String>>> entry : _levelToGroupToSegmentsMap.entrySet()) {
      String key = LEVEL_KEY_PREFIX + entry.getKey();
      Map<String, String> groupSegmentsForLevel = new HashMap<>();
      for (Map.Entry<String, List<String>> groupEntry : entry.getValue().entrySet()) {
        String groupId = groupEntry.getKey();
        String segments = String.join(SEGMENT_DELIMITER, groupEntry.getValue());
        groupSegmentsForLevel.put(groupId, segments);
      }
      groupToSegmentsMap.put(key, groupSegmentsForLevel);
    }
    record.setMapFields(groupToSegmentsMap);

    return record;
  }

  /**
   * Add segment merge lineage information
   *
   * @param groupId a group id
   * @param currentGroupSegments a list of segments that belongs to the group
   * @param childrenGroups a list of children groups that the current group covers. All children group ids has to be
   *                       from the same group level.
   */
  public void addSegmentGroup(String groupId, List<String> currentGroupSegments, List<String> childrenGroups)
      throws InvalidConfigException {
    // Get group level
    Integer groupLevel = getGroupLevel(childrenGroups);

    Map<String, List<String>> groupToSegmentMap =
        _levelToGroupToSegmentsMap.computeIfAbsent(groupLevel, k -> new HashMap<>());
    if (groupToSegmentMap.containsKey(groupId) || _parentGroupToChildrenGroupsMap.containsKey(groupId)) {
      throw new InvalidConfigException("Group id : " + groupId + " already exists for table " + _tableNameWithType);
    }

    // Update group to segments map
    groupToSegmentMap.put(groupId, new ArrayList<>(currentGroupSegments));
    _levelToGroupToSegmentsMap.put(groupLevel, groupToSegmentMap);

    // Update segment group lineage map
    if (groupLevel > DEFAULT_GROUP_LEVEL) {
      _parentGroupToChildrenGroupsMap.put(groupId, new ArrayList<>(childrenGroups));
    }

    LOGGER.info("New group has been added successfully to the segment lineage. (tableName: {}, groupId: {}, "
            + "currentGroupSegments: {}, childrenGroups: {}", _tableNameWithType, groupId, currentGroupSegments,
        childrenGroups);
  }

  /**
   * Remove segment merge information given a group id
   *
   * @param groupId a group id
   */
  public void removeSegmentGroup(String groupId) {
    // Clean up the group id from parent to children group mapping
    _parentGroupToChildrenGroupsMap.remove(groupId);
    for (List<String> childrenGroups : _parentGroupToChildrenGroupsMap.values()) {
      childrenGroups.remove(groupId);
    }

    // Clean up the group id from group to segments mapping
    for (Map<String, List<String>> groupToSegments : _levelToGroupToSegmentsMap.values()) {
      groupToSegments.remove(groupId);
    }

    LOGGER.info("Group {} has been successfully removed for table {}.", groupId, _tableNameWithType);
  }

  /**
   * Construct a lineage tree and returns the root node
   *
   * @return a root node for lineage tree
   */
  public SegmentGroup getMergeLineageRootSegmentGroup() {
    // Create group nodes
    Map<String, SegmentGroup> groupNodes = new HashMap<>();
    for (Map.Entry<Integer, Map<String, List<String>>> groupEntryForLevel : _levelToGroupToSegmentsMap.entrySet()) {
      Integer level = groupEntryForLevel.getKey();
      Map<String, List<String>> groupToSegmentsForLevel = groupEntryForLevel.getValue();
      for (Map.Entry<String, List<String>> entry : groupToSegmentsForLevel.entrySet()) {
        String groupId = entry.getKey();
        List<String> segments = entry.getValue();
        SegmentGroup groupNode = new SegmentGroup();
        groupNode.setGroupId(groupId);
        groupNode.setSegments(new HashSet<>(segments));
        groupNode.setGroupLevel(level);
        groupNodes.put(groupId, groupNode);
      }
    }

    // Add edges by updating children & parent group information
    for (Map.Entry<String, List<String>> lineageEntry : _parentGroupToChildrenGroupsMap.entrySet()) {
      String parentGroupId = lineageEntry.getKey();
      List<String> childrenGroupIds = lineageEntry.getValue();
      List<SegmentGroup> childrenGroups = new ArrayList<>();
      SegmentGroup parentNode = groupNodes.get(parentGroupId);
      for (String groupId : childrenGroupIds) {
        SegmentGroup childNode = groupNodes.get(groupId);
        if (childNode != null) {
          childrenGroups.add(childNode);
          childNode.setParentGroup(parentNode);
        }
      }
      parentNode.setChildrenGroups(childrenGroups);
    }

    // Create a root node
    SegmentGroup root = new SegmentGroup();
    root.setGroupId(ROOT_NODE_GROUP_ID);
    List<SegmentGroup> childrenForRoot = new ArrayList<>();
    for (SegmentGroup group : groupNodes.values()) {
      if (group.getParentGroup() == null) {
        group.setParentGroup(root);
        childrenForRoot.add(group);
      }
    }
    root.setChildrenGroups(childrenForRoot);

    return root;
  }

  /**
   * Get a list of segments for a given group id
   *
   * @param groupId a group id
   * @return a list of segments that belongs to the given group id, null if the group does not exist
   */
  public List<String> getSegmentsForGroup(String groupId) {
    for (Map<String, List<String>> groupToSegmentMap : _levelToGroupToSegmentsMap.values()) {
      List<String> segments = groupToSegmentMap.get(groupId);
      if (segments != null) {
        return segments;
      }
    }
    return null;
  }

  /**
   * Get a list of children group ids for a given group id
   *
   * @param groupId a group id
   * @return a list of children groups that are covered by the given group id, null if the group does not exist
   */
  public List<String> getChildrenForGroup(String groupId) {
    return _parentGroupToChildrenGroupsMap.get(groupId);
  }

  /**
   * Get a list of all group levels
   *
   * @return a list of all group levels
   */
  public List<Integer> getAllGroupLevels() {
    List<Integer> groupLevels = new ArrayList<>(_levelToGroupToSegmentsMap.keySet());
    Collections.sort(groupLevels);
    return groupLevels;
  }

  /**
   * Get a list of group ids for a given group level
   *
   * @param groupLevel a group level
   * @return a list of group ids that belongs to the given group level, null if the group level does not exist
   */
  public List<String> getGroupIdsForGroupLevel(int groupLevel) {
    Map<String, List<String>> groupToSegmentsMap = _levelToGroupToSegmentsMap.get(groupLevel);
    if (groupToSegmentsMap != null) {
      return new ArrayList<>(groupToSegmentsMap.keySet());
    }
    return null;
  }

  /**
   * Helper function to compute group level given children groups
   *
   * @param childrenGroups a list of children group ids
   * @return group level
   */
  private Integer getGroupLevel(List<String> childrenGroups)
      throws InvalidConfigException {
    // If no children exists, the group belongs to the base level.
    if (childrenGroups == null || childrenGroups.isEmpty()) {
      return DEFAULT_GROUP_LEVEL;
    }

    for (Map.Entry<Integer, Map<String, List<String>>> entry : _levelToGroupToSegmentsMap.entrySet()) {
      Integer currentLevel = entry.getKey();
      Map<String, List<String>> currentLevelGroupToSegmentsMap = entry.getValue();
      if (currentLevelGroupToSegmentsMap.keySet().containsAll(childrenGroups)) {
        return currentLevel + 1;
      }
    }

    // At this point, not all children groups are covered, cannot add group
    throw new InvalidConfigException("Cannot compute group level because not all children groups exist "
        + "in the segment merge lineage, table name: " + _tableNameWithType + ", children groups: " + childrenGroups
        + "table");
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    SegmentMergeLineage that = (SegmentMergeLineage) o;

    return EqualityUtils.isEqual(_tableNameWithType, that._tableNameWithType) && EqualityUtils
        .isEqual(_parentGroupToChildrenGroupsMap, that._parentGroupToChildrenGroupsMap) && EqualityUtils
        .isEqual(_levelToGroupToSegmentsMap, that._levelToGroupToSegmentsMap);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_tableNameWithType);
    result = EqualityUtils.hashCodeOf(result, _parentGroupToChildrenGroupsMap);
    result = EqualityUtils.hashCodeOf(result, _levelToGroupToSegmentsMap);
    return result;
  }
}
