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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Class to represent segment lineage information.
 *
 * Segment lineage keeps the metadata required for supporting m -> n segment replacement. Segment lineage is serialized
 * into a znode and stored in a helix property store (zookeeper). This metadata will be used by brokers to make sure
 * that the routing does not pick the segments with the duplicate data.
 *
 * NOTE: Update for the underlying segment lineage znode needs to happen with read-modify-write block to guarantee the
 * atomic update because this metadata can be modified concurrently (e.g. task scheduler tries to add entries after
 * scheduling new tasks while minion task tries to update the state of the existing entry)
 */
public class SegmentLineage {
  private static final String COMMA_SEPARATOR = ",";
  private static final String CUSTOM_MAP_KEY = "custom.map";

  private final String _tableNameWithType;
  private final Map<String, LineageEntry> _lineageEntries;
  private Map<String, String> _customMap = null;

  public SegmentLineage(String tableNameWithType) {
    _tableNameWithType = tableNameWithType;
    _lineageEntries = new HashMap<>();
  }

  public SegmentLineage(String tableNameWithType, Map<String, LineageEntry> lineageEntries,
      @Nullable Map<String, String> customMap) {
    _tableNameWithType = tableNameWithType;
    _lineageEntries = lineageEntries;
    _customMap = customMap;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  /**
   * Add lineage entry to the segment lineage metadata with the given lineage entry id
   * @param lineageEntryId the id for the lineage entry
   * @param lineageEntry a lineage entry
   */
  public void addLineageEntry(String lineageEntryId, LineageEntry lineageEntry) {
    Preconditions.checkArgument(!_lineageEntries.containsKey(lineageEntryId),
        String.format("Lineage entry id ('%s') already exists. Please try with the new lineage id", lineageEntryId));
    _lineageEntries.put(lineageEntryId, lineageEntry);
  }

  /**
   * Update lineage entry to the segment lineage metadata with the given lineage entry id
   * @param lineageEntryId the id for the lineage entry to be updated
   * @param lineageEntry a lineage entry to be updated
   */
  public void updateLineageEntry(String lineageEntryId, LineageEntry lineageEntry) {
    Preconditions.checkArgument(_lineageEntries.containsKey(lineageEntryId),
        String.format("Lineage entry id ('%s') does not exists. Please try with the valid lineage id", lineageEntryId));
    _lineageEntries.put(lineageEntryId, lineageEntry);
  }

  public Map<String, LineageEntry> getLineageEntries() {
    return _lineageEntries;
  }

  /**
   * Retrieve lineage entry
   * @param lineageEntryId the id for the lineage entry
   * @return the lineage entry for the given lineage entry id
   */
  public LineageEntry getLineageEntry(String lineageEntryId) {
    return _lineageEntries.get(lineageEntryId);
  }

  /**
   * Retrieve the lineage ids for all lineage entries
   * @return lineage entry ids
   */
  public Set<String> getLineageEntryIds() {
    return new HashSet<>(_lineageEntries.keySet());
  }

  /**
   * Delete lineage entry
   * @param lineageEntryId the id for the lineage entry
   */
  public void deleteLineageEntry(String lineageEntryId) {
    _lineageEntries.remove(lineageEntryId);
  }

  /**
   * Retrieve custom map
   * @return custom map
   */
  public Map<String, String> getCustomMap() {
    return _customMap;
  }

  /**
   * Set custom map
   * @param customMap
   */
  public void setCustomMap(Map<String, String> customMap) {
    _customMap = customMap;
  }

  /**
   * Convert ZNRecord to segment lineage
   * @param record ZNRecord representation of the segment lineage
   * @return the segment lineage object
   */
  public static SegmentLineage fromZNRecord(ZNRecord record) {
    String tableNameWithType = record.getId();
    Map<String, LineageEntry> lineageEntries = new HashMap<>();
    Map<String, List<String>> listFields = record.getListFields();
    Map<String, String> customMap = record.getMapField(CUSTOM_MAP_KEY);
    for (Map.Entry<String, List<String>> listField : listFields.entrySet()) {
      String lineageId = listField.getKey();
      List<String> value = listField.getValue();
      // Tolerant read: legacy entries are 4-tuples. Newer entries append a 5th element
      // ("autoCompleteLineageEntry") only when true; a missing 5th defaults to false. The reader
      // accepts both formats so older controllers/writers and newer ones can interoperate.
      Preconditions.checkState(value.size() >= 4);
      List<String> segmentsFrom = Arrays.asList(StringUtils.split(value.get(0), COMMA_SEPARATOR));
      List<String> segmentsTo = Arrays.asList(StringUtils.split(value.get(1), COMMA_SEPARATOR));
      LineageEntryState state = LineageEntryState.valueOf(value.get(2));
      long timestamp = Long.parseLong(value.get(3));
      boolean autoCompleteLineageEntry = value.size() >= 5 && Boolean.parseBoolean(value.get(4));
      lineageEntries.put(lineageId,
          new LineageEntry(segmentsFrom, segmentsTo, state, timestamp, autoCompleteLineageEntry));
    }
    return new SegmentLineage(tableNameWithType, lineageEntries, customMap);
  }

  /**
   * Convert the segment lineage object to the ZNRecord
   * @return ZNRecord representation of the segment lineage
   */
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    for (Map.Entry<String, LineageEntry> entry : _lineageEntries.entrySet()) {
      LineageEntry lineageEntry = entry.getValue();
      String segmentsFrom = String.join(COMMA_SEPARATOR, lineageEntry.getSegmentsFrom());
      String segmentsTo = String.join(COMMA_SEPARATOR, lineageEntry.getSegmentsTo());
      String state = lineageEntry.getState().toString();
      String timestamp = Long.toString(lineageEntry.getTimestamp());
      // Omit the 5th element when the flag is the default (false): keeps the wire format
      // identical to legacy 4-tuples for every entry that does not opt into observer-driven
      // completion, so older readers continue to parse them.
      List<String> listEntry = lineageEntry.isAutoCompleteLineageEntry() ? Arrays.asList(segmentsFrom, segmentsTo,
          state, timestamp, Boolean.toString(true)) : Arrays.asList(segmentsFrom, segmentsTo, state, timestamp);
      znRecord.setListField(entry.getKey(), listEntry);
    }
    if (_customMap != null) {
      znRecord.setMapField(CUSTOM_MAP_KEY, _customMap);
    }
    return znRecord;
  }

  /**
   * Returns a json representation of the segment lineage.
   * Segment lineage entries are sorted by timestamp, with the entry id as a tiebreaker so that entries
   * sharing the same millisecond are still ordered deterministically (instead of by HashMap iteration).
   */
  public ObjectNode toJsonObject() {
    ObjectNode jsonObject = JsonUtils.newObjectNode();
    jsonObject.put("tableNameWithType", _tableNameWithType);
    LinkedHashMap<String, LineageEntry> sortedLineageEntries = new LinkedHashMap<>();
    _lineageEntries.entrySet().stream()
        .sorted(Map.Entry.<String, LineageEntry>comparingByValue(
                Comparator.comparingLong(LineageEntry::getTimestamp))
            .thenComparing(Map.Entry.comparingByKey()))
        .forEachOrdered(x -> sortedLineageEntries.put(x.getKey(), x.getValue()));
    jsonObject.set("lineageEntries", JsonUtils.objectToJsonNode(sortedLineageEntries));
    if (_customMap != null) {
      jsonObject.set("customMap", JsonUtils.objectToJsonNode(_customMap));
    }
    return jsonObject;
  }
}
