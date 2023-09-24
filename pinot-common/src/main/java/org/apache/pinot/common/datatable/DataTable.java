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
package org.apache.pinot.common.datatable;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.data.readers.Vector;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 * Data table is used to transfer data from server to broker.
 */
public interface DataTable {
  // TODO: remove this when we stop supporting DataTable V2.
  String EXCEPTION_METADATA_KEY = "Exception";

  void addException(ProcessingException processingException);

  void addException(int exceptionCode, String exceptionMsg);

  Map<Integer, String> getExceptions();

  int getVersion();

  byte[] toBytes()
      throws IOException;

  Map<String, String> getMetadata();

  DataSchema getDataSchema();

  int getNumberOfRows();

  int getInt(int rowId, int colId);

  long getLong(int rowId, int colId);

  float getFloat(int rowId, int colId);

  double getDouble(int rowId, int colId);

  BigDecimal getBigDecimal(int rowId, int colId);

  String getString(int rowId, int colId);

  ByteArray getBytes(int rowId, int colId);

  Vector getVector(int rowId, int columnId);

  int[] getIntArray(int rowId, int colId);

  long[] getLongArray(int rowId, int colId);

  float[] getFloatArray(int rowId, int colId);

  double[] getDoubleArray(int rowId, int colId);

  String[] getStringArray(int rowId, int colId);

  @Nullable
  CustomObject getCustomObject(int rowId, int colId);

  @Nullable
  RoaringBitmap getNullRowIds(int colId);

  DataTable toMetadataOnlyDataTable();

  DataTable toDataOnlyDataTable();

  enum MetadataValueType {
    INT, LONG, STRING
  }

  /* The MetadataKey is used since V3, where we present metadata as Map<MetadataKey, String>
   * ATTENTION:
   *  - DO NOT change the id for the existing keys
   *  - To add a new key, add it with the current MAX_ID, and increase MAX_ID
   *  - NEVER decrease MAX_ID
   *  Otherwise, backward compatibility will be broken.
   */
  enum MetadataKey {
    UNKNOWN(0, "unknown", MetadataValueType.STRING),
    TABLE(1, "table", MetadataValueType.STRING),
    NUM_DOCS_SCANNED(2, "numDocsScanned", MetadataValueType.LONG),
    NUM_ENTRIES_SCANNED_IN_FILTER(3, "numEntriesScannedInFilter", MetadataValueType.LONG),
    NUM_ENTRIES_SCANNED_POST_FILTER(4, "numEntriesScannedPostFilter", MetadataValueType.LONG),
    NUM_SEGMENTS_QUERIED(5, "numSegmentsQueried", MetadataValueType.INT),
    NUM_SEGMENTS_PROCESSED(6, "numSegmentsProcessed", MetadataValueType.INT),
    NUM_SEGMENTS_MATCHED(7, "numSegmentsMatched", MetadataValueType.INT),
    NUM_CONSUMING_SEGMENTS_QUERIED(8, "numConsumingSegmentsQueried", MetadataValueType.INT),
    MIN_CONSUMING_FRESHNESS_TIME_MS(9, "minConsumingFreshnessTimeMs", MetadataValueType.LONG),
    TOTAL_DOCS(10, "totalDocs", MetadataValueType.LONG),
    NUM_GROUPS_LIMIT_REACHED(11, "numGroupsLimitReached", MetadataValueType.STRING),
    TIME_USED_MS(12, "timeUsedMs", MetadataValueType.LONG),
    TRACE_INFO(13, "traceInfo", MetadataValueType.STRING),
    REQUEST_ID(14, "requestId", MetadataValueType.LONG),
    NUM_RESIZES(15, "numResizes", MetadataValueType.INT),
    RESIZE_TIME_MS(16, "resizeTimeMs", MetadataValueType.LONG),
    THREAD_CPU_TIME_NS(17, "threadCpuTimeNs", MetadataValueType.LONG),
    SYSTEM_ACTIVITIES_CPU_TIME_NS(18, "systemActivitiesCpuTimeNs", MetadataValueType.LONG),
    RESPONSE_SER_CPU_TIME_NS(19, "responseSerializationCpuTimeNs", MetadataValueType.LONG),
    NUM_SEGMENTS_PRUNED_BY_SERVER(20, "numSegmentsPrunedByServer", MetadataValueType.INT),
    NUM_SEGMENTS_PRUNED_INVALID(21, "numSegmentsPrunedByInvalid", MetadataValueType.INT),
    NUM_SEGMENTS_PRUNED_BY_LIMIT(22, "numSegmentsPrunedByLimit", MetadataValueType.INT),
    NUM_SEGMENTS_PRUNED_BY_VALUE(23, "numSegmentsPrunedByValue", MetadataValueType.INT),
    EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS(24, "explainPlanNumEmptyFilterSegments", MetadataValueType.INT),
    EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS(25, "explainPlanNumMatchAllFilterSegments", MetadataValueType.INT),
    NUM_CONSUMING_SEGMENTS_PROCESSED(26, "numConsumingSegmentsProcessed", MetadataValueType.INT),
    NUM_CONSUMING_SEGMENTS_MATCHED(27, "numConsumingSegmentsMatched", MetadataValueType.INT),
    NUM_BLOCKS(28, "numBlocks", MetadataValueType.INT),
    NUM_ROWS(29, "numRows", MetadataValueType.INT),
    OPERATOR_EXECUTION_TIME_MS(30, "operatorExecutionTimeMs", MetadataValueType.LONG),
    OPERATOR_ID(31, "operatorId", MetadataValueType.STRING),
    OPERATOR_EXEC_START_TIME_MS(32, "operatorExecStartTimeMs", MetadataValueType.LONG),
    OPERATOR_EXEC_END_TIME_MS(33, "operatorExecEndTimeMs", MetadataValueType.LONG);

    // We keep this constant to track the max id added so far for backward compatibility.
    // Increase it when adding new keys, but NEVER DECREASE IT!!!
    private static final int MAX_ID = 33;

    private static final MetadataKey[] ID_TO_ENUM_KEY_MAP = new MetadataKey[MAX_ID + 1];
    private static final Map<String, MetadataKey> NAME_TO_ENUM_KEY_MAP = new HashMap<>();

    private final int _id;
    private final String _name;
    private final MetadataValueType _valueType;

    MetadataKey(int id, String name, MetadataValueType valueType) {
      _id = id;
      _name = name;
      _valueType = valueType;
    }

    /**
     * Returns the MetadataKey for the given id, or {@code null} if the id does not exist.
     */
    @Nullable
    public static MetadataKey getById(int id) {
      if (id >= 0 && id < ID_TO_ENUM_KEY_MAP.length) {
        return ID_TO_ENUM_KEY_MAP[id];
      }
      return null;
    }

    /**
     * Returns the MetadataKey for the given name, or {@code null} if the name does not exist.
     */
    @Nullable
    public static MetadataKey getByName(String name) {
      return NAME_TO_ENUM_KEY_MAP.get(name);
    }

    public int getId() {
      return _id;
    }

    // getName returns the associated name(string) of the enum key.
    public String getName() {
      return _name;
    }

    // getValueType returns the value type(int/long/String) of the enum key.
    public MetadataValueType getValueType() {
      return _valueType;
    }

    static {
      MetadataKey[] values = values();
      for (MetadataKey key : values) {
        int id = key.getId();
        Preconditions.checkArgument(id >= 0 && id <= MAX_ID,
            "Invalid id: %s for MetadataKey: %s, must be in the range of [0, MAX_ID(%s)]", id, key, MAX_ID);

        Preconditions.checkArgument(ID_TO_ENUM_KEY_MAP[id] == null,
            "Duplicate id: %s defined for MetadataKey: %s and %s", id, ID_TO_ENUM_KEY_MAP[id], key);
        ID_TO_ENUM_KEY_MAP[id] = key;

        String name = key.getName();
        MetadataKey oldKey = NAME_TO_ENUM_KEY_MAP.put(name, key);
        Preconditions.checkArgument(oldKey == null, "Duplicate name: %s defined for MetadataKey: %s and %s", name,
            oldKey, key);
      }
    }
  }
}
