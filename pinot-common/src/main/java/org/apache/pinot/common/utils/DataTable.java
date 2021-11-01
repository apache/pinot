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
package org.apache.pinot.common.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Data table is used to transfer data from server to broker.
 */
public interface DataTable {
  // TODO: remove this when we stop supporting DataTable V2.
  String EXCEPTION_METADATA_KEY = "Exception";

  void addException(ProcessingException processingException);

  Map<Integer, String> getExceptions();

  byte[] toBytes()
      throws IOException;

  Map<String, String> getMetadata();

  DataSchema getDataSchema();

  int getNumberOfRows();

  int getInt(int rowId, int colId);

  long getLong(int rowId, int colId);

  float getFloat(int rowId, int colId);

  double getDouble(int rowId, int colId);

  String getString(int rowId, int colId);

  ByteArray getBytes(int rowId, int colId);

  <T> T getObject(int rowId, int colId);

  int[] getIntArray(int rowId, int colId);

  long[] getLongArray(int rowId, int colId);

  float[] getFloatArray(int rowId, int colId);

  double[] getDoubleArray(int rowId, int colId);

  String[] getStringArray(int rowId, int colId);

  enum MetadataValueType {
    INT, LONG, STRING
  }

  /* The MetadataKey is used in V3, where we present metadata as Map<MetadataKey, String>
   * ATTENTION:
   *  - Don't change existing keys.
   *  - Don't remove existing keys.
   *  - Always add new keys to the end.
   *  Otherwise, backward compatibility will be broken.
   */
  enum MetadataKey {
    UNKNOWN("unknown", MetadataValueType.STRING),
    TABLE("table", MetadataValueType.STRING), // NOTE: this key is only used in PrioritySchedulerTest
    NUM_DOCS_SCANNED("numDocsScanned", MetadataValueType.LONG),
    NUM_ENTRIES_SCANNED_IN_FILTER("numEntriesScannedInFilter", MetadataValueType.LONG),
    NUM_ENTRIES_SCANNED_POST_FILTER("numEntriesScannedPostFilter", MetadataValueType.LONG),
    NUM_SEGMENTS_QUERIED("numSegmentsQueried", MetadataValueType.INT),
    NUM_SEGMENTS_PROCESSED("numSegmentsProcessed", MetadataValueType.INT),
    NUM_SEGMENTS_MATCHED("numSegmentsMatched", MetadataValueType.INT),
    NUM_CONSUMING_SEGMENTS_PROCESSED("numConsumingSegmentsProcessed", MetadataValueType.INT),
    MIN_CONSUMING_FRESHNESS_TIME_MS("minConsumingFreshnessTimeMs", MetadataValueType.LONG),
    TOTAL_DOCS("totalDocs", MetadataValueType.LONG),
    NUM_GROUPS_LIMIT_REACHED("numGroupsLimitReached", MetadataValueType.STRING),
    TIME_USED_MS("timeUsedMs", MetadataValueType.LONG),
    TRACE_INFO("traceInfo", MetadataValueType.STRING),
    REQUEST_ID("requestId", MetadataValueType.LONG),
    NUM_RESIZES("numResizes", MetadataValueType.INT),
    RESIZE_TIME_MS("resizeTimeMs", MetadataValueType.LONG),
    THREAD_CPU_TIME_NS("threadCpuTimeNs", MetadataValueType.LONG);

    private static final Map<String, MetadataKey> NAME_TO_ENUM_KEY_MAP = new HashMap<>();
    private final String _name;
    private final MetadataValueType _valueType;

    MetadataKey(String name, MetadataValueType valueType) {
      _name = name;
      _valueType = valueType;
    }

    // getByOrdinal returns an enum key for a given ordinal or null if the key does not exist.
    @Nullable
    public static MetadataKey getByOrdinal(int ordinal) {
      if (ordinal >= MetadataKey.values().length) {
        return null;
      }
      return MetadataKey.values()[ordinal];
    }

    // getByName returns an enum key for a given name or null if the key does not exist.
    public static MetadataKey getByName(String name) {
      return NAME_TO_ENUM_KEY_MAP.getOrDefault(name, null);
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
      for (MetadataKey key : MetadataKey.values()) {
        if (NAME_TO_ENUM_KEY_MAP.put(key.getName(), key) != null) {
          throw new IllegalArgumentException("Duplicate name defined in the MetadataKey definition: " + key.getName());
        }
      }
    }
  }
}
