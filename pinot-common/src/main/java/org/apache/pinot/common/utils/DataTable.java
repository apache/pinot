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
import java.util.Map;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Data table is used to transfer data from server to broker.
 */
public interface DataTable {
  String EXCEPTION_METADATA_KEY = "Exception";
  String NUM_DOCS_SCANNED_METADATA_KEY = "numDocsScanned";
  String NUM_ENTRIES_SCANNED_IN_FILTER_METADATA_KEY = "numEntriesScannedInFilter";
  String NUM_ENTRIES_SCANNED_POST_FILTER_METADATA_KEY = "numEntriesScannedPostFilter";
  String NUM_SEGMENTS_QUERIED = "numSegmentsQueried";
  String NUM_SEGMENTS_PROCESSED = "numSegmentsProcessed";
  String NUM_SEGMENTS_MATCHED = "numSegmentsMatched";
  String NUM_CONSUMING_SEGMENTS_PROCESSED = "numConsumingSegmentsProcessed";
  String MIN_CONSUMING_FRESHNESS_TIME_MS = "minConsumingFreshnessTimeMs";
  String TOTAL_DOCS_METADATA_KEY = "totalDocs";
  String NUM_GROUPS_LIMIT_REACHED_KEY = "numGroupsLimitReached";
  String TIME_USED_MS_METADATA_KEY = "timeUsedMs";
  String TRACE_INFO_METADATA_KEY = "traceInfo";
  String REQUEST_ID_METADATA_KEY = "requestId";
  String NUM_RESIZES_METADATA_KEY = "numResizes";
  String RESIZE_TIME_MS_METADATA_KEY = "resizeTimeMs";

  void addException(ProcessingException processingException);

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
}
