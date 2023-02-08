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
package org.apache.pinot.query.runtime.operator.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.datatable.DataTable;


public class OperatorUtils {

  private static final Map<String, String> OPERATOR_TOKEN_MAPPING = new HashMap<>();

  static {
    OPERATOR_TOKEN_MAPPING.put("=", "equals");
    OPERATOR_TOKEN_MAPPING.put(">", "greaterThan");
    OPERATOR_TOKEN_MAPPING.put("<", "lessThan");
    OPERATOR_TOKEN_MAPPING.put("<=", "lessThanOrEqual");
    OPERATOR_TOKEN_MAPPING.put(">=", "greaterThanOrEqual");
    OPERATOR_TOKEN_MAPPING.put("<>", "notEquals");
    OPERATOR_TOKEN_MAPPING.put("!=", "notEquals");
    OPERATOR_TOKEN_MAPPING.put("+", "plus");
    OPERATOR_TOKEN_MAPPING.put("-", "minus");
    OPERATOR_TOKEN_MAPPING.put("*", "times");
    OPERATOR_TOKEN_MAPPING.put("/", "divide");
    OPERATOR_TOKEN_MAPPING.put("||", "concat");
  }

  private OperatorUtils() {
    // do not instantiate.
  }

  /**
   * Canonicalize function name since Logical plan uses Parser.jj extracted tokens.
   * @param functionName input Function name
   * @return Canonicalize form of the input function name
   */
  public static String canonicalizeFunctionName(String functionName) {
    functionName = StringUtils.remove(functionName, " ");
    functionName = OPERATOR_TOKEN_MAPPING.getOrDefault(functionName, functionName);
    return functionName;
  }

  /**
   * aggregate metadata in transferable blocks such as query metrics
   * @param metadataList
   * @return aggregated metadata with keys only used for query metrics. \
   * User should replace existing keys in metadata with this result
   * but should take care of keeping the remaining keys as it is
   */
  public static Map<String, String> aggregateMetadata(List<Map<String, String>> metadataList) {

    long numDocsScanned = 0L;
    long numEntriesScannedInFilter = 0L;
    long numEntriesScannedPostFilter = 0L;
    long numTotalDocs = 0;
    long numSegmentsQueried = 0;
    long numSegmentsProcessed = 0;
    long numSegmentsMatched = 0;
    long numConsumingSegmentsQueried = 0;

    for (Map<String, String> metadata: metadataList) {
      String numDocsScannedString =
          metadata.get(DataTable.MetadataKey.NUM_DOCS_SCANNED.getName());
      if (numDocsScannedString != null) {
        numDocsScanned += Long.parseLong(numDocsScannedString);
      }
      String numEntriesScannedInFilterString =
          metadata.get(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER.getName());
      if (numEntriesScannedInFilterString != null) {
        numEntriesScannedInFilter += Long.parseLong(numEntriesScannedInFilterString);
      }
      String numEntriesScannedPostFilterString =
          metadata.get(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER.getName());
      if (numEntriesScannedPostFilterString != null) {
        numEntriesScannedPostFilter += Long.parseLong(numEntriesScannedPostFilterString);
      }
      String numTotalDocsString =
          metadata.get(DataTable.MetadataKey.TOTAL_DOCS.getName());
      if (numTotalDocsString != null) {
        numTotalDocs += Long.parseLong(numTotalDocsString);
      }

      String numSegmentsQueriedString =
          metadata.get(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED.getName());
      if (numSegmentsQueriedString != null) {
        numSegmentsQueried += Long.parseLong(numSegmentsQueriedString);
      }

      String numSegmentsProcessedString =
          metadata.get(DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED.getName());
      if (numSegmentsProcessedString != null) {
        numSegmentsProcessed += Long.parseLong(numSegmentsProcessedString);
      }

      String numSegmentsMatchedString =
          metadata.get(DataTable.MetadataKey.NUM_SEGMENTS_MATCHED.getName());
      if (numSegmentsMatchedString != null) {
        numSegmentsMatched += Long.parseLong(numSegmentsMatchedString);
      }

      String numConsumingSegmentsQueriedString =
          metadata.get(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED.getName());
      if (numConsumingSegmentsQueriedString != null) {
        numConsumingSegmentsQueried += Long.parseLong(numConsumingSegmentsQueriedString);
      }
    }

    Map<String, String> aggregatedMetadata = new HashMap<>();
    aggregatedMetadata.put(DataTable.MetadataKey.NUM_DOCS_SCANNED.getName(),
        String.valueOf(numDocsScanned));
    aggregatedMetadata.put(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER.getName(),
        String.valueOf(numEntriesScannedInFilter));
    aggregatedMetadata.put(DataTable.MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER.getName(),
        String.valueOf(numEntriesScannedPostFilter));
    aggregatedMetadata.put(DataTable.MetadataKey.TOTAL_DOCS.getName(),
        String.valueOf(numTotalDocs));
    aggregatedMetadata.put(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED.getName(),
        String.valueOf(numSegmentsQueried));
    aggregatedMetadata.put(DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED.getName(),
        String.valueOf(numSegmentsProcessed));
    aggregatedMetadata.put(DataTable.MetadataKey.NUM_SEGMENTS_MATCHED.getName(),
        String.valueOf(numSegmentsMatched));
    aggregatedMetadata.put(DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED.getName(),
        String.valueOf(numConsumingSegmentsQueried));
    return aggregatedMetadata;
  }
}
