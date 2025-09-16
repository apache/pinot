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
package org.apache.pinot.client;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Extended BrokerResponse with cursor-specific fields for cursor pagination queries.
 * This class adds cursor metadata fields while maintaining full compatibility with BrokerResponse.
 */
public class CursorAwareBrokerResponse extends BrokerResponse {
  // Cursor-specific fields from Pinot documentation
  private final Long _offset;
  private final Integer _numRows;
  private final Long _numRowsResultSet;
  private Long _cursorResultWriteTimeMs;
  private Long _submissionTimeMs;
  private Long _expirationTimeMs;
  private final String _brokerHost;
  private Integer _brokerPort;
  private Long _bytesWritten;
  private final Long _cursorFetchTimeMs;

  /**
   * Creates a CursorAwareBrokerResponse by parsing cursor-specific fields from the JSON response.
   */
  public static CursorAwareBrokerResponse fromJson(JsonNode brokerResponse) {
    return new CursorAwareBrokerResponse(brokerResponse);
  }


  private CursorAwareBrokerResponse(JsonNode brokerResponse) {
    super(brokerResponse); // Initialize base BrokerResponse fields

    // Parse cursor-specific fields using helper methods (they handle null nodes gracefully)
    _offset = getLongOrNull(brokerResponse, "offset");
    _numRows = getIntOrNull(brokerResponse, "numRows");
    _numRowsResultSet = getLongOrNull(brokerResponse, "numRowsResultSet");
    _cursorResultWriteTimeMs = getLongOrNull(brokerResponse, "cursorResultWriteTimeMs");
    _expirationTimeMs = getLongOrNull(brokerResponse, "expirationTimeMs");
    _submissionTimeMs = getLongOrNull(brokerResponse, "submissionTimeMs");
    _brokerHost = getTextOrNull(brokerResponse, "brokerHost");
    _brokerPort = getIntOrNull(brokerResponse, "brokerPort");
    _bytesWritten = getLongOrNull(brokerResponse, "bytesWritten");
    _cursorFetchTimeMs = getLongOrNull(brokerResponse, "cursorFetchTimeMs");
  }



  // Cursor-specific field getters
  public Long getOffset() {
    return _offset;
  }

  public Integer getNumRows() {
    return _numRows;
  }

  public Long getNumRowsResultSet() {
    return _numRowsResultSet;
  }

  public Long getCursorResultWriteTimeMs() {
    return _cursorResultWriteTimeMs;
  }

  public Long getSubmissionTimeMs() {
    return _submissionTimeMs;
  }

  public Long getExpirationTimeMs() {
    return _expirationTimeMs;
  }

  public String getBrokerHost() {
    return _brokerHost;
  }

  public Integer getBrokerPort() {
    return _brokerPort;
  }

  public Long getBytesWritten() {
    return _bytesWritten;
  }

  public Long getCursorFetchTimeMs() {
    return _cursorFetchTimeMs;
  }

  // Helper methods for extracting values from JsonNode with null checks
  private static Long getLongOrNull(JsonNode node, String fieldName) {
    if (node == null) {
      return null;
    }
    JsonNode valueNode = node.get(fieldName);
    return (valueNode != null && !valueNode.isNull()) ? valueNode.asLong() : null;
  }

  private static Integer getIntOrNull(JsonNode node, String fieldName) {
    if (node == null) {
      return null;
    }
    JsonNode valueNode = node.get(fieldName);
    return (valueNode != null && !valueNode.isNull()) ? valueNode.asInt() : null;
  }

  private static String getTextOrNull(JsonNode node, String fieldName) {
    if (node == null) {
      return null;
    }
    JsonNode valueNode = node.get(fieldName);
    return (valueNode != null && !valueNode.isNull()) ? valueNode.asText() : null;
  }
}
