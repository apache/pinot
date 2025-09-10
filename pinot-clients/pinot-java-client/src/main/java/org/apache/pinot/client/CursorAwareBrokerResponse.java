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

    // Parse cursor-specific fields
    _offset = brokerResponse.has("offset") ? brokerResponse.get("offset").asLong() : null;
    _numRows = brokerResponse.has("numRows") ? brokerResponse.get("numRows").asInt() : null;
    _numRowsResultSet = brokerResponse.has("numRowsResultSet") ? brokerResponse.get("numRowsResultSet").asLong() : null;
    JsonNode cursorResultWriteTimeMsNode = brokerResponse.get("cursorResultWriteTimeMs");
    _cursorResultWriteTimeMs = cursorResultWriteTimeMsNode != null ? cursorResultWriteTimeMsNode.asLong() : null;
    JsonNode expirationTimeMsNode = brokerResponse.get("expirationTimeMs");
    _expirationTimeMs = expirationTimeMsNode != null ? expirationTimeMsNode.asLong() : null;
    _submissionTimeMs = brokerResponse.has("submissionTimeMs") ? brokerResponse.get("submissionTimeMs").asLong() : null;
    _brokerHost = brokerResponse.has("brokerHost") ? brokerResponse.get("brokerHost").asText() : null;
    _brokerPort = brokerResponse.has("brokerPort") ? brokerResponse.get("brokerPort").asInt() : null;
    _bytesWritten = brokerResponse.has("bytesWritten") ? brokerResponse.get("bytesWritten").asLong() : null;
    _cursorFetchTimeMs = brokerResponse.has("cursorFetchTimeMs")
        ? brokerResponse.get("cursorFetchTimeMs").asLong() : null;
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
}
