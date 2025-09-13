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

import java.util.List;

/**
 * A cursor-based result set group that delegates result set access to an internal ResultSetGroup
 * and provides cursor metadata access. This is a pure data container without navigation logic.
 *
 * <p><strong>Thread Safety:</strong> This class is immutable and thread-safe.
 */
public class CursorResultSetGroup implements BaseResultSetGroup {
  private final ResultSetGroup _resultSetGroup;
  private final CursorAwareBrokerResponse _cursorResponse;

  /**
   * Creates a cursor result set group from a cursor-aware broker response.
   *
   * @param cursorResponse the cursor-aware broker response containing results and cursor metadata
   */
  public CursorResultSetGroup(CursorAwareBrokerResponse cursorResponse) {
    _cursorResponse = cursorResponse;
    _resultSetGroup = new ResultSetGroup(cursorResponse);
  }

  public int getResultSetCount() {
    return _resultSetGroup.getResultSetCount();
  }

  public ResultSet getResultSet(int index) {
    return _resultSetGroup.getResultSet(index);
  }

  public String getCursorId() {
    return _cursorResponse.getRequestId();
  }

  public int getPageSize() {
    Integer numRows = _cursorResponse.getNumRows();
    return numRows != null ? numRows : 0;
  }

  public long getExpirationTimeMs() {
    Long expirationTime = _cursorResponse.getExpirationTimeMs();
    return expirationTime != null ? expirationTime : 0L;
  }

  public Long getOffset() {
    return _cursorResponse.getOffset();
  }

  public Long getNumRowsResultSet() {
    return _cursorResponse.getNumRowsResultSet();
  }

  public Long getCursorResultWriteTimeMs() {
    return _cursorResponse.getCursorResultWriteTimeMs();
  }

  public Long getSubmissionTimeMs() {
    return _cursorResponse.getSubmissionTimeMs();
  }

  public Long getTotalRows() {
    return _cursorResponse.getNumRowsResultSet();
  }

  public String getBrokerHost() {
    return _cursorResponse.getBrokerHost();
  }

  public Integer getBrokerPort() {
    return _cursorResponse.getBrokerPort();
  }

  public Long getBytesWritten() {
    return _cursorResponse.getBytesWritten();
  }

  public Long getCursorFetchTimeMs() {
    return _cursorResponse.getCursorFetchTimeMs();
  }

  public ExecutionStats getExecutionStats() {
    return _resultSetGroup.getExecutionStats();
  }

  public List<PinotClientException> getExceptions() {
    return _resultSetGroup.getExceptions();
  }

  public BrokerResponse getBrokerResponse() {
    return _resultSetGroup.getBrokerResponse();
  }

  /**
   * Gets the cursor-aware broker response for accessing cursor metadata.
   *
   * @return the cursor-aware broker response
   */
  public CursorAwareBrokerResponse getCursorResponse() {
    return _cursorResponse;
  }
}
