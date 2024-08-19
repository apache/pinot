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
package org.apache.pinot.common.response;


public interface CursorResponse extends BrokerResponse {

  void setBrokerHost(String brokerHost);

    /**
     * get hostname of the processing broker
     * @return String containing the hostname
     */
  String getBrokerHost();

  void setBrokerPort(int brokerPort);

    /**
     * get port of the processing broker
     * @return int containing the port.
     */
  int getBrokerPort();

  void setOffset(int offset);

  void setNumRows(int numRows);

  /**
   * Current offset in the query result.
   * Starts from 0.
   * @return current offset.
   */
  int getOffset();

  /**
   * Number of rows in the current response.
   * @return Number of rows in the current response.
   */
  int getNumRows();

  /**
   * Return the time to write the results to the query store.
   * @return time in milliseconds
   */
  long getCursorResultWriteTimeMs();

  /**
   * Time taken to write cursor results to query storage.
   * @param cursorResultWriteMs Time in milliseconds.
   */
  void setCursorResultWriteTimeMs(long cursorResultWriteMs);

  /**
   * Return the time to fetch results from the query store.
   * @return time in milliseconds.
   */
  long getCursorFetchTimeMs();

  void setCursorFetchTimeMs(long cursorFetchTimeMs);

  void setSubmissionTimeMs(long submissionTimeMs);

  long getSubmissionTimeMs();

  void setExpirationTimeMs(long expirationTimeMs);

  long getExpirationTimeMs();

  void setNumRowsResultSet(int numRowsResultSet);
}
