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

import java.util.Set;


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

  /**
   * Set the starting offset of result table slice
   * @param offset Offset of the result table slice
   */
  void setOffset(int offset);

  /**
   * Current offset in the query result.
   * Starts from 0.
   * @return current offset.
   */
  int getOffset();

  /**
   * Set the number of rows in the result table slice.
   * @param numRows Number of rows in the result table slice
   */
  void setNumRows(int numRows);

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

  /**
   * Set the time taken to fetch a cursor. The time is specific to the current call.
   * @param cursorFetchTimeMs time in milliseconds
   */
  void setCursorFetchTimeMs(long cursorFetchTimeMs);

  /**
   * Unix timestamp when the query was submitted. The timestamp is used to calculate the expiration time when the
   * response will be deleted from the response store.
   * @param submissionTimeMs Unix timestamp when the query was submitted.
   */
  void setSubmissionTimeMs(long submissionTimeMs);

  /**
   * Get the unix timestamp when the query was submitted
   * @return Submission unix timestamp when the query was submitted
   */
  long getSubmissionTimeMs();

  /**
   * Set the expiration time (unix timestamp) when the response will be deleted from the response store.
   * @param expirationTimeMs unix timestamp when the response expires in the response store
   */
  void setExpirationTimeMs(long expirationTimeMs);

  /**
   * Get the expiration time (unix timestamp) when the response will be deleted from the response store.
   * @return  expirationTimeMs unix timestamp when the response expires in the response store
   */  long getExpirationTimeMs();

  /**
   * Set the number of rows in the result set. This is required because BrokerResponse checks the ResultTable
   * to get the number of rows. However the ResultTable is set to null in CursorResponse. So the numRowsResultSet has to
   * be remembered.
   * @param numRowsResultSet Number of rows in the result set.
   */
  void setNumRowsResultSet(int numRowsResultSet);

  /**
   * Set the number of bytes written to the response store when storing the result table.
   * @param bytesWritten Number of bytes written
   */
  void setBytesWritten(long bytesWritten);

  /**
   * Get the number of bytes written when storing the result table
   * @return number of bytes written
   */
  long getBytesWritten();

  /**
   * Set the table names queried by the SQL query
   * @param tableNames List of table names
   */
  void setTableNames(Set<String> tableNames);

  /**
   * Get the list of table names queried by the SQL query
   * @return Set of table names queried.
   */
  Set<String> getTableNames();
}
