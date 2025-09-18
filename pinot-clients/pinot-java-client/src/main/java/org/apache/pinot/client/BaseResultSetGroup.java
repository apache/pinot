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
 * Base interface for result set groups that provides common contract for accessing query results.
 *
 * This interface defines the core methods that all result set group implementations should provide,
 * allowing for polymorphic usage across different result set group types (regular and cursor-based).
 *
 * <p><strong>Backward Compatibility:</strong> This interface is designed to be implemented by existing
 * classes without breaking any existing code.
 */
public interface BaseResultSetGroup {

  /**
   * Returns the number of result sets in this result set group.
   *
   * There is one result set per aggregation function in the original query
   * and one result set in the case of a selection query.
   *
   * @return The number of result sets, or 0 if there are no result sets
   */
  int getResultSetCount();

  /**
   * Obtains the result set at the given index, starting from zero.
   *
   * @param index The index for which to obtain the result set
   * @return The result set at the given index
   * @throws IndexOutOfBoundsException if the index is out of range
   */
  ResultSet getResultSet(int index);

  /**
   * Gets the execution statistics for this query.
   *
   * @return The execution statistics
   */
  ExecutionStats getExecutionStats();

  /**
   * Gets any exceptions that occurred during query processing.
   *
   * @return A list of exceptions, or an empty list if no exceptions occurred
   */
  List<PinotClientException> getExceptions();

  /**
   * Gets the underlying broker response.
   *
   * @return The broker response
   */
  BrokerResponse getBrokerResponse();
}
