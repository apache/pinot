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
package org.apache.pinot.spi.cursors;

import java.util.Collection;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * ResultStore is a collection of Query Stores.
 * It maintains metadata for all the query stores in a broker.
 * <br/>
 * Concurrency Model:
 * <br/>
 * There are 3 possible roles - writer, reader and delete.
 * <br/>
 * There can only be ONE writer and no other concurrent roles can execute.
 * A query store is written during query execution. During execution, there can be no reads or deletes as the
 * query id would not have been provided to the client.
 * <br/>
 * There can be multiple readers. There maybe concurrent deletes but no concurrent writes.
 * Multiple clients can potentially iterate through the result set.
 * <br/>
 * There can be multiple deletes. There maybe concurrent reads but no concurrent writes.
 * Multiple clients can potentially call the delete API.
 * <br/>
 * Implementations should ensure that concurrent read/delete and delete/delete operations are handled correctly.
 */
public interface ResultStore {
  /**
   * Initialize the store.
   * @param config Configuration of the store.
   */
  void init(PinotConfiguration config, ResponseSerde responseSerde)
      throws Exception;

  /**
   * Checks if the response for a requestId exists.
   * @param requestId The ID of the request
   * @return True if response exists else false
   * @throws Exception Thrown if an error occurs when checking if the response exists.
   */
  boolean exists(String requestId)
    throws Exception;

  /**
   * Get All Query Stores.
   *
   * @return List of QueryStore objects
   */
  Collection<String> getAllStoredRequestIds()
      throws Exception;

  /**
   * Delete a QueryStore for a query id.
   *
   * @param requestId Query id of the query.
   * @return True if response was found and deleted.
   * @throws Exception Exception is thrown if response cannot be deleted by result store.
   */
  boolean deleteResponse(String requestId)
      throws Exception;
}
