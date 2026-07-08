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
package org.apache.pinot.client.admin;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Client for page-cache warmup query administration operations.
 *
 * <p>Page-cache warmup lets operators register a curated set of SQL queries that Pinot servers
 * replay against their resident segments (on restart or after a segment refresh) to fault the
 * hottest pages into the OS page cache before real traffic arrives. The queries are stored on the
 * controller under {@code controller.page.cache.warmup.queries.dataDir} and exposed through the
 * {@code /pagecache/queries/{tableName}} REST endpoints. This client wraps those endpoints.
 */
public class PageCacheWarmupAdminClient extends BaseServiceAdminClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(PageCacheWarmupAdminClient.class);

  public PageCacheWarmupAdminClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    super(transport, controllerAddress, headers);
  }

  private static String warmupQueriesPath(String tableName) {
    return "/pagecache/queries/" + tableName;
  }

  /**
   * Stores (or overwrites) the page-cache warmup query list for the given table.
   *
   * @param tableName raw table name (no type suffix)
   * @param tableType table type, {@code OFFLINE} or {@code REALTIME}
   * @param queries ordered list of SQL warmup queries (must be non-empty per the controller contract)
   * @return {@code true} if the controller accepted and stored the queries
   * @throws PinotAdminException if the request fails (e.g. validation error or I/O failure)
   */
  public boolean storeWarmupQueries(String tableName, String tableType, List<String> queries)
      throws PinotAdminException {
    /// POST /pagecache/queries/{tableName}?tableType=... with a JSON array body. The controller
    /// returns a 2xx with a plain-text confirmation on success and a non-2xx (surfaced as a
    /// PinotAdminException by the transport) otherwise.
    _transport.executePost(_controllerAddress, warmupQueriesPath(tableName), queries,
        Map.of("tableType", tableType), _headers);
    return true;
  }

  /**
   * Retrieves the stored page-cache warmup query list for the given table.
   *
   * @param tableName raw table name (no type suffix)
   * @param tableType table type, {@code OFFLINE} or {@code REALTIME}
   * @return the stored warmup queries in the order they were registered
   * @throws PinotAdminException if the request fails (a {@link PinotAdminNotFoundException} is thrown
   *     when no queries have been stored for the table)
   */
  public List<String> getWarmupQueries(String tableName, String tableType)
      throws PinotAdminException {
    /// GET /pagecache/queries/{tableName}?tableType=... returns a bare JSON array of SQL strings.
    JsonNode response = _transport.executeGet(_controllerAddress, warmupQueriesPath(tableName),
        Map.of("tableType", tableType), _headers);
    return PinotAdminTransport.parseStringArrayNode(response);
  }

  /**
   * Deletes the stored page-cache warmup query list for the given table. The controller treats a
   * delete of a non-existent query file as a no-op success.
   *
   * @param tableName raw table name (no type suffix)
   * @param tableType table type, {@code OFFLINE} or {@code REALTIME}
   * @throws PinotAdminException if the request fails
   */
  public void deleteWarmupQueries(String tableName, String tableType)
      throws PinotAdminException {
    LOGGER.info("Deleting page-cache warmup queries for table: {} of type: {}", tableName, tableType);
    _transport.executeDelete(_controllerAddress, warmupQueriesPath(tableName),
        Map.of("tableType", tableType), _headers);
  }
}
