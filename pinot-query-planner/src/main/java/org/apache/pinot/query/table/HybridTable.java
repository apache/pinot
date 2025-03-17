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
package org.apache.pinot.query.table;

import java.util.List;
import javax.annotation.Nullable;


/**
 * The HybridTable interface represents a table that consists of both offline and realtime tables.
 * An Offline and a realtime table is represented by PhysicalTable objects.
 * In the general case, a HybridTable can have many offline and realtime tables.
 * It is used to organize metadata about hybrid tables and helps with query planning in SSE.
 */
public interface HybridTable {

  /**
   * Checks if the table exists. A table exists if all required TableConfig objects are available.
   *
   * @return true if the table exists, false otherwise
   */
  boolean isExists();

  /**
   * Checks if the broker has routes for at least 1 of the physical tables.
   *
   * @return true if any route exists, false otherwise
   */
  boolean isRouteExists();

  /**
   * Checks if routes for all the offline table(s) is available in the broker.
   *
   * @return true if an offline table route exists, false otherwise
   */
  boolean isOfflineRouteExists();

  /**
   * Checks if routes for all realtime table(s) is available in the broker.
   *
   * @return true if a realtime table route exists, false otherwise
   */
  boolean isRealtimeRouteExists();

  /**
   * Checks if all the physical tables are disabled.
   *
   * @return true if the table is disabled, false otherwise
   */
  boolean isDisabled();

  /**
   * Checks if there are both offline and realtime tables.
   *
   * @return true if the table is a hybrid table, false otherwise
   */
  boolean isHybrid();

  /**
   * Checks if the table has offline tables only
   *
   * @return true if the table has offline tables only, false otherwise
   */
  boolean isOffline();

  /**
   * Checks if the table has realtime tables only.
   *
   * @return true if the table table has realtime tables only, false otherwise
   */
  boolean isRealtime();

  /**
   * Checks if the table has at least 1 offline table. It may or may not have realtime tables as well.
   *
   * @return true if the table has at least 1 offline table, false otherwise
   */
  boolean hasOffline();

  /**
   * Checks if the table has at least 1 realtime table. It may or may not have offline tables as well.
   *
   * @return true if the table at least 1 realtime table, false otherwise
   */
  boolean hasRealtime();

  /**
   * Retrieves the list of disabled physical tables.
   *
   * @return a list of disabled physical tables
   */
  List<PhysicalTable> getDisabledTables();

  /**
   * Retrieves the list of all physical tables.
   *
   * @return a list of all physical tables
   */
  List<PhysicalTable> getAllPhysicalTables();

  /**
   * Retrieves the list of offline physical tables.
   *
   * @return a list of offline physical tables
   */
  List<PhysicalTable> getOfflineTables();

  /**
   * Retrieves the list of realtime physical tables.
   *
   * @return a list of realtime physical tables
   */
  List<PhysicalTable> getRealtimeTables();

  /**
   * Retrieves the primary offline physical table.
   *
   * @return the offline physical table, or null if not available
   */
  @Nullable
  PhysicalTable getOfflineTable();

  /**
   * Retrieves the primary realtime physical table.
   *
   * @return the realtime physical table, or null if not available
   */
  @Nullable
  PhysicalTable getRealtimeTable();
}
