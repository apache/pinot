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
package org.apache.pinot.broker.queryquota;

public interface QueryQuotaManager {

  /**
   * Try to acquire a quota for the given table.
   * @param tableName Table name with or without type suffix
   * @return {@code true} if the table quota has not been reached, {@code false} otherwise
   */
  boolean acquire(String tableName);

  /**
   * Try to acquire a quota for the given database.
   * @param databaseName database name
   * @return {@code true} if the database quota has not been reached, {@code false} otherwise
   */
  boolean acquireDatabase(String databaseName);

  /**
   * Try to acquire a quota for the given application.
   * @param applicationName application name
   * @return {@code true} if the application quota has not been reached, {@code false} otherwise
   */
  boolean acquireApplication(String applicationName);

  /**
   * Get the QPS quota in effect for the table
   * @param tableNameWithType table name with type
   * @return effective quota qps. 0 if no qps quota is set.
   */
  double getTableQueryQuota(String tableNameWithType);

  /**
   * Get the QPS quota in effect for the database
   * @param databaseName table name with type
   * @return effective quota qps. 0 if no qps quota is set.
   */
  double getDatabaseQueryQuota(String databaseName);

  /**
   * Get the QPS quota in effect for the application
   * @param applicationName table name with type
   * @return effective quota qps. 0 if no qps quota is set.
   */
  double getApplicationQueryQuota(String applicationName);
}
