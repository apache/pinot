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
package org.apache.pinot.common.utils;

import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class DatabaseUtilsTest {
  private static final String LOGICAL_TABLE_NAME = "tb1";
  private static final String DATABASE_NAME = "db1";
  private static final String DEFAULT_DATABASE_NAME = CommonConstants.DEFAULT_DATABASE;
  private static final String FULLY_QUALIFIED_TABLE_NAME = "db1.tb1";


  @Test
  public void translateTableNameTest() {
    // valid cases with non-default database
    check(LOGICAL_TABLE_NAME, DATABASE_NAME, FULLY_QUALIFIED_TABLE_NAME);
    check(FULLY_QUALIFIED_TABLE_NAME, DATABASE_NAME, FULLY_QUALIFIED_TABLE_NAME);
    check(FULLY_QUALIFIED_TABLE_NAME, null, FULLY_QUALIFIED_TABLE_NAME);

    // error cases with non-default database
    error(null, DATABASE_NAME);
    error(FULLY_QUALIFIED_TABLE_NAME + "." + "foo", null);
    error(FULLY_QUALIFIED_TABLE_NAME + "." + "foo", DATABASE_NAME);
    error(FULLY_QUALIFIED_TABLE_NAME, DATABASE_NAME + "foo");

    // valid cases with default database
    check(LOGICAL_TABLE_NAME, null, LOGICAL_TABLE_NAME);
    check(LOGICAL_TABLE_NAME, DEFAULT_DATABASE_NAME, LOGICAL_TABLE_NAME);
    check(DEFAULT_DATABASE_NAME + "." + LOGICAL_TABLE_NAME, null, LOGICAL_TABLE_NAME);
    check(DEFAULT_DATABASE_NAME + "." + LOGICAL_TABLE_NAME, DEFAULT_DATABASE_NAME, LOGICAL_TABLE_NAME);

    // error cases with non-default database
    error(null, DEFAULT_DATABASE_NAME);
    error(FULLY_QUALIFIED_TABLE_NAME, DEFAULT_DATABASE_NAME);
    error(DEFAULT_DATABASE_NAME + "." + LOGICAL_TABLE_NAME, DATABASE_NAME);
    error(DEFAULT_DATABASE_NAME + "." + FULLY_QUALIFIED_TABLE_NAME, null);
    error(DEFAULT_DATABASE_NAME + "." + FULLY_QUALIFIED_TABLE_NAME, DATABASE_NAME);
    error(DEFAULT_DATABASE_NAME + "." + FULLY_QUALIFIED_TABLE_NAME, DEFAULT_DATABASE_NAME);
  }

  private void check(String tableName, String databaseName, String fqn) {
    check(tableName, databaseName, fqn, false);
  }

  private void error(String tableName, String databaseName) {
    check(tableName, databaseName, null, true);
  }

  private void check(String tableName, String databaseName, String fqn, boolean isError) {
    if (isError) {
      try {
        DatabaseUtils.translateTableName(tableName, databaseName);
        fail();
      } catch (IllegalArgumentException ignored) {
        return;
      }
    }
    assertEquals(DatabaseUtils.translateTableName(tableName, databaseName), fqn);
  }
}
