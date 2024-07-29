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
package org.apache.pinot.integration.tests.tpch;

import java.io.File;


/**
 * Constants used in TPCH integration tests.
 */
public final class Constants {
  static final String[] TPCH_TABLE_NAMES = {
      "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"
  };
  static final String AVRO_FILE_SUFFIX = ".avro";
  private static final String TPCH_TABLE_RESOURCE_FOLDER_PREFIX = "examples/batch/tpch";
  private Constants() {
  }

  static String getTableResourceFolder(String tableName) {
    return getTableResourceFolder(tableName, false);
  }

  static String getTableResourceFolder(String tableName, boolean useMultiValue) {
    String path = TPCH_TABLE_RESOURCE_FOLDER_PREFIX;
    if (useMultiValue) {
      path += "MultiValue";
    }

    return path + File.separator + tableName;
  }

  static String getTableAvroFilePath(String tableName) {
    return getTableAvroFilePath(tableName, false);
  }

  static String getTableAvroFilePath(String tableName, boolean multiValue) {
    String path = getTableResourceFolder(tableName, multiValue);
    return path + File.separator + "rawdata" + File.separator + tableName + AVRO_FILE_SUFFIX;
  }
}
