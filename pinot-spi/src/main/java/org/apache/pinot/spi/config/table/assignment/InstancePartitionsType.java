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
package org.apache.pinot.spi.config.table.assignment;

/// The type of the instance partitions.
///
///   The instance partitions name will be of the format `<rawTableName>_<instancePartitionsType>`, e.g.
///   `table_OFFLINE`, `table_CONSUMING`, `table_COMPLETED`.
public enum InstancePartitionsType {
  OFFLINE,    // For (ONLINE) segments from offline table
  CONSUMING,  // For consuming (CONSUMING) segments from LLC real-time table
  COMPLETED;  // For completed (ONLINE) segments from LLC real-time table

  public static final char TYPE_SUFFIX_SEPARATOR = '_';

  public String getInstancePartitionsName(String rawTableName) {
    return rawTableName + TYPE_SUFFIX_SEPARATOR + name();
  }
}
