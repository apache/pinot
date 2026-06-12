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
package org.apache.pinot.spi.data;


/// Naming convention for OPEN_STRUCT materialized columns. Each dense OPEN_STRUCT key is stored as
/// a column named `<openStructColumn>$<key>`. Sparse keys share a single synthetic JSON column
/// named `<openStructColumn>$__sparse__`.
public final class OpenStructNaming {
  public static final String SEPARATOR = "$";
  public static final String SPARSE_SUFFIX = "__sparse__";

  private OpenStructNaming() {
  }

  public static String materializedColumnName(String openStructColumn, String key) {
    return openStructColumn + SEPARATOR + key;
  }

  public static String sparseColumnName(String openStructColumn) {
    return openStructColumn + SEPARATOR + SPARSE_SUFFIX;
  }
}
