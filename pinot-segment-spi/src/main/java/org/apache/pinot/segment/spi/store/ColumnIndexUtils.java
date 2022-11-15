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
package org.apache.pinot.segment.spi.store;

import com.google.common.base.Preconditions;


public class ColumnIndexUtils {
  public static final String MAP_KEY_SEPARATOR = ".";
  public static final String MAP_KEY_NAME_START_OFFSET = "startOffset";
  public static final String MAP_KEY_NAME_SIZE = "size";

  private ColumnIndexUtils() {
    // do not instantiate.
  }

  public static String[] parseIndexMapKeys(String key, String segmentDir) {
    // column names can have '.' in it hence scan from backwards
    // parsing names like "column.name.dictionary.startOffset"
    // or, "column.name.dictionary.endOffset" where column.name is the key
    int lastSeparatorPos = key.lastIndexOf(MAP_KEY_SEPARATOR);
    Preconditions
        .checkState(lastSeparatorPos != -1, "Key separator not found: " + key + ", segment: " + segmentDir);
    String propertyName = key.substring(lastSeparatorPos + 1);

    int indexSeparatorPos = key.lastIndexOf(MAP_KEY_SEPARATOR, lastSeparatorPos - 1);
    Preconditions.checkState(indexSeparatorPos != -1,
        "Index separator not found: " + key + " , segment: " + segmentDir);
    String indexName = key.substring(indexSeparatorPos + 1, lastSeparatorPos);
    String columnName = key.substring(0, indexSeparatorPos);
    return new String[]{columnName, indexName, propertyName};
  }
}
