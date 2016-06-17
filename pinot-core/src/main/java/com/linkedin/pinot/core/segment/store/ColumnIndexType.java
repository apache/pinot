/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.store;

public enum ColumnIndexType {
  DICTIONARY("dictionary"),
  FORWARD_INDEX("forward_index"),
  INVERTED_INDEX("inverted_index");

  private final String indexName;
  ColumnIndexType(String name) {
    indexName = name;
  }

  public String getIndexName() {
    return indexName;
  }

  public static ColumnIndexType getValue(String val) {
    for (ColumnIndexType type : values()) {
      if (type.getIndexName().equalsIgnoreCase(val)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown value: " + val);
  }
}
