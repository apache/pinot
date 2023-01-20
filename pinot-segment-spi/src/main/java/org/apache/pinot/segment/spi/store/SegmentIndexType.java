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

/**
 * Different from column index, segment level index involves multiple columns like StarTree index.
 */
public enum SegmentIndexType {
  STAR_TREE_INDEX("star_tree_index");

  private final String _indexName;

  SegmentIndexType(String name) {
    _indexName = name;
  }

  public String getIndexName() {
    return _indexName;
  }

  public static SegmentIndexType getValue(String val) {
    for (SegmentIndexType type : values()) {
      if (type.getIndexName().equalsIgnoreCase(val)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown value: " + val);
  }
}
