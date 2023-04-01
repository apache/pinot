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
package org.apache.pinot.segment.local.segment.store;

import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class representing index name and type
 */
public class IndexKey implements Comparable<IndexKey> {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexKey.class);

  final String _name;
  final IndexType<?, ?, ?> _type;

  /**
   * @param name column name
   * @param type index type
   */
  public IndexKey(String name, IndexType<?, ?, ?> type) {
    _name = name;
    _type = type;
  }

  /**
   * @throws IllegalArgumentException if there is no index with the given index id
   */
  public static IndexKey fromIndexName(String name, String indexName) {
    IndexType<?, ?, ?> type = IndexService.getInstance().get(indexName);
    return new IndexKey(name, type);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IndexKey indexKey = (IndexKey) o;

    if (!_name.equals(indexKey._name)) {
      return false;
    }
    return _type == indexKey._type;
  }

  @Override
  public int hashCode() {
    int result = _name.hashCode();
    result = 31 * result + _type.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return _name + "." + _type.getId();
  }

  @Override
  public int compareTo(IndexKey o) {
    if (_name.equals(o._name)) {
      return _type.getId().compareTo(o._type.getId());
    }
    return _name.compareTo(o._name);
  }
}
