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

import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class representing index name and type
 */
public class IndexKey {
  private static Logger LOGGER = LoggerFactory.getLogger(IndexKey.class);

  String name;
  ColumnIndexType type;

  /**
   * @param name column name
   * @param type index type
   */
  public IndexKey(String name, ColumnIndexType type) {
    this.name = name;
    this.type = type;
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

    if (!name.equals(indexKey.name)) {
      return false;
    }
    return type == indexKey.type;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + type.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return name + "." + type.getIndexName();
  }
}
