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
package org.apache.pinot.tools.scan.query;

import java.util.List;


public class GroupByOperator {
  List<Object> _columns;

  public GroupByOperator(List<Object> columns) {
    _columns = columns;
  }

  List<Object> _getGroupBys() {
    return _columns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GroupByOperator that = (GroupByOperator) o;
    if (_columns == null && that._columns == null) {
      return true;
    } else if (_columns == null || that._columns == null) {
      return false;
    } else if (_columns.size() != that._columns.size()) {
      return false;
    }

    int i = 0;
    for (Object object : _columns) {
      Object otherObject = that._columns.get(i++);
      if (!object.toString().equals(otherObject.toString())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    if (_columns == null || _columns.isEmpty()) {
      return 0;
    }

    int hashCode = 0;
    for (Object column : _columns) {
      hashCode += 31 * column.hashCode();
    }
    return hashCode;
  }
}
