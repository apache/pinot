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
package org.apache.pinot.integration.tests.tpch.generator;

import java.util.ArrayList;
import java.util.List;


public class QuerySkeleton {
  private final List<String> _projections;
  private final List<String> _predicates;
  private final List<String> _groupByColumns;
  private final List<String> _orderByColumns;
  private final List<String> _tables;

  public QuerySkeleton() {
    _projections = new ArrayList<>();
    _predicates = new ArrayList<>();
    _groupByColumns = new ArrayList<>();
    _orderByColumns = new ArrayList<>();
    _tables = new ArrayList<>();
  }

  public void addTable(String table) {
    _tables.add(table);
  }

  public void addProjection(String projection) {
    _projections.add(projection);
  }

  public QuerySkeleton addPredicate(String predicate) {
    _predicates.add(predicate);
    return this;
  }

  public QuerySkeleton addGroupByColumn(String groupByColumn) {
    _groupByColumns.add(groupByColumn);
    return this;
  }

  public QuerySkeleton addOrderByColumn(String orderByColumn) {
    _orderByColumns.add(orderByColumn);
    return this;
  }

  @Override
  public String toString() {
    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    _projections.forEach(predicate -> query.append(predicate).append(", "));
    query.delete(query.length() - 2, query.length());

    query.append(" FROM ");
    _tables.forEach(table -> query.append(table).append(", "));
    query.delete(query.length() - 2, query.length());

    if (_predicates.size() > 0) {
      query.append(" WHERE ");
      _predicates.forEach(predicate -> query.append(predicate).append(" AND "));
      query.delete(query.length() - 5, query.length());
    }

    if (_groupByColumns.size() > 0) {
      query.append(" GROUP BY ");
      _groupByColumns.forEach(groupByColumn -> query.append(groupByColumn).append(", "));
      query.delete(query.length() - 2, query.length());
    }

    if (_orderByColumns.size() > 0) {
      query.append(" ORDER BY ");
      _orderByColumns.forEach(orderByColumn -> query.append(orderByColumn).append(", "));
      query.delete(query.length() - 2, query.length());
    }

    return query.toString();
  }
}
