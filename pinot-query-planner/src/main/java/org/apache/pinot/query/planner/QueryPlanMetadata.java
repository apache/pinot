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
package org.apache.pinot.query.planner;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.util.Pair;


/**
 * QueryPlanMetadata contains the metadata of the {@code QueryPlan}.
 * It contains the table names and the fields of the query result.
 */
public class QueryPlanMetadata {
  private final Set<String> _tableNames;
  private final List<Pair<Integer, String>> _fields;
  private final Map<String, String> _customProperties;

  public QueryPlanMetadata(Set<String> tableNames, ImmutableList<Pair<Integer, String>> fields) {
    _tableNames = tableNames;
    _fields = fields;
    _customProperties = new HashMap<>();
  }

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }

  /**
   * Get the table names.
   * @return table names.
   */
  public Set<String> getTableNames() {
    return _tableNames;
  }

  /**
   * Get the query result field.
   * @return query result field.
   */
  public List<Pair<Integer, String>> getFields() {
    return _fields;
  }
}
