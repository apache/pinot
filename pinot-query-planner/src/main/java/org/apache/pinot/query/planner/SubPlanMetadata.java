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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.util.Pair;


public class SubPlanMetadata {
  public static final String SUBPLAN_ID_KEY = "subplanId";
  private final Map<String, String> _metadata;

  private final Set<String> _tableNames = new HashSet<>();

  // Only valid for SubPlan 0, which surfaces the fields from the original query
  private List<Pair<Integer, String>> _fields;

  public SubPlanMetadata() {
    this(new HashMap<>());
  }

  public SubPlanMetadata(Map<String, String> metadata) {
    _metadata = metadata;
  }

  public Map<String, String> getMetadata() {
    return _metadata;
  }

  public List<Pair<Integer, String>> getFields() {
    return _fields;
  }

  public void setFields(List<Pair<Integer, String>> fields) {
    _fields = fields;
  }

  public Set<String> getTableNames() {
    return _tableNames;
  }

  public void setTableNames(Set<String> tableNames) {
    _tableNames.clear();
    _tableNames.addAll(tableNames);
  }
}
