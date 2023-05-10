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

import java.util.List;
import java.util.Set;
import org.apache.calcite.util.Pair;


/**
 * Metadata for a subplan. This class won't leave the query planner/broker side.
 */
public class SubPlanMetadata {

  /**
   * The set of tables that are scanned in this subplan.
   */
  private final Set<String> _tableNames;

  /**
   * The list of fields that are surfaced by this subplan. Only valid for SubPlan Id 0.
   */
  private List<Pair<Integer, String>> _fields;

  public SubPlanMetadata(Set<String> tableNames, List<Pair<Integer, String>> fields) {
    _tableNames = tableNames;
    _fields = fields;
  }

  public List<Pair<Integer, String>> getFields() {
    return _fields;
  }

  public Set<String> getTableNames() {
    return _tableNames;
  }
}
