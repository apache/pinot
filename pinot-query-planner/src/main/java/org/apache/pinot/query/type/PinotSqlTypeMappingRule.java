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

package org.apache.pinot.query.type;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.type.SqlTypeMappingRule;
import org.apache.calcite.sql.type.SqlTypeName;


/**
 * The {@code PinotSqlTypeMappingRule} overwrites Calcite's SqlTypeMappingRule with Pinot specific logics.
 */
public class PinotSqlTypeMappingRule implements SqlTypeMappingRule {

  private static final Set<SqlTypeName> TYPES = ImmutableSet.of(
      SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.INTEGER, SqlTypeName.BIGINT);

  private final Map<SqlTypeName, ImmutableSet<SqlTypeName>> _map;
  private static PinotSqlTypeMappingRule _instance;

  private PinotSqlTypeMappingRule(SqlTypeMappingRule rule) {
    // support int or long datatype cast to Date or Time in pinot
    Map<SqlTypeName, ImmutableSet<SqlTypeName>> map = new HashMap<>(rule.getTypeMapping());
    if (map.containsKey(SqlTypeName.DATE)) {
      Set<SqlTypeName> fromTypes = new HashSet<>(map.get(SqlTypeName.DATE));
      fromTypes.addAll(TYPES);
      map.put(SqlTypeName.DATE, ImmutableSet.copyOf(fromTypes));
    }
    if (map.containsKey(SqlTypeName.TIME)) {
      Set<SqlTypeName> fromTypes = new HashSet<>(map.get(SqlTypeName.TIME));
      fromTypes.addAll(TYPES);
      map.put(SqlTypeName.TIME, ImmutableSet.copyOf(fromTypes));
    }

    _map = ImmutableMap.copyOf(map);
  }

  @Override
  public Map<SqlTypeName, ImmutableSet<SqlTypeName>> getTypeMapping() {
    return _map;
  }

  public static PinotSqlTypeMappingRule getInstance(SqlTypeMappingRule rule) {
    if (_instance == null) {
      synchronized (PinotSqlTypeMappingRule.class) {
        if (_instance == null) {
          _instance = new PinotSqlTypeMappingRule(rule);
        }
      }
    }
    return _instance;
  }
}
