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
package org.apache.pinot.common.request.context;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

/// Engine-agnostic conventions for GROUP BY GROUPING SETS / ROLLUP / CUBE, shared so both query engines
/// agree on the discriminator column and the GROUPING()/GROUPING_ID() semantics.
///
/// A grouping-set query is represented as the union of all grouping columns plus, per set, a "grouping-id"
/// bitmask over that union where bit `i` is set iff union column `i` is **rolled up** (aggregated away) in
/// that set — matching the SQL/PostgreSQL convention that GROUPING(col) returns 1 for an aggregated-away
/// column. The bitmask is carried through the result as the synthetic key column named {@link
/// #GROUPING_ID_COLUMN}, which the discriminator keeps distinct across sets and which powers
/// GROUPING()/GROUPING_ID().
public class GroupingSets {
  private GroupingSets() {
  }

  /// Name of the synthetic INT key column that carries the per-set grouping-id bitmask. It is internal: it is
  /// appended after the union group-by columns in intermediate results and dropped from the user-facing
  /// result (never projected unless referenced via GROUPING()/GROUPING_ID()).
  public static final String GROUPING_ID_COLUMN = "$groupingId";

  /// The grouping-id discriminator is a 32-bit INT bitmask over the union group-by columns, so a grouping-set query
  /// may reference at most 31 distinct grouping columns (bit 31 is the sign bit).
  public static final int MAX_GROUPING_SET_COLUMNS = 31;

  /// Returns the LEAF row type for a grouping-set aggregate: the synthetic {@link #GROUPING_ID_COLUMN} INT column
  /// inserted right after the {@code groupCount} union group-by columns, i.e. {@code [group keys..., $groupingId,
  /// aggregates...]}. Both query engines share this single layout so the multi-stage final stage can group on
  /// {@code $groupingId}; keeping it here prevents the two planner {@code deriveRowType()} overrides from drifting.
  public static RelDataType appendGroupingIdColumn(RelDataTypeFactory typeFactory, RelDataType rowType,
      int groupCount) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (int i = 0; i < groupCount; i++) {
      builder.add(fields.get(i));
    }
    builder.add(GROUPING_ID_COLUMN, typeFactory.createSqlType(SqlTypeName.INTEGER));
    for (int i = groupCount; i < fields.size(); i++) {
      builder.add(fields.get(i));
    }
    return builder.build();
  }

  /// Computes the {@code GROUPING(args...)} / {@code GROUPING_ID(args...)} value from a row's grouping-id
  /// bitmask. For each argument column (identified by its index in the union of grouping columns) the
  /// corresponding bit is read from {@code groupingId} (1 = rolled up), and the bits are packed with the
  /// first argument as the most significant bit, per the SQL standard.
  ///
  /// @param groupingId the row's grouping-id bitmask (bit i set iff union column i is rolled up)
  /// @param unionColumnIndexes the union-column index of each GROUPING argument, in argument order
  /// @return the GROUPING / GROUPING_ID integer value
  public static int groupingValue(int groupingId, int[] unionColumnIndexes) {
    int result = 0;
    for (int unionColumnIndex : unionColumnIndexes) {
      result = (result << 1) | ((groupingId >>> unionColumnIndex) & 1);
    }
    return result;
  }

  /// Returns true if the given function name denotes {@code GROUPING} or {@code GROUPING_ID}. The argument is
  /// the post-canonicalization form (lower-cased, underscores removed), so {@code GROUPING_ID} appears as
  /// {@code "groupingid"}. Centralizes the name contract shared by the broker post-aggregation handler and the
  /// ORDER BY comparator so the two sites cannot drift.
  public static boolean isGroupingFunction(String functionName) {
    return functionName.equalsIgnoreCase("grouping") || functionName.equalsIgnoreCase("groupingid");
  }
}
