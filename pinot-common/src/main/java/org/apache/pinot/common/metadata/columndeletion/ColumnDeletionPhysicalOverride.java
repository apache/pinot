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
package org.apache.pinot.common.metadata.columndeletion;

import java.util.Collection;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.apache.pinot.spi.data.SchemaDiff;


/// Caller-supplied physical column observations for one segment.
///
/// 3A never opens segment files. A later stage that can cheaply see physical names
/// (column metadata, not "missing therefore gone") passes them here. Presence of a
/// ledger-named column, or of an OPEN_STRUCT child of that column, forces dirty and
/// overrides {@code ctime}. Missing names prove nothing.
///
/// Instances are immutable and thread-safe.
public final class ColumnDeletionPhysicalOverride {
  private final Set<String> _presentPhysicalColumns;
  private final Set<String> _absentPhysicalColumns;

  private ColumnDeletionPhysicalOverride(Set<String> presentPhysicalColumns, Set<String> absentPhysicalColumns) {
    _presentPhysicalColumns = presentPhysicalColumns;
    _absentPhysicalColumns = absentPhysicalColumns;
  }

  /// Presence-only override. Any listed name, or an OPEN_STRUCT child of the deleted column, is dirty.
  public static ColumnDeletionPhysicalOverride ofPresent(Collection<String> presentPhysicalColumns) {
    return new ColumnDeletionPhysicalOverride(copyNames(presentPhysicalColumns), Set.of());
  }

  /// Positive absence proof only. Do not pass "every name we happened to see" and infer the rest.
  public static ColumnDeletionPhysicalOverride ofAbsent(Collection<String> absentPhysicalColumns) {
    return new ColumnDeletionPhysicalOverride(Set.of(), copyNames(absentPhysicalColumns));
  }

  public static ColumnDeletionPhysicalOverride of(Collection<String> presentPhysicalColumns,
      Collection<String> absentPhysicalColumns) {
    return new ColumnDeletionPhysicalOverride(copyNames(presentPhysicalColumns), copyNames(absentPhysicalColumns));
  }

  public Set<String> getPresentPhysicalColumns() {
    return _presentPhysicalColumns;
  }

  public Set<String> getAbsentPhysicalColumns() {
    return _absentPhysicalColumns;
  }

  /// True when the deleted logical column, or one of its materialized OPEN_STRUCT children, is present.
  public boolean indicatesPresent(String logicalColumnName, boolean ignoreCase) {
    for (String present : _presentPhysicalColumns) {
      if (matchesLogicalColumn(logicalColumnName, present, ignoreCase)) {
        return true;
      }
    }
    return false;
  }

  /// True only when the caller positively listed the logical column as absent and listed no child of it
  /// as present.
  public boolean provesLogicalColumnAbsent(String logicalColumnName, boolean ignoreCase) {
    if (indicatesPresent(logicalColumnName, ignoreCase)) {
      return false;
    }
    for (String absent : _absentPhysicalColumns) {
      if (SchemaDiff.columnNamesEqual(logicalColumnName, absent, ignoreCase)) {
        return true;
      }
    }
    return false;
  }

  static boolean matchesLogicalColumn(String logicalColumnName, String physicalColumnName, boolean ignoreCase) {
    if (SchemaDiff.columnNamesEqual(logicalColumnName, physicalColumnName, ignoreCase)) {
      return true;
    }
    if (!OpenStructNaming.isMaterializedOpenStructColumn(physicalColumnName)) {
      return false;
    }
    return SchemaDiff.columnNamesEqual(logicalColumnName, OpenStructNaming.parseParentColumn(physicalColumnName),
        ignoreCase);
  }

  private static Set<String> copyNames(@Nullable Collection<String> names) {
    if (names == null || names.isEmpty()) {
      return Set.of();
    }
    return Set.copyOf(names);
  }
}
