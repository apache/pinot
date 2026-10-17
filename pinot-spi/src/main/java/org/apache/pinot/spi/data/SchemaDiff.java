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
package org.apache.pinot.spi.data;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;


/// Structured difference between two {@link Schema}s.
///
/// Computes the sets used by first-class column deletion (apache/pinot#18808):
/// {@code deleted = old - new}, {@code added = new - old}, and retained columns whose
/// {@link FieldSpec} is not backward compatible or whose field kind changed. A rename is
/// reported as one delete plus one add. This class does not decide whether a delete is
/// allowed and does not write any metadata.
///
/// Instances are immutable and therefore thread-safe.
public final class SchemaDiff {
  private final List<String> _deletedColumnNames;
  private final List<String> _addedColumnNames;
  private final List<RetainedColumnChange> _retainedIncompatibleColumns;
  private final List<String> _oldPrimaryKeyColumns;
  private final List<String> _newPrimaryKeyColumns;
  private final boolean _primaryKeyColumnsChanged;
  private final boolean _existingPrimaryKeyColumnsChanged;
  private final boolean _ignoreCase;

  private SchemaDiff(List<String> deletedColumnNames, List<String> addedColumnNames,
      List<RetainedColumnChange> retainedIncompatibleColumns, List<String> oldPrimaryKeyColumns,
      List<String> newPrimaryKeyColumns, boolean primaryKeyColumnsChanged, boolean existingPrimaryKeyColumnsChanged,
      boolean ignoreCase) {
    _deletedColumnNames = deletedColumnNames;
    _addedColumnNames = addedColumnNames;
    _retainedIncompatibleColumns = retainedIncompatibleColumns;
    _oldPrimaryKeyColumns = oldPrimaryKeyColumns;
    _newPrimaryKeyColumns = newPrimaryKeyColumns;
    _primaryKeyColumnsChanged = primaryKeyColumnsChanged;
    _existingPrimaryKeyColumnsChanged = existingPrimaryKeyColumnsChanged;
    _ignoreCase = ignoreCase;
  }

  /// Case-sensitive diff. Column names must match exactly to be treated as the same column.
  public static SchemaDiff compute(Schema oldSchema, Schema newSchema) {
    return compute(oldSchema, newSchema, false);
  }

  /// Diff two schemas.
  ///
  /// When {@code ignoreCase} is true, names are matched with {@link Locale#ROOT} lowercasing, the
  /// same rule {@code SchemaUtils.validate} uses for case-insensitive tables. A schema that already
  /// contains a case collision is rejected rather than silently merging those columns.
  ///
  /// @param oldSchema previously stored schema
  /// @param newSchema proposed schema
  /// @param ignoreCase whether to treat names that differ only by case as the same column
  public static SchemaDiff compute(Schema oldSchema, Schema newSchema, boolean ignoreCase) {
    Preconditions.checkNotNull(oldSchema, "oldSchema");
    Preconditions.checkNotNull(newSchema, "newSchema");

    Map<String, String> oldIndex = indexColumnNames(oldSchema, ignoreCase);
    Map<String, String> newIndex = indexColumnNames(newSchema, ignoreCase);

    List<String> deletedColumnNames = new ArrayList<>();
    List<String> addedColumnNames = new ArrayList<>();
    List<RetainedColumnChange> retainedIncompatibleColumns = new ArrayList<>();

    for (Map.Entry<String, String> oldEntry : oldIndex.entrySet()) {
      String newColumnName = newIndex.get(oldEntry.getKey());
      if (newColumnName == null) {
        deletedColumnNames.add(oldEntry.getValue());
        continue;
      }
      FieldSpec oldFieldSpec = oldSchema.getFieldSpecFor(oldEntry.getValue());
      FieldSpec newFieldSpec = newSchema.getFieldSpecFor(newColumnName);
      if (isRetainedIncompatible(oldFieldSpec, newFieldSpec)) {
        retainedIncompatibleColumns.add(
            new RetainedColumnChange(oldEntry.getValue(), newColumnName, oldFieldSpec, newFieldSpec));
      }
    }
    for (Map.Entry<String, String> newEntry : newIndex.entrySet()) {
      if (!oldIndex.containsKey(newEntry.getKey())) {
        addedColumnNames.add(newEntry.getValue());
      }
    }

    List<String> oldPrimaryKeyColumns = copyPrimaryKeyColumns(oldSchema);
    List<String> newPrimaryKeyColumns = copyPrimaryKeyColumns(newSchema);
    // Primary-key equality stays exact-name, matching Schema.isBackwardCompatibleWith.
    boolean primaryKeyColumnsChanged = !oldPrimaryKeyColumns.equals(newPrimaryKeyColumns);
    boolean existingPrimaryKeyColumnsChanged = !oldPrimaryKeyColumns.isEmpty() && primaryKeyColumnsChanged;

    return new SchemaDiff(List.copyOf(deletedColumnNames), List.copyOf(addedColumnNames),
        List.copyOf(retainedIncompatibleColumns), oldPrimaryKeyColumns, newPrimaryKeyColumns, primaryKeyColumnsChanged,
        existingPrimaryKeyColumnsChanged, ignoreCase);
  }

  /// Names present in {@code oldSchema} and absent from {@code newSchema}, in schema iteration order.
  public List<String> getDeletedColumnNames() {
    return _deletedColumnNames;
  }

  /// Names present in {@code newSchema} and absent from {@code oldSchema}, in schema iteration order.
  public List<String> getAddedColumnNames() {
    return _addedColumnNames;
  }

  /// Same-name columns that fail {@link FieldSpec#isBackwardCompatibleWith(FieldSpec)} or changed field kind.
  ///
  /// Default-null and max-length edits are compatible today and are not listed.
  public List<RetainedColumnChange> getRetainedIncompatibleColumns() {
    return _retainedIncompatibleColumns;
  }

  public List<String> getOldPrimaryKeyColumns() {
    return _oldPrimaryKeyColumns;
  }

  public List<String> getNewPrimaryKeyColumns() {
    return _newPrimaryKeyColumns;
  }

  /// True when the primary-key lists differ, including adding keys to a schema that had none.
  public boolean isPrimaryKeyColumnsChanged() {
    return _primaryKeyColumnsChanged;
  }

  /// True when the old schema already had primary keys and the new list is not equal.
  ///
  /// This is the existing {@link Schema#isBackwardCompatibleWith(Schema)} primary-key rule.
  public boolean isExistingPrimaryKeyColumnsChanged() {
    return _existingPrimaryKeyColumnsChanged;
  }

  public boolean isIgnoreCase() {
    return _ignoreCase;
  }

  /// True when there are no deletes, no adds, no retained incompatibilities, and no primary-key change.
  ///
  /// Default-null, max-length, transform, and description edits are not structural and do not
  /// affect this result. Do not skip a schema write based on this method alone.
  public boolean isStructurallyUnchanged() {
    return _deletedColumnNames.isEmpty() && _addedColumnNames.isEmpty() && _retainedIncompatibleColumns.isEmpty()
        && !_primaryKeyColumnsChanged;
  }

  /// True when column deletion would be the only extra permission needed versus today's update rules,
  /// plus field-kind changes which {@link Schema#isBackwardCompatibleWith(Schema)} does not check.
  ///
  /// Type, field-kind, and existing primary-key changes still fail this check. An empty deleted set
  /// is a normal compatible update from the deletion-flag point of view.
  public boolean isCompatibleWhenColumnDeletionAllowed() {
    return _retainedIncompatibleColumns.isEmpty() && !_existingPrimaryKeyColumnsChanged;
  }

  public static String normalizeColumnName(String columnName, boolean ignoreCase) {
    Preconditions.checkNotNull(columnName, "columnName");
    return ignoreCase ? columnName.toLowerCase(Locale.ROOT) : columnName;
  }

  public static boolean columnNamesEqual(String left, String right, boolean ignoreCase) {
    if (left == null || right == null) {
      return false;
    }
    return normalizeColumnName(left, ignoreCase).equals(normalizeColumnName(right, ignoreCase));
  }

  private static boolean isRetainedIncompatible(FieldSpec oldFieldSpec, FieldSpec newFieldSpec) {
    return !newFieldSpec.isBackwardCompatibleWith(oldFieldSpec)
        || oldFieldSpec.getFieldType() != newFieldSpec.getFieldType();
  }

  private static Map<String, String> indexColumnNames(Schema schema, boolean ignoreCase) {
    Map<String, String> normalizedToOriginal = new LinkedHashMap<>();
    for (String columnName : schema.getColumnNames()) {
      String normalized = normalizeColumnName(columnName, ignoreCase);
      String previous = normalizedToOriginal.put(normalized, columnName);
      if (previous != null) {
        throw new IllegalArgumentException(
            "Schema '" + schema.getSchemaName() + "' has case-colliding columns '" + previous + "' and '" + columnName
                + "'");
      }
    }
    return normalizedToOriginal;
  }

  private static List<String> copyPrimaryKeyColumns(Schema schema) {
    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
      return List.of();
    }
    return List.copyOf(primaryKeyColumns);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaDiff that = (SchemaDiff) o;
    return _primaryKeyColumnsChanged == that._primaryKeyColumnsChanged
        && _existingPrimaryKeyColumnsChanged == that._existingPrimaryKeyColumnsChanged
        && _ignoreCase == that._ignoreCase && _deletedColumnNames.equals(that._deletedColumnNames)
        && _addedColumnNames.equals(that._addedColumnNames)
        && _retainedIncompatibleColumns.equals(that._retainedIncompatibleColumns)
        && _oldPrimaryKeyColumns.equals(that._oldPrimaryKeyColumns)
        && _newPrimaryKeyColumns.equals(that._newPrimaryKeyColumns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_deletedColumnNames, _addedColumnNames, _retainedIncompatibleColumns, _oldPrimaryKeyColumns,
        _newPrimaryKeyColumns, _primaryKeyColumnsChanged, _existingPrimaryKeyColumnsChanged, _ignoreCase);
  }

  @Override
  public String toString() {
    return "SchemaDiff{deleted=" + _deletedColumnNames + ", added=" + _addedColumnNames + ", retainedIncompatible="
        + _retainedIncompatibleColumns + ", oldPrimaryKeys=" + _oldPrimaryKeyColumns + ", newPrimaryKeys="
        + _newPrimaryKeyColumns + ", ignoreCase=" + _ignoreCase + '}';
  }

  /// A retained column whose {@link FieldSpec} is not backward compatible, or whose field kind changed.
  public static final class RetainedColumnChange {
    private final String _oldColumnName;
    private final String _newColumnName;
    private final FieldSpec _oldFieldSpec;
    private final FieldSpec _newFieldSpec;

    public RetainedColumnChange(String oldColumnName, String newColumnName, FieldSpec oldFieldSpec,
        FieldSpec newFieldSpec) {
      _oldColumnName = oldColumnName;
      _newColumnName = newColumnName;
      _oldFieldSpec = oldFieldSpec;
      _newFieldSpec = newFieldSpec;
    }

    public String getOldColumnName() {
      return _oldColumnName;
    }

    public String getNewColumnName() {
      return _newColumnName;
    }

    public FieldSpec getOldFieldSpec() {
      return _oldFieldSpec;
    }

    public FieldSpec getNewFieldSpec() {
      return _newFieldSpec;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RetainedColumnChange that = (RetainedColumnChange) o;
      return _oldColumnName.equals(that._oldColumnName) && _newColumnName.equals(that._newColumnName)
          && _oldFieldSpec.equals(that._oldFieldSpec) && _newFieldSpec.equals(that._newFieldSpec);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_oldColumnName, _newColumnName, _oldFieldSpec, _newFieldSpec);
    }

    @Override
    public String toString() {
      return _oldColumnName + "->" + _newColumnName + " (" + _oldFieldSpec.getDataType() + "/"
          + _oldFieldSpec.getFieldType() + " -> " + _newFieldSpec.getDataType() + "/" + _newFieldSpec.getFieldType()
          + ")";
    }
  }
}
