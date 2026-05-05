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

package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.Intern;
import org.apache.pinot.spi.data.FieldSpec;


public class DictionaryIndexConfig extends IndexConfig {
  public static final DictionaryIndexConfig DEFAULT = new DictionaryIndexConfig(false);
  public static final DictionaryIndexConfig DISABLED = new DictionaryIndexConfig(true);

  private final boolean _onHeap;
  private final boolean _useVarLengthDictionary;
  private final Intern _intern;

  public DictionaryIndexConfig(Boolean disabled) {
    this(disabled, false, false, null);
  }

  public DictionaryIndexConfig(Boolean onHeap, @Nullable Boolean useVarLengthDictionary) {
    this(onHeap, useVarLengthDictionary, null);
  }

  public DictionaryIndexConfig(Boolean onHeap, @Nullable Boolean useVarLengthDictionary, Intern intern) {
    this(false, onHeap, useVarLengthDictionary, intern);
  }

  /**
   * Constructor for patching an existing config, but overrides the useVarLengthDictionary property with the input
   */
  public DictionaryIndexConfig(DictionaryIndexConfig base, boolean useVarLengthDictionary) {
    this(base.isEnabled(), base.isOnHeap(), useVarLengthDictionary, base.getIntern());
  }

  @JsonCreator
  public DictionaryIndexConfig(@JsonProperty("disabled") Boolean disabled, @JsonProperty("onHeap") Boolean onHeap,
      @JsonProperty("useVarLengthDictionary") @Nullable Boolean useVarLengthDictionary,
      @JsonProperty("intern") @Nullable Intern intern) {
    super(disabled);

    if (intern != null) {
      // Intern configs only work with onHeapDictionary. This precondition can be removed when/if we support interning
      // for off-heap dictionary.
      Preconditions.checkState(intern.isDisabled() || Boolean.TRUE.equals(onHeap),
          "Intern configs only work with on-heap dictionary");
    }

    _onHeap = onHeap != null && onHeap;
    _useVarLengthDictionary = Boolean.TRUE.equals(useVarLengthDictionary);
    _intern = intern;
  }

  public boolean isOnHeap() {
    return _onHeap;
  }

  public boolean isUseVarLengthDictionary() {
    return _useVarLengthDictionary;
  }

  public Intern getIntern() {
    return _intern;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DictionaryIndexConfig that = (DictionaryIndexConfig) o;
    return _onHeap == that._onHeap && _useVarLengthDictionary == that._useVarLengthDictionary && Objects.equals(_intern,
        that._intern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_onHeap, _useVarLengthDictionary, _intern);
  }

  @Override
  public String toString() {
    if (isEnabled()) {
      String internStr = _intern == null ? "null" : _intern.toString();
      return "DictionaryIndexConfig{" + "\"onHeap\":" + _onHeap + ", \"useVarLengthDictionary\":"
          + _useVarLengthDictionary + ", \"intern\":" + internStr + "}";
    } else {
      return "DictionaryIndexConfig{" + "\"disabled\": true}";
    }
  }

  /// Returns `true` if a dictionary must be created for the given column based on the configured indexes.
  /// Iterates over the index types registered in the global [IndexService] singleton; tests that need to exercise a
  /// custom set of index types should call [#requiresDictionary(FieldSpec, FieldIndexConfigs, Iterable)] directly.
  ///
  /// Used at segment creation and segment reload time to decide whether a shared standalone dictionary must be
  /// materialized for a column whose forward index is RAW-encoded but which carries an enabled secondary index that
  /// needs dictionary IDs (for example `inverted`, `fst`, `ifst`).
  public static boolean requiresDictionary(FieldSpec fieldSpec, FieldIndexConfigs fieldIndexConfigs) {
    return requiresDictionary(fieldSpec, fieldIndexConfigs, IndexService.getInstance().getAllIndexes());
  }

  /// Test/plugin-friendly variant: returns `true` if any of the supplied `indexTypes` requires a dictionary for the
  /// column. Decoupled from [IndexService#getInstance()] so callers can pass a controlled set of index types.
  public static boolean requiresDictionary(FieldSpec fieldSpec, FieldIndexConfigs fieldIndexConfigs,
      Iterable<IndexType<?, ?, ?>> indexTypes) {
    for (IndexType<?, ?, ?> indexType : indexTypes) {
      if (StandardIndexes.DICTIONARY_ID.equals(indexType.getId())) {
        continue;
      }
      if (requiresDictionaryBy(indexType, fieldSpec, fieldIndexConfigs)) {
        return true;
      }
    }
    return false;
  }

  private static <C extends IndexConfig> boolean requiresDictionaryBy(IndexType<C, ?, ?> indexType,
      FieldSpec fieldSpec, FieldIndexConfigs fieldIndexConfigs) {
    C config = fieldIndexConfigs.getConfig(indexType);
    if (config == null || config.isDisabled()) {
      return false;
    }
    return indexType.requiresDictionary(fieldSpec, config);
  }

  /// Returns the list of index types that require a dictionary for the given column based on the configured
  /// indexes. See [#requiresDictionary(FieldSpec, FieldIndexConfigs)] for the singleton-vs-injected discussion.
  public static List<IndexType<?, ?, ?>> getIndexTypesWithDictionaryRequired(FieldSpec fieldSpec,
      FieldIndexConfigs fieldIndexConfigs) {
    return getIndexTypesWithDictionaryRequired(fieldSpec, fieldIndexConfigs,
        IndexService.getInstance().getAllIndexes());
  }

  /// Test/plugin-friendly variant: returns the list of index types from the supplied iterable that require a
  /// dictionary for the column.
  public static List<IndexType<?, ?, ?>> getIndexTypesWithDictionaryRequired(FieldSpec fieldSpec,
      FieldIndexConfigs fieldIndexConfigs, Iterable<IndexType<?, ?, ?>> indexTypes) {
    List<IndexType<?, ?, ?>> result = new ArrayList<>();
    for (IndexType<?, ?, ?> indexType : indexTypes) {
      if (StandardIndexes.DICTIONARY_ID.equals(indexType.getId())) {
        continue;
      }
      if (requiresDictionaryBy(indexType, fieldSpec, fieldIndexConfigs)) {
        result.add(indexType);
      }
    }
    return result;
  }

  /// Returns the set of index types whose existing on-disk index must be invalidated (deleted and rebuilt) when the
  /// dictionary for the column is added or removed across a segment reload. Each candidate index type is queried
  /// via [IndexType#shouldInvalidateOnDictionaryChange(FieldSpec, IndexConfig)].
  ///
  /// The returned set is the per-column work-list for the dictionary-change rebuild pass: each contained index
  /// type's existing payload is no longer trustworthy under the new dictionary state and must be regenerated.
  public static Set<IndexType<?, ?, ?>> getIndexTypesToInvalidateOnDictionaryChange(FieldSpec fieldSpec,
      FieldIndexConfigs fieldIndexConfigs) {
    return getIndexTypesToInvalidateOnDictionaryChange(fieldSpec, fieldIndexConfigs,
        IndexService.getInstance().getAllIndexes());
  }

  /// Test/plugin-friendly variant of
  /// [#getIndexTypesToInvalidateOnDictionaryChange(FieldSpec, FieldIndexConfigs)].
  public static Set<IndexType<?, ?, ?>> getIndexTypesToInvalidateOnDictionaryChange(FieldSpec fieldSpec,
      FieldIndexConfigs fieldIndexConfigs, Iterable<IndexType<?, ?, ?>> indexTypes) {
    Set<IndexType<?, ?, ?>> result = new HashSet<>();
    for (IndexType<?, ?, ?> indexType : indexTypes) {
      if (shouldInvalidateOnDictionaryChange(indexType, fieldSpec, fieldIndexConfigs)) {
        result.add(indexType);
      }
    }
    return result;
  }

  private static <C extends IndexConfig> boolean shouldInvalidateOnDictionaryChange(IndexType<C, ?, ?> indexType,
      FieldSpec fieldSpec, FieldIndexConfigs fieldIndexConfigs) {
    C config = fieldIndexConfigs.getConfig(indexType);
    if (config == null || config.isDisabled()) {
      return false;
    }
    return indexType.shouldInvalidateOnDictionaryChange(fieldSpec, config);
  }
}
