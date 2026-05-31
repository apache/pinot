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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/// Configuration for the OPEN_STRUCT index on an OPEN_STRUCT column.
///
/// **Dense vs sparse:** a key is materialized as its own column if (a) it appears in the explicit
/// `denseKeys` set, or (b) its fill rate (fraction of documents containing the key) is ≥
/// `denseKeyMinFillRate`. Keys not satisfying either criterion go into a sparse OPEN_STRUCT column.
///
/// **maxDenseKeys cutoff:** when more keys qualify as dense than `maxDenseKeys` allows, the top
/// `maxDenseKeys` keys ranked by fill rate are materialized; the rest fall back to the sparse
/// column. `-1` (default) means unlimited — every qualifying key is materialized. `0` disables
/// dense keys entirely (all keys go to the sparse column). Use `denseKeys` to pin specific keys
/// regardless of fill rate ranking.
///
/// **Per-key index settings** are specified via `valueFieldConfigs` — each entry is a standard
/// [FieldConfig] (modern `indexes` format) for one materialized OPEN_STRUCT key. Keys without an
/// entry fall back to `defaultValueFieldConfig`. When neither is set, the built-in default is
/// DICTIONARY encoding with an inverted index.
public class OpenStructIndexConfig extends IndexConfig {
  public static final OpenStructIndexConfig DISABLED = new OpenStructIndexConfig(false);
  public static final OpenStructIndexConfig DEFAULT = new OpenStructIndexConfig(true);

  public static final double DEFAULT_DENSE_KEY_MIN_FILL_RATE = 0.5;
  /// Default `maxDenseKeys`. `-1` means unlimited.
  public static final int DEFAULT_MAX_DENSE_KEYS = -1;
  private static final String INVERTED_INDEX_KEY = "inverted";

  private final FieldConfig _defaultValueFieldConfig;
  private final int _maxDenseKeys;
  private final Set<String> _denseKeys;
  private final double _denseKeyMinFillRate;
  private final List<FieldConfig> _valueFieldConfigs;
  // Eager lookup from key name → FieldConfig for O(1) per-key access. Built in constructor
  // so the config is fully immutable and safe to share across threads.
  private final Map<String, FieldConfig> _valueFieldConfigIndex;

  public OpenStructIndexConfig(boolean enabled) {
    this(!enabled, null, DEFAULT_MAX_DENSE_KEYS, null, DEFAULT_DENSE_KEY_MIN_FILL_RATE, null);
  }

  @JsonCreator
  public OpenStructIndexConfig(
      @JsonProperty("disabled") Boolean disabled,
      @JsonProperty("defaultValueFieldConfig") @Nullable FieldConfig defaultValueFieldConfig,
      @JsonProperty("maxDenseKeys") int maxDenseKeys,
      @JsonProperty("denseKeys") @Nullable Set<String> denseKeys,
      @JsonProperty("denseKeyMinFillRate") @Nullable Double denseKeyMinFillRate,
      @JsonProperty("valueFieldConfigs") @Nullable List<FieldConfig> valueFieldConfigs) {
    super(disabled);
    _defaultValueFieldConfig = defaultValueFieldConfig;
    _maxDenseKeys = maxDenseKeys;
    _denseKeys = denseKeys;
    _denseKeyMinFillRate = denseKeyMinFillRate != null ? denseKeyMinFillRate : DEFAULT_DENSE_KEY_MIN_FILL_RATE;
    _valueFieldConfigs = valueFieldConfigs;
    if (valueFieldConfigs == null || valueFieldConfigs.isEmpty()) {
      _valueFieldConfigIndex = Map.of();
    } else {
      Map<String, FieldConfig> index = new HashMap<>(valueFieldConfigs.size());
      for (FieldConfig fc : valueFieldConfigs) {
        index.put(fc.getName(), fc);
      }
      _valueFieldConfigIndex = index;
    }
  }

  /// Fallback [FieldConfig] applied to OPEN_STRUCT keys that are materialized as dense columns
  /// but have no entry in `valueFieldConfigs`. When this is also unset, the built-in defaults
  /// kick in: DICTIONARY encoding and an inverted index.
  @Nullable
  public FieldConfig getDefaultValueFieldConfig() {
    return _defaultValueFieldConfig;
  }

  /// Maximum number of OPEN_STRUCT keys to materialise as dense columns. `-1` (default) means
  /// unlimited — every key qualifying as dense is materialized. `0` disables dense keys entirely.
  /// When positive and more keys qualify, the top `maxDenseKeys` by fill rate are materialized;
  /// the rest fall back to the sparse OPEN_STRUCT column.
  public int getMaxDenseKeys() {
    return _maxDenseKeys;
  }

  public Set<String> getDenseKeys() {
    return _denseKeys != null ? _denseKeys : Set.of();
  }

  public double getDenseKeyMinFillRate() {
    return _denseKeyMinFillRate;
  }

  public boolean isDenseKey(String key) {
    return _denseKeys != null && _denseKeys.contains(key);
  }

  /// Per-key index settings. Each entry is a standard [FieldConfig] whose `name` matches an
  /// OPEN_STRUCT key name. Keys without an entry fall back to `defaultValueFieldConfig`, or to
  /// the built-in defaults (DICTIONARY + inverted) when no default is set.
  @Nullable
  public List<FieldConfig> getValueFieldConfigs() {
    return _valueFieldConfigs;
  }

  /// Returns the [FieldConfig] for the given key, or null if none was configured.
  @Nullable
  public FieldConfig getValueFieldConfig(String key) {
    return _valueFieldConfigIndex.get(key);
  }

  /// `true` if the given key should be built with an inverted index. Resolution order:
  /// per-key [FieldConfig] → `defaultValueFieldConfig` → built-in default of inverted-on.
  public boolean shouldEnableInvertedIndexForKey(String key) {
    FieldConfig keyConfig = getValueFieldConfig(key);
    if (keyConfig != null) {
      return invertedFromIndexes(keyConfig, key);
    }
    if (_defaultValueFieldConfig != null) {
      return invertedFromIndexes(_defaultValueFieldConfig, key);
    }
    return true;
  }

  /// `true` if the given key should be dictionary-encoded. Resolution order: per-key
  /// [FieldConfig] → `defaultValueFieldConfig` → built-in default of DICTIONARY.
  public boolean shouldUseDictionaryForKey(String key) {
    FieldConfig keyConfig = getValueFieldConfig(key);
    if (keyConfig != null) {
      return keyConfig.getEncodingType() != FieldConfig.EncodingType.RAW;
    }
    if (_defaultValueFieldConfig != null) {
      return _defaultValueFieldConfig.getEncodingType() != FieldConfig.EncodingType.RAW;
    }
    return true;
  }

  private static boolean invertedFromIndexes(FieldConfig fieldConfig, String key) {
    JsonNode indexes = fieldConfig.getIndexes();
    if (indexes == null || !indexes.isObject()) {
      return false;
    }
    JsonNode inverted = indexes.get(INVERTED_INDEX_KEY);
    if (inverted == null) {
      return false;
    }
    try {
      return JsonUtils.jsonNodeToObject(inverted, IndexConfig.class).isEnabled();
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to parse inverted index config for OPEN_STRUCT key '" + key + "'", e);
    }
  }
}
