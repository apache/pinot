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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Configuration for the ColumnarMap index on a MAP column.
 * Controls which keys are indexed per-key in columnar storage and whether
 * per-key inverted indexes are enabled for fast value-based filtering.
 *
 * <p>Inverted index control:
 * <ul>
 *   <li>{@code enableInvertedIndexForAll: true} — inverted index on ALL keys
 *       ({@code invertedIndexKeys} is ignored)</li>
 *   <li>{@code enableInvertedIndexForAll: false} + {@code invertedIndexKeys: [...]} —
 *       only the listed keys get inverted indexes</li>
 *   <li>{@code enableInvertedIndexForAll: false} + no {@code invertedIndexKeys} —
 *       no inverted indexes</li>
 * </ul>
 */
public class ColumnarMapIndexConfig extends IndexConfig {
  public static final ColumnarMapIndexConfig DISABLED = new ColumnarMapIndexConfig(false);
  public static final ColumnarMapIndexConfig DEFAULT = new ColumnarMapIndexConfig(true);

  private final Set<String> _indexedKeys;
  private final boolean _enableInvertedIndexForAll;
  private final Set<String> _invertedIndexKeys;
  private final Set<String> _noDictionaryKeys;
  private final int _maxKeys;

  /**
   * Creates a ColumnarMapIndexConfig from FieldConfig properties map.
   * Reads the COLUMNAR_MAP_INDEX_* property constants from {@link FieldConfig}.
   */
  public static ColumnarMapIndexConfig fromProperties(@Nullable Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return DEFAULT;
    }
    int maxKeys = Integer.parseInt(
        properties.getOrDefault(FieldConfig.COLUMNAR_MAP_INDEX_MAX_KEYS, "1000"));
    Set<String> invertedIndexKeys = parseCommaSeparated(
        properties.get(FieldConfig.COLUMNAR_MAP_INDEX_INVERTED_INDEX_KEYS));
    Set<String> noDictionaryKeys = parseCommaSeparated(
        properties.get(FieldConfig.COLUMNAR_MAP_INDEX_NO_DICTIONARY_KEYS));
    boolean enableInvertedForAll = Boolean.parseBoolean(
        properties.getOrDefault(FieldConfig.COLUMNAR_MAP_INDEX_ENABLE_INVERTED_FOR_ALL, "false"));
    return new ColumnarMapIndexConfig(true, null, enableInvertedForAll, invertedIndexKeys, noDictionaryKeys, maxKeys);
  }

  @Nullable
  private static Set<String> parseCommaSeparated(@Nullable String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    Set<String> result = new HashSet<>();
    for (String part : value.split(FieldConfig.COLUMNAR_MAP_INDEX_KEY_SEPARATOR)) {
      String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        result.add(trimmed);
      }
    }
    return result.isEmpty() ? null : result;
  }

  public ColumnarMapIndexConfig(boolean enabled) {
    this(enabled, null, false, null, null, 1000);
  }

  public ColumnarMapIndexConfig(boolean enabled, @Nullable Set<String> indexedKeys,
      boolean enableInvertedIndexForAll, @Nullable Set<String> invertedIndexKeys, int maxKeys) {
    this(enabled, indexedKeys, enableInvertedIndexForAll, invertedIndexKeys, null, maxKeys);
  }

  @JsonCreator
  public ColumnarMapIndexConfig(
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("indexedKeys") @Nullable Set<String> indexedKeys,
      @JsonProperty("enableInvertedIndexForAll") boolean enableInvertedIndexForAll,
      @JsonProperty("invertedIndexKeys") @Nullable Set<String> invertedIndexKeys,
      @JsonProperty("noDictionaryKeys") @Nullable Set<String> noDictionaryKeys,
      @JsonProperty("maxKeys") int maxKeys) {
    super(!enabled);
    _indexedKeys = indexedKeys;
    _enableInvertedIndexForAll = enableInvertedIndexForAll;
    _invertedIndexKeys = invertedIndexKeys;
    _noDictionaryKeys = noDictionaryKeys;
    _maxKeys = maxKeys > 0 ? maxKeys : 1000;
  }

  /**
   * Returns the set of keys to index, or null if all keys should be indexed.
   */
  @Nullable
  public Set<String> getIndexedKeys() {
    return _indexedKeys;
  }

  /**
   * Returns true if inverted indexes should be created for ALL keys.
   */
  public boolean isEnableInvertedIndexForAll() {
    return _enableInvertedIndexForAll;
  }

  /**
   * Returns the set of keys that should have inverted indexes, or null if not specified.
   * Only consulted when {@link #isEnableInvertedIndexForAll()} is false.
   */
  @Nullable
  public Set<String> getInvertedIndexKeys() {
    return _invertedIndexKeys;
  }

  /**
   * Returns true if an inverted index should be created for the given key.
   * Returns true if {@code enableInvertedIndexForAll} is set, or if the key
   * is in the {@code invertedIndexKeys} set.
   */
  public boolean shouldEnableInvertedIndexForKey(String key) {
    return _enableInvertedIndexForAll
        || (_invertedIndexKeys != null && _invertedIndexKeys.contains(key));
  }

  /**
   * Returns the set of keys that should always use raw encoding (no dictionary),
   * or null if not specified (all keys eligible for dictionary encoding).
   */
  @Nullable
  public Set<String> getNoDictionaryKeys() {
    return _noDictionaryKeys;
  }

  /**
   * Returns true if dictionary encoding is allowed for the given key.
   * Returns false if the key is in the {@code noDictionaryKeys} set.
   */
  public boolean shouldUseDictionaryForKey(String key) {
    return _noDictionaryKeys == null || !_noDictionaryKeys.contains(key);
  }

  /**
   * Returns the maximum number of distinct keys allowed. Keys beyond this cap fall back to
   * the forward index blob. Default is 1000.
   */
  public int getMaxKeys() {
    return _maxKeys;
  }
}
