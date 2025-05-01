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
import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Configs related to the JSON index:
 * - maxLevels: Max levels to flatten the json object (array is also counted as one level), non-positive value means
 *              unlimited
 * - excludeArray: Whether to exclude array when flattening the object
 * - disableCrossArrayUnnest: Whether to not unnest multiple arrays (unique combination of all elements)
 * - includePaths: Only include the given paths, e.g. "$.a.b", "$.a.c[*]" (mutual exclusive with excludePaths). Paths
 *                 under the included paths will be included, e.g. "$.a.b.c" will be included when "$.a.b" is configured
 *                 to be included.
 * - excludePaths: Exclude the given paths, e.g. "$.a.b", "$.a.c[*]" (mutual exclusive with includePaths). Paths under
 *                 the excluded paths will also be excluded, e.g. "$.a.b.c" will be excluded when "$.a.b" is configured
 *                 to be excluded.
 * - excludeFields: Exclude the given fields, e.g. "b", "c", even if it is under the included paths.
 * - indexPaths: Index the given paths, e.g. "*.*", "a.**". Paths matches the indexed paths will be indexed.
 *               This config could work together with other configs, e.g. includePaths, excludePaths, maxLevels but
 *               usually does not have to because it should be flexible enough to config any scenarios. By default, it
 *               is working as "**" this is to allow everything.
 *               Check {@link org.apache.pinot.spi.utils.JsonSchemaTreeNode} for more details.
 * - maxValueLength: Exclude field values which are longer than this length. A value of "0" disables this filter.
 *                   Excluded values will be replaced with JsonUtils.SKIPPED_VALUE_REPLACEMENT.
 * - skipInvalidJson: If the raw data is not a valid json string, then replace with {"":SKIPPED_VALUE_REPLACEMENT}
 *                    and continue indexing on following Json records.
 */
public class JsonIndexConfig extends IndexConfig {
  public static final JsonIndexConfig DEFAULT = new JsonIndexConfig();
  public static final JsonIndexConfig DISABLED = new JsonIndexConfig(true);

  private int _maxLevels = -1;
  private boolean _excludeArray = false;
  private boolean _disableCrossArrayUnnest = false;
  private Set<String> _includePaths;
  private Set<String> _excludePaths;
  private Set<String> _excludeFields;
  private Set<String> _indexPaths;
  private int _maxValueLength = 0;
  private boolean _skipInvalidJson = false;
  private long _maxBytesSize = Long.MAX_VALUE;

  public JsonIndexConfig() {
    super(false);
  }

  public JsonIndexConfig(Boolean disabled) {
    super(disabled);
  }

  @JsonCreator
  public JsonIndexConfig(@JsonProperty("disabled") Boolean disabled, @JsonProperty("maxLevels") int maxLevels,
      @JsonProperty("excludeArray") boolean excludeArray,
      @JsonProperty("disableCrossArrayUnnest") boolean disableCrossArrayUnnest,
      @JsonProperty("includePaths") @Nullable Set<String> includePaths,
      @JsonProperty("excludePaths") @Nullable Set<String> excludePaths,
      @JsonProperty("excludeFields") @Nullable Set<String> excludeFields,
      @JsonProperty("indexPaths") @Nullable Set<String> indexPaths,
      @JsonProperty("maxValueLength") int maxValueLength,
      @JsonProperty("skipInvalidJson") boolean skipInvalidJson,
      @JsonProperty("maxBytesSize") @Nullable Long maxBytesSize) {
    super(disabled);
    _maxLevels = maxLevels;
    _excludeArray = excludeArray;
    _disableCrossArrayUnnest = disableCrossArrayUnnest;
    _includePaths = includePaths;
    _excludePaths = excludePaths;
    _excludeFields = excludeFields;
    _indexPaths = indexPaths;
    _maxValueLength = maxValueLength;
    _skipInvalidJson = skipInvalidJson;
    _maxBytesSize = maxBytesSize == null ? _maxBytesSize : maxBytesSize;
  }

  public int getMaxLevels() {
    return _maxLevels;
  }

  public void setMaxLevels(int maxLevels) {
    _maxLevels = maxLevels;
  }

  public boolean isExcludeArray() {
    return _excludeArray;
  }

  public void setExcludeArray(boolean excludeArray) {
    _excludeArray = excludeArray;
  }

  public boolean isDisableCrossArrayUnnest() {
    return _disableCrossArrayUnnest;
  }

  public void setDisableCrossArrayUnnest(boolean disableCrossArrayUnnest) {
    _disableCrossArrayUnnest = disableCrossArrayUnnest;
  }

  @Nullable
  public Set<String> getIncludePaths() {
    return _includePaths;
  }

  public void setIncludePaths(@Nullable Set<String> includePaths) {
    Preconditions.checkArgument(includePaths == null || _excludePaths == null,
        "Cannot configure both include and exclude paths");
    Preconditions.checkArgument(includePaths == null || includePaths.size() > 0, "Include paths cannot be empty");
    _includePaths = includePaths;
  }

  @Nullable
  public Set<String> getExcludePaths() {
    return _excludePaths;
  }

  public void setExcludePaths(@Nullable Set<String> excludePaths) {
    Preconditions.checkArgument(excludePaths == null || _includePaths == null,
        "Cannot configure both include and exclude paths");
    _excludePaths = excludePaths;
  }

  @Nullable
  public Set<String> getExcludeFields() {
    return _excludeFields;
  }

  public void setExcludeFields(@Nullable Set<String> excludeFields) {
    _excludeFields = excludeFields;
  }

  @Nullable
  public Set<String> getIndexPaths() {
    return _indexPaths;
  }

  public void setIndexPaths(@Nullable Set<String> indexPaths) {
    _indexPaths = indexPaths;
  }

  public int getMaxValueLength() {
    return _maxValueLength;
  }

  public void setMaxValueLength(int maxValueLength) {
    _maxValueLength = maxValueLength;
  }

  public boolean getSkipInvalidJson() {
    return _skipInvalidJson;
  }

  public void setSkipInvalidJson(boolean skipInvalidJson) {
    _skipInvalidJson = skipInvalidJson;
  }

  public long getMaxBytesSize() {
    return _maxBytesSize;
  }

  public void setMaxBytesSize(int maxBytesSize) {
    Preconditions.checkArgument(maxBytesSize > 0, "Max bytes size must be greater than 0");
    _maxBytesSize = maxBytesSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    JsonIndexConfig config = (JsonIndexConfig) o;
    return _maxLevels == config._maxLevels && _excludeArray == config._excludeArray
        && _disableCrossArrayUnnest == config._disableCrossArrayUnnest && Objects.equals(_includePaths,
        config._includePaths) && Objects.equals(_excludePaths, config._excludePaths) && Objects.equals(_excludeFields,
        config._excludeFields) && _maxValueLength == config._maxValueLength
        && _skipInvalidJson == config._skipInvalidJson;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _maxLevels, _excludeArray, _disableCrossArrayUnnest, _includePaths,
        _excludePaths, _excludeFields, _maxValueLength, _skipInvalidJson);
  }
}
