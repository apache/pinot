/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.metadata.segment;

import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.configuration.Configuration;


/**
 * This is resource level index loading configs.
 *
 */
public class IndexLoadingConfigMetadata {
  public static final String KEY_OF_LOADING_INVERTED_INDEX = "metadata.loading.inverted.index.columns";
  public static final String KEY_OF_SEGMENT_FORMAT_VERSION = "segment.format.version";
  public static final String KEY_OF_ENABLE_DEFAULT_COLUMNS = "enable.default.columns";
  public static final String KEY_OF_STAR_TREE_FORMAT_VERSION = "startree.format.version";
  public static final String KEY_OF_COLUMN_MIN_MAX_VALUE_GENERATOR_MODE = "column.min.max.value.generator.mode";

  private static final String DEFAULT_SEGMENT_FORMAT = "v1";
  private static final String DEFAULT_COLUMN_MIN_MAX_VALUE_GENERATOR_MODE = "NONE";

  private final Set<String> _loadingInvertedIndexColumnSet = new HashSet<>();
  private final String _segmentVersionToLoad;
  private final String _starTreeVersionToLoad;
  private boolean _enableDefaultColumns;
  private String _generateColumnMinMaxValueMode;

  @SuppressWarnings("unchecked")
  public IndexLoadingConfigMetadata(Configuration tableDataManagerConfig) {
    List<String> valueOfLoadingInvertedIndexConfig =
        tableDataManagerConfig.getList(KEY_OF_LOADING_INVERTED_INDEX, null);
    if ((valueOfLoadingInvertedIndexConfig != null) && (!valueOfLoadingInvertedIndexConfig.isEmpty())) {
      initLoadingInvertedIndexColumnSet(
          valueOfLoadingInvertedIndexConfig.toArray(new String[valueOfLoadingInvertedIndexConfig.size()]));
    }

    _segmentVersionToLoad = tableDataManagerConfig.getString(KEY_OF_SEGMENT_FORMAT_VERSION, DEFAULT_SEGMENT_FORMAT);
    _enableDefaultColumns = tableDataManagerConfig.getBoolean(KEY_OF_ENABLE_DEFAULT_COLUMNS, false);
    _starTreeVersionToLoad = tableDataManagerConfig.getString(KEY_OF_STAR_TREE_FORMAT_VERSION,
        CommonConstants.Server.DEFAULT_STAR_TREE_FORMAT_VERSION);
    _generateColumnMinMaxValueMode = tableDataManagerConfig.getString(KEY_OF_COLUMN_MIN_MAX_VALUE_GENERATOR_MODE,
        DEFAULT_COLUMN_MIN_MAX_VALUE_GENERATOR_MODE);
  }

  public void initLoadingInvertedIndexColumnSet(String[] columnCollections) {
    _loadingInvertedIndexColumnSet.addAll(Arrays.asList(columnCollections));
  }

  public Set<String> getLoadingInvertedIndexColumns() {
    return _loadingInvertedIndexColumnSet;
  }

  public boolean isLoadingInvertedIndexForColumn(String columnName) {
    return _loadingInvertedIndexColumnSet.contains(columnName);
  }

  public String segmentVersionToLoad() {
    return _segmentVersionToLoad;
  }

  public static String getKeyOfLoadingInvertedIndex() {
    return KEY_OF_LOADING_INVERTED_INDEX;
  }

  public void setEnableDefaultColumns(boolean enableDefaultColumns) {
    _enableDefaultColumns = enableDefaultColumns;
  }

  public boolean isEnableDefaultColumns() {
    return _enableDefaultColumns;
  }

  public void setGenerateColumnMinMaxValueMode(String generateColumnMinMaxValueMode) {
    _generateColumnMinMaxValueMode = generateColumnMinMaxValueMode;
  }

  public String getGenerateColumnMinMaxValueMode() {
    return _generateColumnMinMaxValueMode;
  }

  public String getStarTreeVersionToLoad() {
    return _starTreeVersionToLoad;
  }
}
