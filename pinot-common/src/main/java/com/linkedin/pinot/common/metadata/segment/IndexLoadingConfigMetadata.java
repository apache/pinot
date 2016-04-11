/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
  private final Set<String> _loadingInvertedIndexColumnSet = new HashSet<String>();
  private final String DEFAULT_SEGMENT_FORMAT = "v1";
  private String segmentVersionToLoad;

  public IndexLoadingConfigMetadata(Configuration tableDataManagerConfig) {
    List<String> valueOfLoadingInvertedIndexConfig = tableDataManagerConfig.getList(KEY_OF_LOADING_INVERTED_INDEX, null);
    if ((valueOfLoadingInvertedIndexConfig != null) && (!valueOfLoadingInvertedIndexConfig.isEmpty())) {
      initLoadingInvertedIndexColumnSet(valueOfLoadingInvertedIndexConfig.toArray(new String[0]));
    }

    segmentVersionToLoad = tableDataManagerConfig.getString(KEY_OF_SEGMENT_FORMAT_VERSION, DEFAULT_SEGMENT_FORMAT);
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
    return segmentVersionToLoad;
  }

  public static String getKeyOfLoadingInvertedIndex() {
    return KEY_OF_LOADING_INVERTED_INDEX;
  }
}
