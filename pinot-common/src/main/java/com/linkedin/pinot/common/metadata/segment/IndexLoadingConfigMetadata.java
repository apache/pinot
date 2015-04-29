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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;


/**
 * This is per table index loading configs.
 *
 */
public class IndexLoadingConfigMetadata {

  private final static String PREFIX_OF_KEY_OF_LOADING_INVERTED_INDEX = "metadata.loading.inverted.index.columns";
  private final Map<String, Set<String>> _tableToloadingInvertedIndexColumnMap = new HashMap<String, Set<String>>();

  public IndexLoadingConfigMetadata(Configuration resourceDataManagerConfig) {
    Iterator<String> keyIter = resourceDataManagerConfig.getKeys(PREFIX_OF_KEY_OF_LOADING_INVERTED_INDEX);
    while (keyIter.hasNext()) {
      String key = keyIter.next();
      String tableName = key.substring(PREFIX_OF_KEY_OF_LOADING_INVERTED_INDEX.length() + 1);
      List<String> valueOfLoadingInvertedIndexConfig = resourceDataManagerConfig.getList(key, null);
      if ((valueOfLoadingInvertedIndexConfig != null) && (!valueOfLoadingInvertedIndexConfig.isEmpty())) {
        initLoadingInvertedIndexColumnSet(tableName, valueOfLoadingInvertedIndexConfig.toArray(new String[0]));
      }
    }
  }

  public void initLoadingInvertedIndexColumnSet(String tableName, String[] columnCollections) {
    _tableToloadingInvertedIndexColumnMap.put(tableName, new HashSet<String>(Arrays.asList(columnCollections)));
  }

  public Set<String> getLoadingInvertedIndexColumns(String tableName) {
    return _tableToloadingInvertedIndexColumnMap.get(tableName);
  }

  public boolean isLoadingInvertedIndexForColumn(String tableName, String columnName) {
    if (_tableToloadingInvertedIndexColumnMap.containsKey(tableName)) {
      return _tableToloadingInvertedIndexColumnMap.get(tableName).contains(columnName);
    } else {
      return false;
    }
  }

  public boolean containsTable(String tableName) {
    return _tableToloadingInvertedIndexColumnMap.containsKey(tableName);
  }

  public int size() {
    return _tableToloadingInvertedIndexColumnMap.size();
  }
}
