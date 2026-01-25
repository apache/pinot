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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * Utility helpers for forward index configuration.
 */
public final class ForwardIndexUtils {
  private ForwardIndexUtils() {
  }

  /**
   * Returns the set of columns that are configured to use raw forward indexes.
   * This includes legacy no-dictionary settings and FieldConfig RAW encoding.
   */
  public static Set<String> getRawForwardIndexColumns(TableConfig tableConfig) {
    Set<String> rawColumns = new HashSet<>();
    if (tableConfig == null) {
      return rawColumns;
    }
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    if (indexingConfig != null) {
      List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
      if (noDictionaryColumns != null) {
        rawColumns.addAll(noDictionaryColumns);
      }
      Map<String, String> noDictionaryConfig = indexingConfig.getNoDictionaryConfig();
      if (noDictionaryConfig != null) {
        rawColumns.addAll(noDictionaryConfig.keySet());
      }
    }
    List<FieldConfig> fieldConfigs = tableConfig.getFieldConfigList();
    if (fieldConfigs != null) {
      for (FieldConfig fieldConfig : fieldConfigs) {
        if (fieldConfig.getEncodingType() == FieldConfig.EncodingType.RAW) {
          rawColumns.add(fieldConfig.getName());
        }
      }
    }
    return rawColumns;
  }
}
