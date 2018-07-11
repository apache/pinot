/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.utils.CommonConstants;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import java.util.Collection;


/**
 * Adjusts the name of tables to add/remove the suffix as needed (offline/realtime table configs require the suffix, but
 * the configs shown to the user don't have it).
 */
public class AdjustTableNameChildKeyTransformer implements ChildKeyTransformer {
  @Override
  public Map<String, ?> apply(Map<String, ?> childKeys, String pathPrefix) {
    // Adjust the name to add the table suffix to table.name.realtime/table.name.offline
    List<String> tableTypes = childKeys
        .get("table.types")
        .map(tableTypesListOrString -> {
          if (tableTypesListOrString instanceof String) {
            return List.of((String) tableTypesListOrString);
          } else if (tableTypesListOrString instanceof Collection) {
            return List.ofAll((Collection<String>) tableTypesListOrString);
          } else {
            return List.empty();
          }
        })
        .getOrElse(List.empty())
        .map(Object::toString);

    String tableName = childKeys.get("table.name").map(Object::toString).getOrNull();

    Map<String, Object> remappedConfig = (Map<String, Object>) childKeys;

    for (String tableType : tableTypes) {
      String tableNameKey = "table.name." + tableType.toLowerCase();
      CommonConstants.Helix.TableType type = CommonConstants.Helix.TableType.valueOf(tableType.toUpperCase());

      remappedConfig = remappedConfig.put(tableNameKey, TableNameBuilder.forType(type).tableNameWithType(tableName));
    }

    remappedConfig = remappedConfig.remove("table.name");

    return remappedConfig;
  }

  @Override
  public Map<String, ?> unapply(Map<String, ?> childKeys, String pathPrefix) {
    String tableNameWithSuffix = childKeys.filterKeys(key -> key.startsWith("table.name")).head()._2.toString();

    return ((Map<String, Object>) childKeys)
        .filterKeys(key -> !key.startsWith("table.name"))
        .put("table.name", TableNameBuilder.extractRawTableName(tableNameWithSuffix));
  }
}
