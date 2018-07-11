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

import io.vavr.Tuple;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import java.util.Collection;
import java.util.function.Function;


/**
 * Expand table.types=[offline,realtime] => table.type.realtime=REALTIME, table.type.offline=OFFLINE
 */
public class RemapTableTypesChildKeyTransformer implements ChildKeyTransformer {
  @Override
  public Map<String, ?> apply(Map<String, ?> config, String keyPrefix) {
    // Generate keys for table types based on table.types
    // eg. table.types=[offline,realtime] -> table.type.realtime=realtime, table.type.offline=offline
    config = ((Map<String, Object>) config).merge(
        config.get("table.types")
            .map(typeList -> {
              if (typeList instanceof String) {
                return List.of((String) typeList);
              } else if (typeList instanceof Collection){
                return List.ofAll((Collection<String>) typeList);
              } else {
                return List.empty();
              }
            })
            .map(typeList -> typeList
                .map(type -> Tuple.of("table.type." + type.toString().toLowerCase(), type))
                .toMap(Function.identity()))
            .getOrElse(HashMap::empty)
    );

    config = config.remove("table.types");

    return config;
  }

  @Override
  public Map<String, ?> unapply(Map<String, ?> config, String keyPrefix) {
    Map<String, ?> tableTypeMap = config
        .filterKeys(key -> key.startsWith("table.type."));

    java.util.List<String> tableTypes = tableTypeMap.values().map(Object::toString).toSet().toJavaList();

    Map<String, Object> remappedConfig = ((Map<String, Object>) config)
        .removeAll(tableTypeMap.keySet())
        .put("table.types", tableTypes);

    return remappedConfig;
  }
}
