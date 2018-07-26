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

import com.linkedin.pinot.common.utils.EqualityUtils;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import io.vavr.collection.Set;
import io.vavr.control.Option;


/**
 * Reorganize config keys to separate a user friendly combined config into offline, realtime, and schema configs.
 */
public class CombinedConfigSeparatorChildKeyTransformer implements ChildKeyTransformer {
  @Override
  public Map<String, ?> apply(Map<String, ?> config, String keyPrefix) {
    // Generate key for schema name
    Option<String> tableName = config.get("table.name").map(Object::toString);

    if (tableName.isDefined() && config.getOrElse("table.schemaName", null) == null) {
      config = ((Map<String, Object>) config).put("table.schemaName", tableName.get());
    }

    // Check if this config has both offline and realtime
    final boolean hasRealtime = config.containsKey("table.type.realtime");
    final boolean hasOffline = config.containsKey("table.type.offline");

    // Move keys around so that they match with the combined config
    Map<String, Object> remappedConfig = config.flatMap((k, v) -> {
      if(k.startsWith("table.schema.")) {
        // table.schema.foo -> schema.foo
        return List.of(Tuple.of(k.replaceFirst("table.schema", "schema"), v));
      } else if (k.endsWith(".realtime") && hasRealtime) {
        // Realtime-only config? Move from table.foo.realtime -> realtime.table.foo
        return List.of(Tuple.of("realtime." + k.substring(0, k.length() - ".realtime".length()), v));
      } else if (k.endsWith(".offline") && hasOffline) {
        // Offline-only config? Move from table.foo.offline -> offline.table.foo
        return List.of(Tuple.of("offline." + k.substring(0, k.length() - ".offline".length()), v));
      } else {
        // Config for both? Copy the key from table.foo -> realtime.table.foo and offline.table.foo
        Seq<Tuple2<String, Object>> returnValue = List.empty();

        if (hasOffline) {
          returnValue = returnValue.append(Tuple.of("offline." + k, v));
        }

        if (hasRealtime) {
          returnValue = returnValue.append(Tuple.of("realtime." + k, v));
        }

        return returnValue;
      }
    });

    return remappedConfig;
  }

  @Override
  public Map<String, ?> unapply(Map<String, ?> config, String keyPrefix) {
    // TODO Check if the table is hybrid or not
    // Move keys around so that they provide a nice config file format
    Map<String, Object> remappedConfig = (Map<String, Object>) config.mapKeys(key -> {
      if (key.startsWith("schema.")) {
        // schema.foo -> table.schema.foo
        return key.replaceFirst("schema.", "table.schema.");
      } else if (key.startsWith("realtime.")) {
        // realtime.table.foo -> table.foo.realtime
        return key.replaceFirst("realtime.", "") + ".realtime";
      } else if (key.startsWith("offline.")) {
        // offline.table.foo -> table.foo.offline
        return key.replaceFirst("offline.", "") + ".offline";
      } else {
        throw new RuntimeException("Unexpected key " + key);
      }
    });

    // Merge keys realtime/offline key pairs with the same value
    // table.foo.realtime + table.foo.offline -> table.foo?
    Set<String> realtimeKeySet = remappedConfig.keySet()
        .filter(key -> key.startsWith("table.") && key.endsWith(".realtime"))
        .map(key -> key.substring(0, key.lastIndexOf(".realtime")));

    Set<String> offlineKeySet = remappedConfig.keySet()
        .filter(key -> key.startsWith("table.") && key.endsWith(".offline"))
        .map(key -> key.substring(0, key.lastIndexOf(".offline")));

    Set<String> commonOfflineAndRealtimeKeys = realtimeKeySet.intersect(offlineKeySet);

    for (String commonOfflineAndRealtimeKey : commonOfflineAndRealtimeKeys) {
      String realtimeKey = commonOfflineAndRealtimeKey + ".realtime";
      String offlineKey = commonOfflineAndRealtimeKey + ".offline";

      Object realtimeValue = remappedConfig.getOrElse(realtimeKey, null);
      Object offlineValue = remappedConfig.getOrElse(offlineKey, null);

      if (EqualityUtils.isEqual(offlineValue, realtimeValue)) {
        remappedConfig = remappedConfig
            .remove(realtimeKey)
            .remove(offlineKey)
            .put(commonOfflineAndRealtimeKey, offlineValue);
      }
    }

    return remappedConfig;
  }
}
