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
import io.vavr.collection.Map;
import java.util.HashMap;


/**
 * Child key handler for maps which have values that require deserialization.
 */
public class TypedMapChildKeyHandler<T> implements ChildKeyHandler<java.util.Map<String, T>> {
  private Class<T> _type;

  protected TypedMapChildKeyHandler(Class<T> type) {
    _type = type;
  }

  @Override
  public java.util.Map<String, T> handleChildKeys(Map<String, ?> childKeys, String pathPrefix) {
    java.util.Map<String, T> returnedMap = new HashMap<>();

    childKeys
        .groupBy(tuple2 -> tuple2._1.split("\\.", 2)[0])
        .map((key, values) -> {
          // Drop the prefix
          Map<String, ?> valuesWithoutPrefix = values
              .map((configKey, configValue) -> Tuple.of(configKey.substring(key.length() + 1), configValue));

          T value;
          try {
            value = Deserializer.deserialize(_type, valuesWithoutPrefix, "");
          } catch (Exception e) {
            value = null;
            e.printStackTrace();
          }

          return Tuple.of(key, value);
        })
        .forEach(returnedMap::put);

    if (returnedMap.isEmpty()) {
      return null;
    } else {
      return returnedMap;
    }
  }

  @Override
  public Map<String, ?> unhandleChildKeys(java.util.Map<String, T> value, String pathPrefix) {
    if (value == null) {
      return null;
    }

    final io.vavr.collection.HashMap<String, ?> serialized = io.vavr.collection.HashMap.ofAll(value)
        .flatMap((columnName, columnPartitionConfig) -> Serializer.serialize(columnPartitionConfig)
            .map((key, obj) -> Tuple.of(columnName + "." + key, obj)));
    return serialized;
  }
}
