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
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import io.vavr.control.Option;
import java.util.function.Function;


/**
 * Handler for list of items that have a "name" property
 */
public class NamedListChildKeyHandler<T> implements ChildKeyHandler<java.util.List<T>> {
  private Class<T> _type;

  protected NamedListChildKeyHandler(Class<T> type) {
    _type = type;
  }

  @Override
  public java.util.List<T> handleChildKeys(Map<String, ?> childKeys, String pathPrefix) {
    Seq<T> valueList = childKeys
        .groupBy(tuple2 -> tuple2._1.split("\\.", 2)[0])
        .flatMap(tuple2 -> {
          String key = tuple2._1;
          Map<String, ?> values = tuple2._2;

          // Drop the prefix
          Map<String, Object> valuesWithoutPrefix = values
              .map((configKey, configValue) -> Tuple.of(configKey.substring(key.length() + 1), configValue));
          valuesWithoutPrefix = valuesWithoutPrefix.put("name", key);

          T value;
          try {
            value = Deserializer.deserialize(_type, valuesWithoutPrefix, "");
            return Option.some(value);
          } catch (Exception e) {
            e.printStackTrace();
            return Option.none();
          }
        });

    return valueList.asJava();
  }

  @Override
  public Map<String, ?> unhandleChildKeys(java.util.List<T> values, String pathPrefix) {
    if (values == null) {
      return null;
    }

    return List.ofAll(values)
        .flatMap(value -> {
          Map<String, ?> serializedValue = Serializer.serialize(value);
          final String name = (String) serializedValue.getOrElse("name", null);
          return serializedValue
              .remove("name")
              .mapKeys(key -> name + "." + key);
        })
        .toMap(Function.identity());
  }
}
