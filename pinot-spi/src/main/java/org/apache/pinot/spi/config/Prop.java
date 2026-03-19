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
package org.apache.pinot.spi.config;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;


/// Typed property contract, which can optionally read from runtime map (usually query options)
public interface Prop<T> {
  /// Parses one raw value for this property.
  @FunctionalInterface
  interface OptionValueParser<T> {
    @Nullable
    T parse(String concept, String rawValue);
  }

  /// Resolves the property from explicit runtime map/config.
  @Nullable
  T resolve(@Nullable Map<String, String> runtimeMap, @Nullable PinotConfiguration pinotConfiguration);

  /// Resolves the property from the current [QueryThreadContext].
  @Nullable
  default T resolve() {
    QueryThreadContext queryThreadContext = QueryThreadContext.get();
    return resolve(queryThreadContext.getQueryOptions(), queryThreadContext.getPinotConfiguration());
  }

  static Prop<Long> fromNullLong() {
    return fromDefault(null);
  }

  static Prop<String> fromNullString() {
    return fromDefault(null);
  }

  static Prop<Integer> fromNullInteger() {
    return fromDefault(null);
  }

  static Prop<Boolean> fromNullBoolean() {
    return fromDefault(null);
  }

  static <T> Prop<T> fromNull() {
    return fromDefault(null);
  }

  static <T> Prop<T> fromDefault(T defaultValue) {
    return (runtimeMap, config) -> defaultValue;
  }

  default Prop<T> withRuntimeMap(String runtimeKey, OptionValueParser<T> parser) {
    Prop<T> parent = this;
    return (runtimeMap, config) -> {
      if (runtimeMap == null || !runtimeMap.containsKey(runtimeKey)) {
        return parent.resolve(runtimeMap, config);
      }
      String value = runtimeMap.get(runtimeKey);
      if (value == null) {
        return parent.resolve(runtimeMap, config);
      }
      T parsedValue = parser.parse(runtimeKey, value);
      return parsedValue != null ? parsedValue : parent.resolve(runtimeMap, config);
    };
  }

  default Prop<T> withConfig(String configKey, OptionValueParser<T> parser) {
    Prop<T> parent = this;
    return (runtimeMap, config) -> {
      if (config == null || !config.containsKey(configKey)) {
        return parent.resolve(runtimeMap, config);
      }
      String value = config.getProperty(configKey);
      if (value == null) {
        return parent.resolve(runtimeMap, config);
      }
      T parsedValue = parser.parse(configKey, value);
      return parsedValue != null ? parsedValue : parent.resolve(runtimeMap, config);
    };
  }
}
