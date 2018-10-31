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

package com.linkedin.pinot.common.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigIncludeContext;
import com.typesafe.config.ConfigIncluder;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigParseOptions;
import io.vavr.Tuple;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import java.io.File;
import java.util.Map;


/**
 * Combined configuration loader, used to load combined configurations from instances of {@link java.io.File},
 * {@link java.lang.String}, {@link java.util.Map}, etc.
 */
public class CombinedConfigLoader {
  static io.vavr.collection.Map<String, ?> loadConfigFromFile(File file) {
    Config config = ConfigFactory.parseFile(file,
        ConfigParseOptions.defaults().prependIncluder(new ConfigIncluder() {
          private ConfigIncluder parent = null;

          public ConfigObject include(ConfigIncludeContext context, String what) {
            return ConfigFactory.parseFileAnySyntax(new File(what)).root();
          }

          public ConfigIncluder withFallback(ConfigIncluder fallback) {
            parent = fallback;
            return this;
          }
        }));

    config = config.resolve();

    return HashSet.ofAll(config.entrySet())
        .toMap(entry -> Tuple.of(entry.getKey(), entry.getValue().unwrapped()));
  }

  static io.vavr.collection.Map<String, ?> loadConfigFromString(String string) {
    Config config = ConfigFactory.parseString(string,
        ConfigParseOptions.defaults().prependIncluder(new ConfigIncluder() {
          private ConfigIncluder parent = null;

          public ConfigObject include(ConfigIncludeContext context, String what) {
            return ConfigFactory.parseFileAnySyntax(new File(what)).root();
          }

          public ConfigIncluder withFallback(ConfigIncluder fallback) {
            parent = fallback;
            return this;
          }
        }));

    config = config.resolve();

    return HashSet.ofAll(config.entrySet())
        .toMap(entry -> Tuple.of(entry.getKey(), entry.getValue().unwrapped()));
  }

  public static CombinedConfig loadCombinedConfig(io.vavr.collection.Map<String, ?> config) {
    // Deserialize the combined config
    try {
      return Deserializer.deserialize(CombinedConfig.class, config, "");
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public static CombinedConfig loadCombinedConfig(Map<String, ?> config) {
    return loadCombinedConfig(HashMap.ofAll(config));
  }

  public static CombinedConfig loadCombinedConfig(File file) {
    return loadCombinedConfig(loadConfigFromFile(file));
  }

  public static CombinedConfig loadCombinedConfig(String string) {
    return loadCombinedConfig(loadConfigFromString(string));
  }
}
