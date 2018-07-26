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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigIncludeContext;
import com.typesafe.config.ConfigIncluder;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigValue;
import io.vavr.Tuple;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import java.io.File;
import java.util.Arrays;
import java.util.Map;


/**
 * Combined configuration loader, used to load combined configurations from instances of {@link java.io.File},
 * {@link java.lang.String}, {@link java.util.Map}, etc.
 */
public class CombinedConfigLoader {
  static io.vavr.collection.Map<String, ?> loadConfigFromFile(File file, String... profiles) {
    ConfigParseOptions options = ConfigParseOptions.defaults().prependIncluder(new ConfigIncluder() {
      private ConfigIncluder parent = null;

      public ConfigObject include(ConfigIncludeContext context, String what) {
        return ConfigFactory.parseFileAnySyntax(new File(what)).root();
      }

      public ConfigIncluder withFallback(ConfigIncluder fallback) {
        parent = fallback;
        return this;
      }
    });

    Config config = ConfigFactory.parseFile(file, options);

    // Load profiles
    for (String profile : profiles) {
      Config profileConfig = ConfigFactory.parseFile(new File("profiles", profile + ".conf"), options);
      config = config.withFallback(profileConfig);
    }

    config = config.resolve();

    config = processProfileConditionals(config, profiles);

    return HashSet.ofAll(config.entrySet())
        .toMap(entry -> Tuple.of(entry.getKey(), entry.getValue().unwrapped()));
  }

  private static Config processProfileConditionals(Config config, String... profiles) {
    Set<String> enabledProfiles = HashSet.ofAll(Arrays.asList(profiles));

    io.vavr.collection.Map<String, ConfigValue> configMap =
        HashSet.ofAll(config.entrySet()).toMap(entry -> Tuple.of(entry.getKey(), entry.getValue()));

    // Get all profile-specific keys
    Set<String> profileKeys = configMap.keySet()
        .filter(key -> key.contains("_"))
        .toSet();

    // Keep profile-specific keys for enabled profiles
    Set<String> enabledProfileKeys = profileKeys
        .filter(key -> {
          int lastUnderscoreIndex = key.lastIndexOf('_');
          String profile = key.substring(lastUnderscoreIndex + 1, key.length());
          return enabledProfiles.contains(profile);
        });

    // Merge all the enabled keys together
    io.vavr.collection.Map<String, ConfigValue> overrideConfigMap = HashMap.empty();

    for (String enabledProfileKey : enabledProfileKeys) {
      int lastUnderscoreIndex = enabledProfileKey.lastIndexOf('_');
      String destinationKey = enabledProfileKey.substring(0, lastUnderscoreIndex);

      if (!overrideConfigMap.containsKey(destinationKey)) {
        overrideConfigMap = overrideConfigMap.put(Tuple.of(destinationKey, config.getValue(enabledProfileKey)));
      } else {
        // Value already exists, ensure that it is the same
        ConfigValue previousOverrideValue = overrideConfigMap.get(destinationKey).get();
        ConfigValue newConfigValue = config.getValue(enabledProfileKey);
        if (!EqualityUtils.isEqual(previousOverrideValue.unwrapped(), newConfigValue.unwrapped())) {
          throw new RuntimeException("Found conflicting value for key " + destinationKey +
              " due to multiple enabled profiles for this configuration key. Previous override was " +
              previousOverrideValue.unwrapped() + ", new override value is " + newConfigValue.unwrapped() +
              ". Ensure that all enabled profiles for this profile override key have the same resulting value.");
        }
      }
    }

    // Remove the profile keys
    for (String profileKey : profileKeys) {
      config = config.withoutPath(profileKey);
    }

    // Merge the configs
    config = ConfigFactory.parseMap(overrideConfigMap.toJavaMap()).withFallback(config);

    return config;
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

  public static CombinedConfig loadCombinedConfig(File file, String... profiles) {
    return loadCombinedConfig(loadConfigFromFile(file, profiles));
  }

  public static CombinedConfig loadCombinedConfig(String string) {
    return loadCombinedConfig(loadConfigFromString(string));
  }
}
