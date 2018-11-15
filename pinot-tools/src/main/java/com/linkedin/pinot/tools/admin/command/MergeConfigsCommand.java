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

package com.linkedin.pinot.tools.admin.command;

import com.google.common.base.Splitter;
import com.linkedin.pinot.tools.Command;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigIncludeContext;
import com.typesafe.config.ConfigIncluder;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import com.typesafe.config.impl.ConfigReferenceHelper;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import io.vavr.collection.Set;
import io.vavr.control.Either;
import io.vavr.control.Option;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Command to merge config files coming from different Pinot clusters to avoid repeated values.
 */
public class MergeConfigsCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(MergeConfigsCommand.class);
  private static final String[] CONFIG_FILE_EXTENSIONS = {"conf"};
  private static final String PROFILE_SEPARATOR = "___";

  @org.kohsuke.args4j.Option(name = "-inputDir", required = true, metaVar = "<String>", usage = "Input directory containing configuration files to merge.")
  private String _inputDir;

  @org.kohsuke.args4j.Option(name = "-outputDir", required = false, metaVar = "<String>", usage = "Output directory for the merged configuration files.")
  private String _outputDir;

  @org.kohsuke.args4j.Option(name = "-profileDir", required = false, metaVar = "<String>", usage = "Directory containing configuration profiles.")
  private String _profileDir;

  // jfim: Since there's no typedef in Java, use a generic parameter for the config value type (it gets erased to
  // Object, but makes the types below clearer)
  private <TYPE> boolean executeInternal() throws Exception {
    // Build a list of all input files and their associated configuration profiles
    LOGGER.info("Searching for configs...");

    File inputDir = new File(_inputDir);
    if (!inputDir.exists()) {
      throw new RuntimeException("Input directory " + inputDir + " does not exist!");
    } else if (!inputDir.isDirectory()) {
      throw new RuntimeException("Input directory " + inputDir + " is not a directory!");
    }

    List<File> configFiles = List.ofAll(FileUtils.listFiles(inputDir, CONFIG_FILE_EXTENSIONS, true));

    List<Tuple2<File, Set<String>>> configFilesAndProfiles = configFiles.map(configFile -> {
      File currentFile = configFile.getParentFile();
      Set<String> configurationProfiles = HashSet.empty();

      // Iterate through parent files until the config directory is found to build the set of configuration profiles
      // that apply to this particular configuration file
      while(!currentFile.equals(inputDir)) {
        configurationProfiles = configurationProfiles.add(currentFile.getName());

        File parentOfCurrentFile = currentFile.getParentFile();
        if (parentOfCurrentFile != null) {
          currentFile = parentOfCurrentFile;
        } else {
          throw new RuntimeException("Failed to get parent of " + currentFile.getPath() + " while finding configuration profiles for " + configFile.getPath());
        }
      }

      return Tuple.of(configFile, configurationProfiles);
    });

    LOGGER.info("Found {} config files", configFilesAndProfiles.length());

    // Load all the configuration profiles
    LOGGER.info("Loading all configuration profiles...");

    if (_profileDir == null) {
      _profileDir = "." + File.separator + "profiles";
    }
    File profileDir = new File(_profileDir);

    List<Tuple2<String, Config>> profileConfigurations;
    if (!profileDir.exists()) {
      System.out.println("Input directory " + inputDir + " does not exist.");
      profileConfigurations = List.empty();
    } else if (!inputDir.isDirectory()) {
      throw new RuntimeException("Input directory " + inputDir + " is not a directory!");
    } else {
      profileConfigurations = List
          .ofAll(FileUtils.listFiles(profileDir, CONFIG_FILE_EXTENSIONS, true))
          .map(file -> {
            Config config = loadConfigFromFile(file);
            String configName = file.getName().replaceAll("\\.conf$", "");

            return Tuple.of(configName, config);
          });
    }

    LOGGER.info("Loaded {} configuration profiles: {}", profileConfigurations.length(), profileConfigurations.map(Tuple2::_1).asJava());

    // Build a map of value to configuration key for each configuration profile
    Map<TYPE, Map<String, Set<String>>> valueToProfileAndKeys = profileConfigurations
        .map(profileNameAndConfigTuple -> {
          String profileName = profileNameAndConfigTuple._1;
          Config config = profileNameAndConfigTuple._2;

          Seq<Tuple2<TYPE, String>> configValuesAndKeys = HashSet
              .ofAll(config.entrySet())
              .toList()
              .map(entry -> Tuple.of((TYPE) entry.getValue().unwrapped(), entry.getKey()));

          return Tuple.of(profileName, configValuesAndKeys);
        })
        .flatMap(profileAndValueKeyTuples -> {
          String profileName = profileAndValueKeyTuples._1;
          Seq<Tuple2<TYPE, String>> valuesAndKeys = profileAndValueKeyTuples._2;

          return valuesAndKeys
              .map(valueAndKeyTuple -> {
                TYPE value = valueAndKeyTuple._1;
                String key = valueAndKeyTuple._2;

                return Tuple.of(value, profileName, key);
              });
        })
        .groupBy(valuesProfilesAndKeys -> valuesProfilesAndKeys._1)
        .map((value, valueProfileKeyTriples) -> {
          Map<String, Set<String>> profilesAndKeys = valueProfileKeyTriples
              .groupBy(valueProfileKeyTriple -> valueProfileKeyTriple._2)
              .map((profile, valueProfileKeyTriples2) -> {
                Set<String> keys = valueProfileKeyTriples2
                    .map(valueProfileKeyTriple -> valueProfileKeyTriple._3)
                    .toSet();

                return Tuple.of(profile, keys);
              })
              .toMap(Function.identity());

          return Tuple.of(value, profilesAndKeys);
        })
        .toMap(Function.identity())
        .removeKeys(key -> key.toString().trim().isEmpty());

    // Group input files by their name
    Map<String, Config> mergedConfigs = configFilesAndProfiles
        .groupBy(configFileAndProfiles -> configFileAndProfiles._1.getName())
        .map((configFilename, configFileAndProfilesTuples) -> {
          System.out.print("\rWorking on " + configFilename + "                            ");

          Map<File, Set<String>> fileToProfilesMap = configFileAndProfilesTuples.toMap(Function.identity());
          Map<File, Config> configs = fileToProfilesMap
              .map((file, profiles) -> Tuple.of(file, expandConfig(loadConfigFromFile(file))));

          // Generate all combinations of profiles that cover each file exactly once
          Set<File> allConfigFiles = configFileAndProfilesTuples.map(Tuple2::_1).toSet();
          Set<String> allProfiles = configFileAndProfilesTuples.flatMap(Tuple2::_2).toSet();

          Map<String, Set<File>> profileToConfigFiles = configFileAndProfilesTuples
              .flatMap(fileAndConfigsTuple -> fileAndConfigsTuple._2
                  .map(profile -> Tuple.of(profile, fileAndConfigsTuple._1)))
              .groupBy(Tuple2::_1)
              .toMap(profileToProfileFileTuples -> Tuple.of(profileToProfileFileTuples._1, profileToProfileFileTuples._2
                  .map(Tuple2::_2)
                  .toSet()));
          List<List<String>> allProfileCombinations = allProfiles.toList().sortBy(String::length).combinations();
          List<List<String>> validProfileCombinations = allProfileCombinations
              .filter(profiles -> {
                List<File> filesForProfiles = profiles.flatMap(profileToConfigFiles::apply);
                Set<File> filesCovered = filesForProfiles.toSet();
                return filesForProfiles.size() == filesForProfiles.distinct().size() && filesCovered.equals(allConfigFiles);
              });

          // Check that each config file has at least one profile that identifies it uniquely
          Map<String, File> profileToFiles = configFileAndProfilesTuples
              .flatMap(fileAndProfilesTuple -> fileAndProfilesTuple._2
                  .map(profile -> Tuple.of(profile, fileAndProfilesTuple._1)))
              .groupBy(Tuple2::_1)
              .filter((profile, files) -> files.size() == 1)
              .toMap(profileAndFilesTuple -> Tuple.of(profileAndFilesTuple._1, profileAndFilesTuple._2.head()._2));

          Set<File> allFilesWithAtLeastOneUniqueProfileKey = profileToFiles.values().toSet();

          if (!allConfigFiles.equals(allFilesWithAtLeastOneUniqueProfileKey)) {
            System.out.println(configFilename + " does not have at least one unique profile per configuration file.");
            return Tuple.of(configFilename, null);
          }

          // Build a map of file to the unique profile name (the longest unique profile associated with this file)
          Map<File, String> fileToUniqueProfileName = profileToFiles
              .toList()
              .map(Tuple2::swap)
              .groupBy(Tuple2::_1)
              .toMap(fileToFileProfileTuples -> Tuple.of(fileToFileProfileTuples._1, fileToFileProfileTuples._2
                  .map(Tuple2::_2)
                  .maxBy(String::length)
                  .get()));

          // Compute replacements for all values that can have a replacement value
          // (config file -> config key and set of potential replacement keys)
          Map<File, Map<String, Set<String>>> perFileReplacementConfigValues = fileToProfilesMap
              .map((file, profiles) -> {
                Config config = configs.apply(file);

                Map<String, Set<String>> configValues = HashSet
                    .ofAll(config.entrySet())
                    .toMap(Tuple::fromEntry)
                    .mapValues(configValue -> valueToProfileAndKeys
                        .getOrElse((TYPE) configValue.unwrapped(), HashMap.empty())
                        .filterKeys(profiles::contains)
                        .values()
                        .fold(HashSet.empty(), Set::addAll))
                    .filterValues(replacementKeys -> !replacementKeys.isEmpty());

                return Tuple.of(file, configValues);
              });

          // Gather all config key values across all configs
          Set<String> allKeys = configs
              .toList()
              .flatMap(fileConfigTuple2 -> HashSet
                  .ofAll(fileConfigTuple2._2.entrySet())
                  .map(java.util.Map.Entry::getKey))
              .toSet();

          // Gather all keys and their potential values per file
          Map<String, Map<File, TYPE>> keyToFileValueTuples = allKeys
              .toMap(key -> Tuple.of(key,
                  configs
                      .flatMap(fileConfigTuple2 -> {
                        File file = fileConfigTuple2._1;
                        Config config = fileConfigTuple2._2;
                        if (config.hasPath(key)) {
                          return Option.some(Tuple.of(file, (TYPE) config.getValue(key).unwrapped()));
                        } else {
                          return Option.none();
                        }
                      })
                      .toMap(Function.identity())));

          Map<String, Map<File, Set<String>>> keyToFileReplacementsTuples = allKeys
              .toMap(key -> Tuple.of(key,
                  perFileReplacementConfigValues
                      .flatMap((file, replacementValues) ->
                          replacementValues.get(key).map(values -> Tuple.of(file, values)))))
              .mapValues(fileToReplacementValues -> fileToReplacementValues
                  .filterValues(replacementValues -> !replacementValues.isEmpty()))
              .filterValues(fileToReplacementValues -> !fileToReplacementValues.isEmpty());

          Set<String> keysWithReplacements = keyToFileReplacementsTuples.keySet();
          Set<String> keysWithoutReplacements = keyToFileValueTuples.keySet().removeAll(keysWithReplacements);

          Config outputConfig = ConfigFactory.empty();

          // Add all keys that have replacement values
          for (String keyWithReplacement : keysWithReplacements) {
            Map<File, Set<String>> perFilePotentialReplacements = perFileReplacementConfigValues
                .toList()
                .flatMap(fileAndKeyReplacementsTuples -> fileAndKeyReplacementsTuples._2.get(keyWithReplacement)
                    .map(replacements -> Tuple.of(fileAndKeyReplacementsTuples._1, replacements)))
                .toMap(Function.identity());

            // Do the replacements cover all files for which the key is defined?
            Set<File> filesWithThisKey = keyToFileValueTuples.apply(keyWithReplacement)
                .map(Tuple2::_1)
                .toSet();
            Set<File> filesWithReplacements = perFilePotentialReplacements.keySet();

            if (!filesWithThisKey.equals(filesWithReplacements)) {
              // Add the key to be processed without replacement
              keysWithoutReplacements = keysWithoutReplacements.add(keyWithReplacement);
            } else {
              // Build a mapping of replacement to the set of files that it applies to
              Map<String, Set<File>> replacementValuesToFiles = perFilePotentialReplacements
                  .toList()
                  .flatMap(fileAndReplacementSetTuple -> fileAndReplacementSetTuple._2
                      .map(replacement -> Tuple.of(replacement, fileAndReplacementSetTuple._1)))
                  .groupBy(Tuple2::_1)
                  .toMap(Function.identity())
                  .mapValues(replacementFileTuples -> replacementFileTuples
                      .map(Tuple2::_2)
                      .toSet());

              Set<String> profilesWithThisKey = filesWithThisKey
                  .flatMap(fileToProfilesMap::apply);

              // Build the minimal config
              List<List<String>> validProfileCombinationsForThisKey;
              if (profilesWithThisKey.equals(allProfiles)) {
                validProfileCombinationsForThisKey = validProfileCombinations;
              } else {
                validProfileCombinationsForThisKey = validProfileCombinations
                    .map(profileCombination -> profileCombination.filter(profilesWithThisKey::contains))
                    .distinct()
                    .sortBy(List::length);
              }

              Either<String, Map<String, String>> minimalConfigEither = minimalConfigMulti(
                  perFilePotentialReplacements,
                  replacementValuesToFiles,
                  validProfileCombinationsForThisKey,
                  profileToConfigFiles,
                  allConfigFiles
              );

              // Fold both sides of the either into a map of the actual values to put in the config
              Map<String, String> minimalProfileToValueMap = minimalConfigEither
                  .fold(
                      allProfilesValue -> HashMap.of(keyWithReplacement, allProfilesValue),
                      profileToValueMap -> profileToValueMap
                          .map((profile, value) -> Tuple.of(keyWithReplacement + PROFILE_SEPARATOR + profile, value )));

              // Add the values to the configuration
              for (Tuple2<String, String> profileAndValue : minimalProfileToValueMap) {
                outputConfig = outputConfig.withValue(profileAndValue._1,
                    buildReferenceConfigValue(profileAndValue._2));
              }
            }
          }

          // Add all keys that have no replacement value in the output config
          for (String keyWithoutReplacement : keysWithoutReplacements) {
            Set<File> filesWithThisKey = keyToFileValueTuples.apply(keyWithoutReplacement)
                .map(Tuple2::_1)
                .toSet();
            Set<String> profilesWithThisKey = filesWithThisKey
                .flatMap(fileToProfilesMap::apply);

            // Compute the minimal value map
            List<List<String>> validProfileCombinationsForThisKey;

            if (profilesWithThisKey.equals(allProfiles)) {
              validProfileCombinationsForThisKey = validProfileCombinations;
            } else {
              validProfileCombinationsForThisKey = validProfileCombinations
                  .map(profileCombination -> profileCombination.filter(profilesWithThisKey::contains))
                  .distinct()
                  .sortBy(List::length);
            }

            Map<File, TYPE> fileToValues = keyToFileValueTuples.apply(keyWithoutReplacement);
            Either<TYPE, Map<String, TYPE>> minimalConfigEither = minimalConfig(
                fileToValues,
                validProfileCombinationsForThisKey,
                profileToConfigFiles,
                allConfigFiles
            );

            // Fold both sides of the either into a map of the actual values to put in the config
            Map<String, TYPE> minimalProfileToValueMap = minimalConfigEither
                .fold(
                    allProfilesValue -> HashMap.of(keyWithoutReplacement, allProfilesValue),
                    profileToValueMap -> profileToValueMap
                        .map((profile, value) -> Tuple.of(keyWithoutReplacement + PROFILE_SEPARATOR + profile, value )));

            // Add the values to the configuration
            for (Tuple2<String, TYPE> profileAndValue : minimalProfileToValueMap) {
              outputConfig = outputConfig.withValue(profileAndValue._1,
                  ConfigValueFactory.fromAnyRef(profileAndValue._2));
            }
          }

          // Add the list of profiles for this file
          outputConfig = outputConfig.withValue("profiles",
              ConfigValueFactory.fromAnyRef(allProfiles.toList().sorted().toJavaList()));

          return Tuple.of(configFilename, mergeConfig(outputConfig));
        });

    // Write back the resulting configs
    mergedConfigs.forEach((filename, config) -> {
      try {
        File outputFile = new File(_outputDir, filename);
        FileUtils.write(outputFile,
            config.root().render(ConfigRenderOptions.defaults().setJson(false).setFormatted(true).setOriginComments(false)));
      } catch (IOException e) {
        LOGGER.warn("Failed to write configuration file {}", e, filename);
      }
    });

    return false;
  }

  private <T> Set<T> coerceToSet(Object object) {
    if (object instanceof Collection) {
      return HashSet.ofAll((Collection<T>) object);
    } else {
      return (Set<T>) HashSet.of(object);
    }
  }

  private Config expandConfig(Config config) {
    Set<String> configKeySet = HashSet.ofAll(config.entrySet())
        .map(java.util.Map.Entry::getKey);
    Set<String> tableTypes = this.<String>coerceToSet(config.getAnyRef("table.types"))
        .map(tableType -> tableType.trim().toLowerCase());

    for (String configKey : configKeySet) {
      if (configKey.endsWith(".offline") || configKey.endsWith(".realtime")) {
        // Nothing to do, the key is already expanded
      } else {
        // Remove the old value and add values for each table type
        ConfigValue value = config.getValue(configKey);

        config = config.withoutPath(configKey);
        for (String tableType : tableTypes) {
          config = config.withValue(configKey + "." + tableType, value);
        }
      }
    }

    return config;
  }

  private Config mergeConfig(final Config initialConfig) {
    Set<String> configKeySet = HashSet.ofAll(initialConfig.entrySet())
        .map(java.util.Map.Entry::getKey);

    Set<String> tableTypes = configKeySet
        .filter(key -> key.startsWith("table.types"))
        .flatMap(key -> this.<String>coerceToSet(initialConfig.getAnyRef(key)))
        .map(tableType -> tableType.trim().toLowerCase());

    Config config = initialConfig;
    Map<String, ConfigValue> configValueMap = HashSet.ofAll(config.entrySet())
        .toMap(Tuple::fromEntry);

    Map<String, Set<String>> configPrefixToSuffixes = configKeySet
        .filter(configKey -> configKey.contains("."))
        .groupBy(configKey -> {
          int lastPeriod = configKey.lastIndexOf('.');
          return configKey.substring(0, lastPeriod); })
        .map((configKeyPrefix, configKeys) -> Tuple.of(configKeyPrefix, configKeys.map(key ->
            key.substring(configKeyPrefix.length() + 1))));

    if (tableTypes.size() == 1) {
      // Only one table type, remove all the offline/realtime qualifiers
      String tableType = tableTypes.head();
      String tableTypeAndProfilePrefix = tableType + PROFILE_SEPARATOR;

      for (Tuple2<String, Set<String>> configPrefixAndSuffixes : configPrefixToSuffixes) {
        String configPrefix = configPrefixAndSuffixes._1;
        Set<String> configSuffixes = configPrefixAndSuffixes._2;

        for (String configSuffix : configSuffixes) {
          String fullConfigKey = configPrefix + "." + configSuffix;
          ConfigValue value = configValueMap.apply(fullConfigKey);

          if (configSuffix.equals(tableType)) {
            config = config
                .withoutPath(fullConfigKey)
                .withValue(configPrefix, value);
          } else if (configSuffix.startsWith(tableTypeAndProfilePrefix)) {
            String configProfile = configSuffix.substring(tableType.length());

            config = config
                .withoutPath(fullConfigKey)
                .withoutPath(configPrefix)
                .withValue(configPrefix + configProfile, value);
          }
        }
      }
    } else {
      for (Tuple2<String, Set<String>> configPrefixAndSuffixes : configPrefixToSuffixes) {
        String configPrefix = configPrefixAndSuffixes._1;
        Set<String> configSuffixes = configPrefixAndSuffixes._2;
        Map<String, ConfigValue> suffixValues = configSuffixes
            .toMap(suffix ->
                Tuple.of(suffix, configValueMap.apply(configPrefix + "." + suffix)));

        if (configSuffixes.equals(tableTypes)) {
          // Only one value for both?
          if (suffixValues.values().distinct().size() == 1) {
            for (String configSuffix : configSuffixes) {
              config = config.withoutPath(configPrefix + "." + configSuffix);
            }

            config = config.withValue(configPrefix, suffixValues.values().head());
          } else {
            // Nothing to do, just keep the distinct values as they are
          }
        } else {
          // Split the suffixes to extract table types and profiles
          final Splitter splitter = Splitter.on(PROFILE_SEPARATOR).limit(2).trimResults();
          Map<Tuple2<String, String>, ConfigValue> tableTypeAndProfileToValueMap = suffixValues
              .mapKeys(keyName -> List.ofAll(splitter.split(keyName)))
              .filterKeys(keyNameSplits -> keyNameSplits.length() == 2)
              .mapKeys(keyNameSplits -> Tuple.of(keyNameSplits.get(0), keyNameSplits.get(1)));

          // Group by profile
          Map<String, Map<String, ConfigValue>> profileToTableTypeValueMap = tableTypeAndProfileToValueMap
              .groupBy(tuple2_2 -> tuple2_2._1._2)
              .map((profile, group) -> Tuple.of(profile, group
                  .toMap(typeAndProfileTupleToValue -> Tuple.of(
                      typeAndProfileTupleToValue._1._1,
                      typeAndProfileTupleToValue._2))));

          // Replace values where the profile has the same value for realtime and offline
          for (Tuple2<String, Map<String, ConfigValue>> stringMapTuple2 : profileToTableTypeValueMap
              .filter((profile, typeToValueMap) -> typeToValueMap.values().distinct().size() == 1 &&
                  typeToValueMap.size() == 2)) {
            String profile = PROFILE_SEPARATOR + stringMapTuple2._1;

            for (String tableType : stringMapTuple2._2.keySet()) {
              config = config.withoutPath(configPrefix + "." + tableType + profile);
            }

            String fullKey = configPrefix + "." + stringMapTuple2._2.keySet().head() + profile;
            ConfigValue configValue = configValueMap.apply(fullKey);
            config = config.withValue(configPrefix + profile, configValue);
          }
        }
      }
    }

    // If there are any table.types that have a profile and a table type (eg. table.type.realtime___myprofile), remove
    // the table type (eg. table.type___myprofile).
    Set<String> lastConfigKeySet = HashSet.ofAll(config.entrySet())
        .map(java.util.Map.Entry::getKey);
    Map<String, Object> lastConfigValueMap = HashSet.ofAll(config.entrySet())
        .toMap(Tuple::fromEntry);

    Pattern pattern = Pattern.compile("table\\.types\\.(realtime|offline)___([^.]*)");
    for (String key : lastConfigKeySet) {
      Matcher matcher = pattern.matcher(key);

      if (matcher.matches()) {
        Object value = lastConfigValueMap.apply(key);

        config = config
            .withoutPath(key)
            .withoutPath("table.types")
            .withValue("table.types___" + matcher.group(2), ConfigValueFactory.fromAnyRef(value));
      }
    }

    return config;
  }

  private <ORIGIN_TYPE, VALUE_TYPE> Either<VALUE_TYPE, Map<String, VALUE_TYPE>> minimalConfigMulti(
      Map<ORIGIN_TYPE, Set<VALUE_TYPE>> originToPossibleValuesMap,
      Map<VALUE_TYPE, Set<ORIGIN_TYPE>> valueToOriginsMap,
      List<List<String>> validProfileCombinations,
      Map<String, Set<ORIGIN_TYPE>> profileToOriginsMap,
      Set<ORIGIN_TYPE> allOrigins
  ) {
    Set<ORIGIN_TYPE> allValidOrigins = originToPossibleValuesMap.keySet();

    // Check if there is a value that maps to all profiles
    Option<VALUE_TYPE> valueForAllProfilesOption = valueToOriginsMap
        .find(valueAndOriginsTuple -> valueAndOriginsTuple._2.equals(allValidOrigins))
        .map(valueAndOriginsTuple -> valueAndOriginsTuple._1);
    if (valueForAllProfilesOption.isDefined() && allValidOrigins.equals(allOrigins)) {
      return Either.left(valueForAllProfilesOption.get());
    }

    // Find the profile combination that has the fewest elements
    Option<Map<String, VALUE_TYPE>> bestProfileCombination = validProfileCombinations
        // For each profile, there needs to be at least one possible value that covers all the files with that profile
        .find(profileCombination -> profileCombination.forAll(profile -> {
          // Get all the possible files for that profile
          Set<ORIGIN_TYPE> allFilesWithThisProfile = profileToOriginsMap.apply(profile);

          // Get all the possible values for this profile
          Set<VALUE_TYPE> allValuesForThisProfile = profileToOriginsMap.apply(profile)
              .filter(originToPossibleValuesMap::containsKey)
              .flatMap(originToPossibleValuesMap::apply);

          // Check if there is at least one value that covers all of the files
          return allValuesForThisProfile.exists(value -> valueToOriginsMap.apply(value)
              .containsAll(allFilesWithThisProfile)); }))
        .map(profileCombination -> profileCombination.flatMap(profile -> {
          // Get all the possible files for that profile
          Set<ORIGIN_TYPE> allFilesWithThisProfile = profileToOriginsMap.apply(profile);

          // Get all the possible values for this profile
          Set<VALUE_TYPE> allValuesForThisProfile = profileToOriginsMap.apply(profile)
              .flatMap(originToPossibleValuesMap::apply);

          return allValuesForThisProfile.find(value -> valueToOriginsMap.apply(value)
              .containsAll(allFilesWithThisProfile))
              .map(value -> Tuple.of(profile, value)); })
        .toMap(Function.identity()));

    return Either.right(bestProfileCombination.get());
  }

  private <ORIGIN_TYPE, VALUE_TYPE> Either<VALUE_TYPE, Map<String, VALUE_TYPE>> minimalConfig(
      Map<ORIGIN_TYPE, VALUE_TYPE> originToValueMap,
      List<List<String>> validProfileCombinations,
      Map<String, Set<ORIGIN_TYPE>> profileToOriginsMap,
      Set<ORIGIN_TYPE> allOrigins
  ) {
    Set<ORIGIN_TYPE> allValidOrigins = originToValueMap.keySet();
    // All profiles match this value, so return the single value
    Seq<? extends VALUE_TYPE> distinctValues = originToValueMap.values().distinct();
    if (distinctValues.size() == 1 && allOrigins.equals(allValidOrigins)) {
      return Either.left(distinctValues.head());
    }

    // Find the profile combination that has the fewest elements
    Option<Map<String, VALUE_TYPE>> bestProfileCombination = validProfileCombinations
        .find(profileCombination ->
          profileCombination.forAll(profile -> profileToOriginsMap
              .apply(profile)
              .flatMap(originToValueMap::get)
              .distinct()
              .size() <= 1) &&
              profileCombination.flatMap(profileToOriginsMap).toSet().equals(allValidOrigins)
            )
        .flatMap(profileCombination -> Option.of(profileCombination
            .flatMap(profile -> profileToOriginsMap
                .apply(profile)
                .map(originToValueMap)
                .distinct()
                .headOption()
                .map(value -> Tuple.of(profile, value)))))
        .map(tuples -> tuples.toMap(Function.identity()));

    return Either.right(bestProfileCombination.get());
  }

  private ConfigValue buildReferenceConfigValue(String path) {
    return ConfigReferenceHelper.buildReferenceConfigValue(path);
  }

  @Override
  public boolean execute() throws Exception {
    return executeInternal();
  }

  private Config loadConfigFromFile(File file) {
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

    return ConfigFactory.parseFile(file, options).resolve();
  }

  @Override
  public String description() {
    return null;
  }

  @Override
  public boolean getHelp() {
    return false;
  }
}
