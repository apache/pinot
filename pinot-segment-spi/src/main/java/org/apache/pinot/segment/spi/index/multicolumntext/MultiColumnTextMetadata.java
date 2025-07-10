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
package org.apache.pinot.segment.spi.index.multicolumntext;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration2.Configuration;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextIndexConstants.MetadataKey;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Multi-column metadata as read from metadata.properties file.
 * Note: only settings that multi-column text index uses are parsed & stored, ignoring everything else.
 * That is to prevent storing useless data in properties file and potentially triggering unnecessary index rebuild.
 */
public class MultiColumnTextMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiColumnTextMetadata.class);
  public static final int VERSION_1 = 1;

  private static final Set<String> SHARED_PROPERTIES = Set.of(
      FieldConfig.TEXT_FST_TYPE, // fst type should only be 'default' (never 'native'!)
      FieldConfig.TEXT_INDEX_ENABLE_QUERY_CACHE,
      FieldConfig.TEXT_INDEX_USE_AND_FOR_MULTI_TERM_QUERIES,
      FieldConfig.TEXT_INDEX_STOP_WORD_INCLUDE_KEY,
      FieldConfig.TEXT_INDEX_STOP_WORD_EXCLUDE_KEY,
      FieldConfig.TEXT_INDEX_LUCENE_USE_COMPOUND_FILE,
      FieldConfig.TEXT_INDEX_LUCENE_MAX_BUFFER_SIZE_MB,
      FieldConfig.TEXT_INDEX_CASE_SENSITIVE_KEY,
      FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS,
      FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARGS,
      FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARG_TYPES,
      FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS,
      FieldConfig.TEXT_INDEX_ENABLE_PREFIX_SUFFIX_PHRASE_QUERIES,
      FieldConfig.TEXT_INDEX_LUCENE_REUSE_MUTABLE_INDEX,
      FieldConfig.TEXT_INDEX_LUCENE_NRT_CACHING_DIRECTORY_BUFFER_SIZE,
      FieldConfig.TEXT_INDEX_LUCENE_USE_LBS_MERGE_POLICY,
      FieldConfig.TEXT_INDEX_LUCENE_DOC_ID_TRANSLATOR_MODE
      // The following aren't handled at the moment.
      //FieldConfig.TEXT_INDEX_NO_RAW_DATA
      //FieldConfig.TEXT_INDEX_RAW_VALUE
  );

  // keys allowed in per-column properties
  private static final Set<String> PER_COLUMN_PROPERTIES = Set.of(
      FieldConfig.TEXT_INDEX_USE_AND_FOR_MULTI_TERM_QUERIES,
      FieldConfig.TEXT_INDEX_ENABLE_PREFIX_SUFFIX_PHRASE_QUERIES,
      FieldConfig.TEXT_INDEX_STOP_WORD_INCLUDE_KEY,
      FieldConfig.TEXT_INDEX_STOP_WORD_EXCLUDE_KEY,
      FieldConfig.TEXT_INDEX_CASE_SENSITIVE_KEY,
      FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS,
      FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARGS,
      FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARG_TYPES,
      FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS);

  private final int _version;
  private final List<String> _columns;
  private final Map<String, String> _sharedProperties;
  private final Map<String, Map<String, String>> _perColumnProperties;

  public MultiColumnTextMetadata(int version,
      List<String> columns,
      Map<String, String> sharedProperties,
      Map<String, Map<String, String>> perColumnProperties) {
    _version = version;
    _columns = columns;
    _sharedProperties = sharedProperties != null ? sharedProperties : Collections.emptyMap();
    _perColumnProperties = perColumnProperties != null ? perColumnProperties : Collections.emptyMap();
  }

  public MultiColumnTextMetadata(Configuration config) {
    _version = config.getInt(MetadataKey.INDEX_VERSION);
    _columns = Arrays.asList(config.getStringArray(MetadataKey.COLUMNS));

    Configuration sharedProps = config.subset(MetadataKey.PROPERTY_SUBSET);
    if (sharedProps != null && !sharedProps.isEmpty()) {
      _sharedProperties = new HashMap<>();

      Iterator<String> keysIterator = sharedProps.getKeys();
      while (keysIterator.hasNext()) {
        String key = keysIterator.next();
        if (SHARED_PROPERTIES.contains(key)) {
          if (key.equals(FieldConfig.TEXT_INDEX_STOP_WORD_INCLUDE_KEY)
              || key.equals(FieldConfig.TEXT_INDEX_STOP_WORD_EXCLUDE_KEY)) {
            setArrayProperty(sharedProps, key);
          } else {
            setProperty(sharedProps, key);
          }
        } else {
          LOGGER.debug("Ignoring unknown shared setting from multi-column text index configuration: " + key);
        }
      }
    } else {
      _sharedProperties = Collections.emptyMap();
    }

    Map<String, Map<String, String>> perColumnProps = null;
    Configuration columnConfigs = config.subset(MetadataKey.COLUMN);
    Iterator<String> keysIterator = columnConfigs.getKeys();
    while (keysIterator.hasNext()) {
      String key = keysIterator.next();
      int idx = key.indexOf('.' + MetadataKey.PROPERTY + ".");
      if (idx < 0) {
        continue;
      }
      String column = key.substring(0, idx);
      if (!_columns.contains(column)) {
        LOGGER.debug(
            "Ignoring unknown column setting from multi-column text index configuration, column: " + column + " key: "
                + key);
        continue;
      }
      String property = key.substring(idx + ("." + MetadataKey.PROPERTY + ".").length());
      if (!PER_COLUMN_PROPERTIES.contains(property)) {
        LOGGER.debug(
            "Ignoring unknown setting from multi-column text index configuration, column: " + column + " key: "
                + property);
        continue;
      }

      if (perColumnProps == null) {
        perColumnProps = new HashMap<>();
      }

      String value = columnConfigs.getString(key);

      if (value == null) {
        continue;
      }

      Map<String, String> targetMap = perColumnProps.get(column);
      if (targetMap == null) {
        targetMap = new HashMap<>();
        perColumnProps.put(column, targetMap);
      }
      targetMap.put(property, value);
    }

    if (perColumnProps == null) {
      perColumnProps = Collections.emptyMap();
    }
    _perColumnProperties = perColumnProps;
  }

  private void setProperty(Configuration from, String key) {
    if (from.containsKey(key)) {
      _sharedProperties.put(key, from.getString(key));
    }
  }

  private void setArrayProperty(Configuration from, String key) {
    if (from.containsKey(key)) {
      String[] words = from.getStringArray(key);
      String value;
      if (words == null || words.length == 0) {
        value = null;
      } else {
        value = String.join(FieldConfig.TEXT_INDEX_STOP_WORD_SEPERATOR, words);
      }
      _sharedProperties.put(key, value);
    }
  }

  public static void writeMetadata(Configuration target,
      int version,
      List<String> columns,
      Map<String, String> textProps,
      Map<String, Map<String, String>> perColumnProperties) {
    target.setProperty(MetadataKey.ROOT_PREFIX + MetadataKey.INDEX_VERSION, version);
    target.setProperty(MetadataKey.ROOT_PREFIX + MetadataKey.COLUMNS, columns);

    if (textProps != null) {
      setProperty(target, FieldConfig.TEXT_INDEX_ENABLE_QUERY_CACHE, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_USE_AND_FOR_MULTI_TERM_QUERIES, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_STOP_WORD_INCLUDE_KEY, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_STOP_WORD_EXCLUDE_KEY, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_LUCENE_USE_COMPOUND_FILE, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_LUCENE_MAX_BUFFER_SIZE_MB, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARGS, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARG_TYPES, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_ENABLE_PREFIX_SUFFIX_PHRASE_QUERIES, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_LUCENE_REUSE_MUTABLE_INDEX, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_LUCENE_NRT_CACHING_DIRECTORY_BUFFER_SIZE, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_LUCENE_USE_LBS_MERGE_POLICY, textProps);
      setProperty(target, FieldConfig.TEXT_INDEX_LUCENE_DOC_ID_TRANSLATOR_MODE, textProps);
    }

    if (perColumnProperties != null) {
      for (Map.Entry<String, Map<String, String>> entry : perColumnProperties.entrySet()) {
        String column = entry.getKey();
        if (column == null || column.isEmpty() || !columns.contains(column)) {
          continue;
        }

        Map<String, String> props = entry.getValue();
        for (String property : PER_COLUMN_PROPERTIES) {
          setProperty(target, column, property, props);
        }
      }
    }
  }

  // compares SHARED_PROPERTIES only, ignores everything else
  public static boolean equalsSharedProps(Map<String, String> props1, Map<String, String> props2) {
    return filterSharedProps(props1).equals(filterSharedProps(props2));
  }

  private static Map<String, String> filterSharedProps(Map<String, String> sharedProps) {
    if (sharedProps == null) {
      return Collections.emptyMap();
    }
    if (sharedProps.isEmpty()) {
      return sharedProps;
    }

    Map<String, String> filteredProps = new HashMap<>(sharedProps.size());
    for (Map.Entry<String, String> columnEntry : sharedProps.entrySet()) {
      String sharedKey = columnEntry.getKey();
      String value = columnEntry.getValue();
      if (value == null || !SHARED_PROPERTIES.contains(sharedKey)) {
        continue;
      }

      filteredProps.put(sharedKey, value);
    }

    return filteredProps;
  }

  // compares PER_COLUMN_PROPERTIES only, ignores everything else
  public static boolean equalsColumnProps(Map<String, Map<String, String>> props1,
      Map<String, Map<String, String>> props2) {
    return filterColumnProps(props1).equals(filterColumnProps(props2));
  }

  private static Map<String, Map<String, String>> filterColumnProps(Map<String, Map<String, String>> allColumnProps) {
    if (allColumnProps == null || allColumnProps.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, Map<String, String>> filteredAllProps = new HashMap<>(allColumnProps.size());
    for (Map.Entry<String, Map<String, String>> columnEntry : allColumnProps.entrySet()) {
      String column = columnEntry.getKey();
      Map<String, String> columnProps = columnEntry.getValue();
      if (columnProps == null || columnProps.isEmpty()) {
        continue;
      }

      HashMap<String, String> filteredColProps = null;

      for (String key : columnProps.keySet()) {
        if (PER_COLUMN_PROPERTIES.contains(key)) {
          if (filteredColProps == null) {
            filteredColProps = new HashMap<>();
          }

          filteredColProps.put(key, columnProps.get(key));
        }
      }

      if (filteredColProps != null) {
        filteredAllProps.put(column, filteredColProps);
      }
    }

    return filteredAllProps;
  }

  private static void setProperty(Configuration target, String column, String key, Map<String, String> source) {
    String value = source.get(key);
    if (value != null) {
      target.setProperty(MetadataKey.COLUMN_PREFIX + column + "." + MetadataKey.PROPERTY_SUFFIX + key, value);
    }
  }

  private static void setProperty(Configuration target, String key, Map<String, String> source) {
    String value = source.get(key);
    if (value != null) {
      target.setProperty(MetadataKey.PROPERTY_PREFIX + key, value);
    }
  }

  public Map<String, String> getSharedProperties() {
    return _sharedProperties;
  }

  public Map<String, Map<String, String>> getPerColumnProperties() {
    return _perColumnProperties;
  }

  public List<String> getColumns() {
    return _columns;
  }

  public static boolean isValidSharedProperty(String key) {
    return SHARED_PROPERTIES.contains(key);
  }

  public static boolean isValidPerColumnProperty(String key) {
    return PER_COLUMN_PROPERTIES.contains(key);
  }
}
