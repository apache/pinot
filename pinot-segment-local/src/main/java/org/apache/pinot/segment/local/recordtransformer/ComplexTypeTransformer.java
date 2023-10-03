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
package org.apache.pinot.segment.local.recordtransformer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.scalar.JsonFunctions;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A transformer to handle the complex types such as Map and Collection, with flattening and unnesting.
 * <p>
 * The map flattening rule will recursively flatten all the map types, except for those under the collection that is
 * not marked as to unnest.
 *
 * For example:
 * <pre>
 * {
 *    "t1":{
 *       "array":[
 *          {
 *             "t2":{
 *                "a":"v1"
 *             }
 *          }
 *       ]
 *    }
 * }
 * </pre>
 *
 * flattens to
 * <pre>
 * {
 *    "t1.array":[
 *       {
 *          "t2.a":"v1"
 *       }
 *    ]
 * }
 * </pre>
 *
 * </p>
 * The unnesting rule will flatten all the collections provided, which are the paths navigating to the collections. For
 * the same example above. If the the collectionToUnnest is provided as "t1.array", then the rule will unnest the
 * previous output to:
 *
 * <pre>
 * [
 *    {
 *       "t1.arrayt2.a": "v1",
 *    }
 * ]
 * </pre>
 *
 *  Note the unnest rule will output a collection of generic rows under the field
 *  {@link GenericRow#MULTIPLE_RECORDS_KEY}.
 *  TODO: support multi-dimensional array handling
 *
 */
public class ComplexTypeTransformer implements RecordTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ComplexTypeTransformer.class);

  public static final String DEFAULT_DELIMITER = ".";
  public static final ComplexTypeConfig.CollectionNotUnnestedToJson DEFAULT_COLLECTION_TO_JSON_MODE =
      ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE;
  private final List<String> _fieldsToUnnest;
  private final String _delimiter;
  private final ComplexTypeConfig.CollectionNotUnnestedToJson _collectionNotUnnestedToJson;
  private final Map<String, String> _prefixesToRename;
  private final boolean _continueOnError;

  public ComplexTypeTransformer(TableConfig tableConfig) {
    this(parseFieldsToUnnest(tableConfig), parseDelimiter(tableConfig),
            parseCollectionNotUnnestedToJson(tableConfig), parsePrefixesToRename(tableConfig), tableConfig);
  }

  @VisibleForTesting
  ComplexTypeTransformer(List<String> fieldsToUnnest, String delimiter) {
    this(fieldsToUnnest, delimiter, DEFAULT_COLLECTION_TO_JSON_MODE, Collections.emptyMap(), null);
  }

  @VisibleForTesting
  ComplexTypeTransformer(List<String> fieldsToUnnest, String delimiter,
      ComplexTypeConfig.CollectionNotUnnestedToJson collectionNotUnnestedToJson, Map<String, String> prefixesToRename,
      TableConfig tableConfig) {
    _fieldsToUnnest = new ArrayList<>(fieldsToUnnest);
    _delimiter = delimiter;
    _collectionNotUnnestedToJson = collectionNotUnnestedToJson;
    // the unnest fields are sorted to achieve the topological sort of the collections, so that the parent collection
    // (e.g. foo) is unnested before the child collection (e.g. foo.bar)
    Collections.sort(_fieldsToUnnest);
    _prefixesToRename = prefixesToRename;
    _continueOnError =
        tableConfig != null && tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig()
            .isContinueOnError();
  }

  private static List<String> parseFieldsToUnnest(TableConfig tableConfig) {
    if (tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getComplexTypeConfig() != null
        && tableConfig.getIngestionConfig().getComplexTypeConfig().getFieldsToUnnest() != null) {
      return tableConfig.getIngestionConfig().getComplexTypeConfig().getFieldsToUnnest();
    } else {
      return new ArrayList<>();
    }
  }

  private static String parseDelimiter(TableConfig tableConfig) {
    if (tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getComplexTypeConfig() != null
        && tableConfig.getIngestionConfig().getComplexTypeConfig().getDelimiter() != null) {
      return tableConfig.getIngestionConfig().getComplexTypeConfig().getDelimiter();
    } else {
      return DEFAULT_DELIMITER;
    }
  }

  /**
   * @return the complex type transformer defined table config, null if the table config does not have the config
   */
  @Nullable
  public static ComplexTypeTransformer getComplexTypeTransformer(TableConfig tableConfig) {
    if (tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getComplexTypeConfig() != null) {
      return new ComplexTypeTransformer(tableConfig);
    }
    return null;
  }

  private static ComplexTypeConfig.CollectionNotUnnestedToJson parseCollectionNotUnnestedToJson(
      TableConfig tableConfig) {
    if (tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getComplexTypeConfig() != null
        && tableConfig.getIngestionConfig().getComplexTypeConfig().getCollectionNotUnnestedToJson() != null) {
      return tableConfig.getIngestionConfig().getComplexTypeConfig().getCollectionNotUnnestedToJson();
    } else {
      return DEFAULT_COLLECTION_TO_JSON_MODE;
    }
  }

  private static Map<String, String> parsePrefixesToRename(TableConfig tableConfig) {
    if (tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getComplexTypeConfig() != null
            && tableConfig.getIngestionConfig().getComplexTypeConfig().getPrefixesToRename() != null) {
      return tableConfig.getIngestionConfig().getComplexTypeConfig().getPrefixesToRename();
    } else {
      return Collections.emptyMap();
    }
  }

  @Override
  public GenericRow transform(GenericRow record) {
    try {
      flattenMap(record, new ArrayList<>(record.getFieldToValueMap().keySet()));
      for (String collection : _fieldsToUnnest) {
        unnestCollection(record, collection);
      }
      renamePrefixes(record);
    } catch (Exception e) {
      if (!_continueOnError) {
        throw new RuntimeException("Caught exception while transforming complex types", e);
      } else {
        LOGGER.debug("Caught exception while transforming complex types for record: {}", record.toString(), e);
        record.putValue(GenericRow.INCOMPLETE_RECORD_KEY, true);
      }
    }
    return record;
  }

  private GenericRow unnestCollection(GenericRow record, String column) {
    Object value = record.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    if (value == null) {
      List<GenericRow> list = new ArrayList<>();
      unnestCollection(record, column, list);
      record.putValue(GenericRow.MULTIPLE_RECORDS_KEY, list);
    } else {
      Collection<GenericRow> records = (Collection) value;
      List<GenericRow> list = new ArrayList<>();
      for (GenericRow innerRecord : records) {
        unnestCollection(innerRecord, column, list);
      }
      record.putValue(GenericRow.MULTIPLE_RECORDS_KEY, list);
    }
    return record;
  }

  private void unnestCollection(GenericRow record, String column, List<GenericRow> list) {
    Object value = record.removeValue(column);
    if (value == null) {
      // use the record itself
      list.add(record);
    } else if (value instanceof Collection) {
      if (((Collection) value).isEmpty()) {
        // use the record itself
        list.add(record);
      } else {
        for (Object obj : (Collection) value) {
          GenericRow copy = flattenCollectionItem(record, obj, column);
          list.add(copy);
        }
      }
    } else if (isNonPrimitiveArray(value)) {
      if (((Object[]) value).length == 0) {
        // use the record itself
        list.add(record);
      } else {
        for (Object obj : (Object[]) value) {
          GenericRow copy = flattenCollectionItem(record, obj, column);
          list.add(copy);
        }
      }
    }
  }

  private GenericRow flattenCollectionItem(GenericRow record, Object obj, String column) {
    GenericRow copy = record.copy();
    if (obj instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) obj;
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        String flattenName = concat(column, entry.getKey());
        copy.putValue(flattenName, entry.getValue());
      }
    } else {
      copy.putValue(column, obj);
    }
    return copy;
  }

  /**
   * Recursively flatten all the Maps in the record. It will also navigate into the collections marked as "unnest" and
   * flatten the nested maps.
   */
  @VisibleForTesting
  protected void flattenMap(GenericRow record, List<String> columns) {
    for (String column : columns) {
      Object value = record.getValue(column);
      if (value instanceof Map) {
        Map<String, Object> map = (Map) record.removeValue(column);
        List<String> mapColumns = new ArrayList<>();
        for (Map.Entry<String, Object> entry : new ArrayList<>(map.entrySet())) {
          String flattenName = concat(column, entry.getKey());
          Object nestedValue = entry.getValue();
          record.putValue(flattenName, nestedValue);
          if (nestedValue instanceof Map || nestedValue instanceof Collection || isNonPrimitiveArray(nestedValue)) {
            mapColumns.add(flattenName);
          }
        }
        flattenMap(record, mapColumns);
      } else if (value instanceof Collection) {
        Collection collection = (Collection) value;
        if (_fieldsToUnnest.contains(column)) {
          for (Object inner : collection) {
            if (inner instanceof Map) {
              Map<String, Object> innerMap = (Map<String, Object>) inner;
              flattenMap(column, innerMap, new ArrayList<>(innerMap.keySet()));
            }
          }
        } else if (shallConvertToJson(collection)) {
          try {
            // convert the collection to JSON string
            String jsonString = JsonFunctions.jsonFormat(collection);
            record.putValue(column, jsonString);
          } catch (JsonProcessingException e) {
            throw new RuntimeException(
                String.format("Caught exception while converting value to JSON string %s", value), e);
          }
        }
      } else if (isNonPrimitiveArray(value)) {
        Object[] array = (Object[]) value;
        if (_fieldsToUnnest.contains(column)) {
          for (Object inner : array) {
            if (inner instanceof Map) {
              Map<String, Object> innerMap = (Map<String, Object>) inner;
              flattenMap(column, innerMap, new ArrayList<>(innerMap.keySet()));
            }
          }
        } else if (shallConvertToJson(array)) {
          try {
            // convert the array to JSON string
            String jsonString = JsonFunctions.jsonFormat(array);
            record.putValue(column, jsonString);
          } catch (JsonProcessingException e) {
            throw new RuntimeException(
                String.format("Caught exception while converting value to JSON string %s", value), e);
          }
        }
      }
    }
  }

  /**
   * Loops through all columns and renames the column's prefix with the corresponding replacement if the prefix matches.
   */
  @VisibleForTesting
  protected void renamePrefixes(GenericRow record) {
    if (_prefixesToRename.isEmpty()) {
      return;
    }
    List<String> fields = new ArrayList<>(record.getFieldToValueMap().keySet());
    for (Map.Entry<String, String> entry : _prefixesToRename.entrySet()) {
      for (String field : fields) {
        String prefix = entry.getKey();
        String replacementPrefix = entry.getValue();
        if (field.startsWith(prefix)) {
          Object value = record.removeValue(field);
          String remainingColumnName = field.substring(prefix.length());
          String newName = replacementPrefix + remainingColumnName;
          if (newName.isEmpty() || record.getValue(newName) != null) {
            throw new RuntimeException(
                    String.format("Name conflict after attempting to rename field %s to %s", field, newName));
          }
          record.putValue(newName, value);
        }
      }
    }
  }

  private boolean containPrimitives(Object[] value) {
    if (value.length == 0) {
      return true;
    }
    Object element = value[0];
    return !(element instanceof Map || element instanceof Collection || isNonPrimitiveArray(element));
  }

  /**
   * This function assumes the collection is a homogeneous data structure that elements have same data type.
   * So it checks the first element only.
   */
  private boolean containPrimitives(Collection value) {
    if (value.isEmpty()) {
      return true;
    }
    Object element = value.iterator().next();
    return !(element instanceof Map || element instanceof Collection || isNonPrimitiveArray(element));
  }

  protected static boolean isNonPrimitiveArray(Object obj) {
    return obj instanceof Object[];
  }

  private void flattenMap(String path, Map<String, Object> map, Collection<String> fields) {
    for (String field : fields) {
      Object value = map.get(field);
      String concatName = concat(path, field);
      if (value instanceof Map) {
        Map<String, Object> innerMap = (Map<String, Object>) map.remove(field);
        List<String> innerMapFields = new ArrayList<>();
        for (Map.Entry<String, Object> innerEntry : new ArrayList<>(innerMap.entrySet())) {
          Object innerValue = innerEntry.getValue();
          String innerConcatName = concat(field, innerEntry.getKey());
          map.put(innerConcatName, innerEntry.getValue());
          if (innerValue instanceof Map || innerValue instanceof Collection || isNonPrimitiveArray(innerValue)) {
            innerMapFields.add(innerConcatName);
          }
        }
        if (!innerMapFields.isEmpty()) {
          flattenMap(path, map, innerMapFields);
        }
      } else if (value instanceof Collection && _fieldsToUnnest.contains(concatName)) {
        Collection collection = (Collection) value;
        if (_fieldsToUnnest.contains(concatName)) {
          for (Object inner : (Collection) value) {
            if (inner instanceof Map) {
              Map<String, Object> innerMap = (Map<String, Object>) inner;
              flattenMap(concatName, innerMap, new ArrayList<>(innerMap.keySet()));
            }
          }
        } else if (shallConvertToJson(collection)) {
          try {
            // convert the collection to JSON string
            String jsonString = JsonFunctions.jsonFormat(collection);
            map.put(field, jsonString);
          } catch (JsonProcessingException e) {
            throw new RuntimeException(
                String.format("Caught exception while converting value to JSON string %s", value), e);
          }
        }
      } else if (isNonPrimitiveArray(value)) {
        Object[] array = (Object[]) value;
        if (_fieldsToUnnest.contains(concatName)) {
          for (Object inner : (Object[]) value) {
            if (inner instanceof Map) {
              Map<String, Object> innerMap = (Map<String, Object>) inner;
              flattenMap(concatName, innerMap, new ArrayList<>(innerMap.keySet()));
            }
          }
        } else if (shallConvertToJson(array)) {
          try {
            // convert the array to JSON string
            String jsonString = JsonFunctions.jsonFormat(array);
            map.put(field, jsonString);
          } catch (JsonProcessingException e) {
            throw new RuntimeException(
                String.format("Caught exception while converting value to JSON string %s", value), e);
          }
        }
      }
    }
  }

  private boolean shallConvertToJson(Object[] value) {
    switch (_collectionNotUnnestedToJson) {
      case ALL:
        return true;
      case NONE:
        return false;
      case NON_PRIMITIVE:
        return !containPrimitives(value);
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported collectionNotUnnestedToJson %s", _collectionNotUnnestedToJson));
    }
  }

  private boolean shallConvertToJson(Collection value) {
    switch (_collectionNotUnnestedToJson) {
      case ALL:
        return true;
      case NONE:
        return false;
      case NON_PRIMITIVE:
        return !containPrimitives(value);
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported collectionNotUnnestedToJson %s", _collectionNotUnnestedToJson));
    }
  }

  private String concat(String left, String right) {
    return String.join(_delimiter, left, right);
  }

  public String describe() {
    return "Complex Type Transformer";
  }
}
