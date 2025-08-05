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
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.scalar.JsonFunctions;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig.CollectionNotUnnestedToJson;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
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
 * TODO: support multi-dimensional array handling
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ComplexTypeTransformer implements RecordTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ComplexTypeTransformer.class);

  public static final String DEFAULT_DELIMITER = ".";
  public static final CollectionNotUnnestedToJson DEFAULT_COLLECTION_TO_JSON_MODE =
      CollectionNotUnnestedToJson.NON_PRIMITIVE;
  private final List<String> _fieldsToUnnest;
  private final String _delimiter;
  private final CollectionNotUnnestedToJson _collectionNotUnnestedToJson;
  private final Map<String, String> _prefixesToRename;
  private final boolean _continueOnError;

  private ComplexTypeTransformer(TableConfig tableConfig) {
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    assert ingestionConfig != null;
    ComplexTypeConfig complexTypeConfig = ingestionConfig.getComplexTypeConfig();
    assert complexTypeConfig != null;

    List<String> fieldsToUnnestFromConfig = complexTypeConfig.getFieldsToUnnest();
    if (fieldsToUnnestFromConfig != null) {
      _fieldsToUnnest = new ArrayList<>(fieldsToUnnestFromConfig);
      // NOTE: Sort the unnest fields to achieve topological sort of the collections, so that the parent collection
      // (e.g. foo) is unnested before the child collection (e.g. foo.bar).
      Collections.sort(_fieldsToUnnest);
    } else {
      _fieldsToUnnest = List.of();
    }

    _delimiter = Objects.requireNonNullElse(complexTypeConfig.getDelimiter(), DEFAULT_DELIMITER);
    _collectionNotUnnestedToJson =
        Objects.requireNonNullElse(complexTypeConfig.getCollectionNotUnnestedToJson(), DEFAULT_COLLECTION_TO_JSON_MODE);
    _prefixesToRename = Objects.requireNonNullElse(complexTypeConfig.getPrefixesToRename(), Map.of());
    _continueOnError = ingestionConfig.isContinueOnError();
  }

  /// Returns a [ComplexTypeTransformer] if it is defined in the table config, `null` otherwise.
  @Nullable
  public static ComplexTypeTransformer create(TableConfig tableConfig) {
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig != null && ingestionConfig.getComplexTypeConfig() != null) {
      return new ComplexTypeTransformer(tableConfig);
    }
    return null;
  }

  private ComplexTypeTransformer(List<String> fieldsToUnnest, String delimiter,
      CollectionNotUnnestedToJson collectionNotUnnestedToJson, Map<String, String> prefixesToRename,
      boolean continueOnError) {
    _fieldsToUnnest = fieldsToUnnest;
    _delimiter = delimiter;
    _collectionNotUnnestedToJson = collectionNotUnnestedToJson;
    _prefixesToRename = prefixesToRename;
    _continueOnError = continueOnError;
  }

  @VisibleForTesting
  static class Builder {
    private List<String> _fieldsToUnnest = List.of();
    private String _delimiter = DEFAULT_DELIMITER;
    private CollectionNotUnnestedToJson _collectionNotUnnestedToJson = DEFAULT_COLLECTION_TO_JSON_MODE;
    private Map<String, String> _prefixesToRename = Map.of();
    private boolean _continueOnError = false;

    public Builder setFieldsToUnnest(List<String> fieldsToUnnest) {
      _fieldsToUnnest = fieldsToUnnest;
      return this;
    }

    public Builder setDelimiter(String delimiter) {
      _delimiter = delimiter;
      return this;
    }

    public Builder setCollectionNotUnnestedToJson(CollectionNotUnnestedToJson collectionNotUnnestedToJson) {
      _collectionNotUnnestedToJson = collectionNotUnnestedToJson;
      return this;
    }

    public Builder setPrefixesToRename(Map<String, String> prefixesToRename) {
      _prefixesToRename = prefixesToRename;
      return this;
    }

    public Builder setContinueOnError(boolean continueOnError) {
      _continueOnError = continueOnError;
      return this;
    }

    public ComplexTypeTransformer build() {
      return new ComplexTypeTransformer(_fieldsToUnnest, _delimiter, _collectionNotUnnestedToJson, _prefixesToRename,
          _continueOnError);
    }
  }

  @Override
  public List<String> getInputColumns() {
    return _fieldsToUnnest;
  }

  @Override
  public List<GenericRow> transform(List<GenericRow> records) {
    List<GenericRow> transformedRecords = new ArrayList<>();
    for (GenericRow record : records) {
      try {
        ArrayList<String> columns = new ArrayList<>(record.getFieldToValueMap().keySet());
        if (_fieldsToUnnest.isEmpty()) {
          flattenMap(record, columns);
          transformedRecords.add(record);
        } else {
          Map<String, Object> originalValues = record.copy(_fieldsToUnnest).getFieldToValueMap();
          flattenMap(record, columns);
          List<GenericRow> unnestedRecords = List.of(record);
          for (String field : _fieldsToUnnest) {
            unnestedRecords = unnestCollection(unnestedRecords, field);
          }
          unnestedRecords.forEach(unnestedRecord -> {
            Map<String, Object> values = unnestedRecord.getFieldToValueMap();
            for (Map.Entry<String, Object> entry : originalValues.entrySet()) {
              values.putIfAbsent(entry.getKey(), entry.getValue());
            }
          });
          if (record.isIncomplete()) {
            unnestedRecords.forEach(GenericRow::markIncomplete);
          }
          transformedRecords.addAll(unnestedRecords);
        }
      } catch (Exception e) {
        if (!_continueOnError) {
          throw new RuntimeException("Caught exception while transforming complex types", e);
        }
        LOGGER.debug("Caught exception while transforming complex types for record: {}", record.toString(), e);
        record.markIncomplete();
      }
    }
    if (!_prefixesToRename.isEmpty()) {
      for (GenericRow record : transformedRecords) {
        try {
          renamePrefixes(record);
        } catch (Exception e) {
          if (!_continueOnError) {
            throw new RuntimeException("Caught exception while renaming prefixes", e);
          }
          LOGGER.debug("Caught exception while renaming prefixes for record: {}", record.toString(), e);
          record.markIncomplete();
        }
      }
    }
    return transformedRecords;
  }

  private List<GenericRow> unnestCollection(List<GenericRow> records, String column) {
    List<GenericRow> unnestedRecords = new ArrayList<>();
    for (GenericRow record : records) {
      unnestCollection(record, column, unnestedRecords);
    }
    return unnestedRecords;
  }

  private void unnestCollection(GenericRow record, String column, List<GenericRow> list) {
    Object value = record.getValue(column);
    if (value instanceof Collection) {
      if (((Collection) value).isEmpty()) {
        // use the record itself
        list.add(record);
      } else {
        // Remove the value before flattening since we are going to add the flattened items
        record.removeValue(column);
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
        // Remove the value before flattening since we are going to add the flattened items
        record.removeValue(column);
        for (Object obj : (Object[]) value) {
          GenericRow copy = flattenCollectionItem(record, obj, column);
          list.add(copy);
        }
      }
    } else {
      list.add(record);
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
        Map<String, Object> map = (Map) value;
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
  void renamePrefixes(GenericRow record) {
    assert !_prefixesToRename.isEmpty();
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
        Map<String, Object> innerMap = (Map<String, Object>) value;
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
          for (Object inner : collection) {
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
          for (Object inner : array) {
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
    return left + _delimiter + right;
  }
}
