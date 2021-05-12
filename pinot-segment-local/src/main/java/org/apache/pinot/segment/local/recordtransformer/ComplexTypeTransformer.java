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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * A transformer to handle the complex types such as Map and Collection, with flattening and unnesting.
 * <p>
 * The map flattening rule will recursively flatten all the map types, except for those under the collection that is not marked as to unnest.
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
 * <pre/>
 *
 * <p>
 *
 * The unnesting rule will flatten all the collections provided, which are the paths navigating to the collections. For
 * the same example above. If the the collectionToUnnest is provided as "t1.array", then the rule will unnest the
 * previous output to:
 *
 * <pre>
 *  [{
 *     "t1.arrayt2.a": "v1",
 *  }]
 *  * <pre/>
 *
 *  Note the unnest rule will output a collection of generic rows under the field {@link GenericRow#MULTIPLE_RECORDS_KEY}.
 *  TODO: support multi-dimensional array handling
 *
 */
public class ComplexTypeTransformer implements RecordTransformer {
  // TODO: make configurable
  private static final CharSequence DELIMITER = ".";
  private final List<String> _unnestFields;

  public ComplexTypeTransformer(TableConfig tableConfig) {
    if (tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getComplexTypeConfig() != null) {
      _unnestFields = tableConfig.getIngestionConfig().getComplexTypeConfig().getUnnestFields() != null ? tableConfig
          .getIngestionConfig().getComplexTypeConfig().getUnnestFields() : new ArrayList<>();
      // the unnest fields are sorted to achieve the topological sort of the collections, so that the parent collection
      // (e.g. foo) is unnested before the child collection (e.g. foo.bar)
      Collections.sort(_unnestFields);
    } else {
      _unnestFields = new ArrayList<>();
    }
  }

  @VisibleForTesting
  public ComplexTypeTransformer(List<String> unnestFields) {
    _unnestFields = new ArrayList<>(unnestFields);
    Collections.sort(_unnestFields);
  }

  public static boolean isComplexTypeHandlingEnabled(TableConfig tableConfig) {
    return tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getComplexTypeConfig() != null;
  }

  @Nullable
  @Override
  public GenericRow transform(GenericRow record) {
    flattenMap(record, new ArrayList<>(record.getFieldToValueMap().keySet()));
    for (String collection : _unnestFields) {
      unnestCollection(record, collection);
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
    } else if (isArray(value)) {
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
          if (nestedValue instanceof Map || nestedValue instanceof Collection || isArray(nestedValue)) {
            mapColumns.add(flattenName);
          }
        }
        flattenMap(record, mapColumns);
      } else if (value instanceof Collection && _unnestFields.contains(column)) {
        for (Object inner : (Collection) value) {
          if (inner instanceof Map) {
            Map<String, Object> innerMap = (Map<String, Object>) inner;
            flattenMap(column, innerMap, new ArrayList<>(innerMap.keySet()));
          }
        }
      } else if (isArray(value) && _unnestFields.contains(column)) {
        for (Object inner : (Object[]) value) {
          if (inner instanceof Map) {
            Map<String, Object> innerMap = (Map<String, Object>) inner;
            flattenMap(column, innerMap, new ArrayList<>(innerMap.keySet()));
          }
        }
      }
    }
  }

  static private boolean isArray(Object obj) {
    if (obj == null) {
      return false;
    }
    return obj.getClass().isArray();
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
          String innerCancatName = concat(field, innerEntry.getKey());
          map.put(innerCancatName, innerEntry.getValue());
          if (innerValue instanceof Map || innerValue instanceof Collection || isArray(innerValue)) {
            innerMapFields.add(innerCancatName);
          }
        }
        if (!innerMapFields.isEmpty()) {
          flattenMap(concatName, map, innerMapFields);
        }
      } else if (value instanceof Collection && _unnestFields.contains(concatName)) {
        for (Object inner : (Collection) value) {
          if (inner instanceof Map) {
            Map<String, Object> innerMap = (Map<String, Object>) inner;
            flattenMap(concatName, innerMap, new ArrayList<>(innerMap.keySet()));
          }
        }
      } else if (isArray(value) && _unnestFields.contains(concatName)) {
        for (Object inner : (Object[]) value) {
          if (inner instanceof Map) {
            Map<String, Object> innerMap = (Map<String, Object>) inner;
            flattenMap(concatName, innerMap, new ArrayList<>(innerMap.keySet()));
          }
        }
      }
    }
  }

  private static String concat(String left, String right) {
    return String.join(DELIMITER, left, right);
  }
}
