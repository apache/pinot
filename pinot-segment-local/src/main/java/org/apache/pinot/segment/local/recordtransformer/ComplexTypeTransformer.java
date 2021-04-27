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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeHandlingConfig;
import org.apache.pinot.spi.data.readers.GenericRow;


public class ComplexTypeTransformer implements RecordTransformer {
  private static final CharSequence DELIMITER = ".";
  private final List<String> _collectionsToUnnest;

  public ComplexTypeTransformer(TableConfig tableConfig) {
    if (tableConfig.getIngestionConfig() != null
        && tableConfig.getIngestionConfig().getComplexTypeHandlingConfig() != null) {
      _collectionsToUnnest =
          tableConfig.getIngestionConfig().getComplexTypeHandlingConfig().getUnnestConfig() != null ? tableConfig
              .getIngestionConfig().getComplexTypeHandlingConfig().getUnnestConfig() : new ArrayList<>();
    } else {
      _collectionsToUnnest = new ArrayList<>();
    }
  }

  @VisibleForTesting
  public ComplexTypeTransformer(List<String> unnestCollections) {
    _collectionsToUnnest = new ArrayList<>(unnestCollections);
    Collections.sort(_collectionsToUnnest);
  }

  public static boolean isComplexTypeHandlingEnabled(TableConfig tableConfig) {
    if (tableConfig.getIngestionConfig() == null
        || tableConfig.getIngestionConfig().getComplexTypeHandlingConfig() == null
        || tableConfig.getIngestionConfig().getComplexTypeHandlingConfig().getMode() == null) {
      return false;
    }
    return tableConfig.getIngestionConfig().getComplexTypeHandlingConfig().getMode()
        != ComplexTypeHandlingConfig.Mode.NONE;
  }

  @Nullable
  @Override
  public GenericRow transform(GenericRow record) {
    flattenMap(record, new HashSet<>(record.getFieldToValueMap().keySet()));
    for (String collection : _collectionsToUnnest) {
      unnestCollection(record, collection);
    }
    return record;
  }

  private GenericRow unnestCollection(GenericRow record, String column) {
    if (record.getValue(GenericRow.MULTIPLE_RECORDS_KEY) == null) {
      List<GenericRow> list = new ArrayList<>();
      unnestCollection(record, column, list);
      record.putValue(GenericRow.MULTIPLE_RECORDS_KEY, list);
    } else {
      Collection<GenericRow> records = (Collection) record.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
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
      return;
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
    } else if (value.getClass().isArray()) {
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
      for (Map.Entry<String, Object> entry : new HashSet<>(map.entrySet())) {
        String flattenName = concat(column, entry.getKey());
        copy.putValue(flattenName, entry.getValue());
      }
    } else {
      copy.putValue(column, obj);
    }
    return copy;
  }

  private GenericRow flattenMap(GenericRow record, Collection<String> columns) {
    for (String column : columns) {
      if (record.getValue(column) instanceof Map) {
        Map<String, Object> map = (Map) record.removeValue(column);
        List<String> mapColumns = new ArrayList<>();
        for (Map.Entry<String, Object> entry : new HashSet<>(map.entrySet())) {
          String flattenName = concat(column, entry.getKey());
          record.putValue(flattenName, entry.getValue());
          mapColumns.add(flattenName);
        }
        record = flattenMap(record, mapColumns);
      } else if (record.getValue(column) instanceof Collection && _collectionsToUnnest.contains(column)) {
        for (Object inner : (Collection) record.getValue(column)) {
          if (inner instanceof Map) {
            Map<String, Object> innerMap = (Map<String, Object>) inner;
            flattenMap(column, innerMap, new HashSet<>(innerMap.keySet()));
          }
        }
      }
    }
    return record;
  }

  private void flattenMap(String path, Map<String, Object> map, Collection<String> fields) {
    for (String field : fields) {
      if (map.get(field) instanceof Map) {
        Map<String, Object> innerMap = (Map<String, Object>) map.remove(field);
        List<String> innerMapFields = new ArrayList<>();
        for (Map.Entry<String, Object> innerEntry : new HashSet<>(innerMap.entrySet())) {
          map.put(concat(field, innerEntry.getKey()), innerEntry.getValue());
          innerMapFields.add(concat(field, innerEntry.getKey()));
        }
        if (!innerMapFields.isEmpty()) {
          flattenMap(concat(path, field), map, innerMapFields);
        }
      } else if (map.get(field) instanceof Collection && _collectionsToUnnest.contains(concat(path, field))) {
        for (Object inner : (Collection) map.get(field)) {
          if (inner instanceof Map) {
            Map<String, Object> innerMap = (Map<String, Object>) inner;
            flattenMap(concat(path, field), innerMap, new HashSet<>(innerMap.keySet()));
          }
        }
      }
    }
  }

  private String concat(String left, String right) {
    return String.join(DELIMITER, left, right);
  }
}
