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
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.JsonLogTransformerConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamDataDecoderImpl;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This transformer transforms a record representing a JSON log event such that it can be stored in a table. JSON log
 * events typically have a user-defined schema, so it is impractical to store each field in its own table column. At the
 * same time, most (if not all) fields are important to the user, so we should not drop any field unnecessarily. Thus,
 * this transformer primarily takes record-fields that don't exist in the schema and stores them in a type of catchall
 * field.
 * <p>
 * For example, consider this log event:
 * <pre>
 * {
 *   "timestamp": 1687786535928,
 *   "hostname": "host1",
 *   "level": "INFO",
 *   "message": "Started processing job1",
 *   "tags": {
 *     "platform": "data",
 *     "service": "serializer",
 *     "params": {
 *       "queueLength": 5,
 *       "timeout": 299,
 *       "userData_noIndex": {
 *         "nth": 99
 *       }
 *     }
 *   }
 * }
 * </pre>
 * And let's say the table's schema contains these fields:
 * <ul>
 *   <li>timestamp</li>
 *   <li>hostname</li>
 *   <li>level</li>
 *   <li>message</li>
 *   <li>tags.platform</li>
 *   <li>tags.service</li>
 *   <li>indexableExtras</li>
 *   <li>unindexableExtras</li>
 * </ul>
 * <p>
 * Without this transformer, the entire "tags" field would be dropped when storing the record in the table. However,
 * with this transformer, the record would be transformed into the following:
 * <pre>
 * {
 *   "timestamp": 1687786535928,
 *   "hostname": "host1",
 *   "level": "INFO",
 *   "message": "Started processing job1",
 *   "tags.platform": "data",
 *   "tags.service": "serializer",
 *   "indexableExtras": {
 *     "tags": {
 *       "params": {
 *         "queueLength": 5,
 *         "timeout": 299
 *       }
 *     }
 *   },
 *   "unindexableExtras": {
 *     "tags": {
 *       "userData_noIndex": {
 *         "nth": 99
 *       }
 *     }
 *   }
 * }
 * </pre>
 * Notice that the transformer:
 * <ul>
 *   <li>Flattens nested fields which exist in the schema, like "tags.platform"</li>
 *   <li>Moves fields which don't exist in the schema into the "indexableExtras" field</li>
 *   <li>Moves fields which don't exist in the schema and have the suffix "_noIndex" into the "unindexableExtras"
 *   field</li>
 * </ul>
 * <p>
 * The "unindexableExtras" field allows the transformer to separate fields which don't need indexing (because they are
 * only retrieved, not searched) from those that do. The transformer also has other configuration options specified in
 * {@link JsonLogTransformerConfig}.
 * <p>
 * One notable complication that this class handles is adding nested fields to the "extras" fields. E.g., consider
 * this record
 * <pre>
 * {
 *   a: {
 *     b: {
 *       c: 0,
 *       d: 1
 *     }
 *   }
 * }
 * </pre>
 * Assume "$.a.b.c" exists in the schema but "$.a.b.d" doesn't. This class processes the record recursively from the
 * root node to the children, so it would only know that "$.a.b.d" doesn't exist when it gets to "d". At this point we
 * need to add "d" and all of its parents to the indexableExtrasField. To do so efficiently, the class builds this
 * branch starting from the leaf and attaches it to parent nodes as we return from each recursive call.
 */
public class JsonLogTransformer implements RecordTransformer {
  private static final Logger _logger = LoggerFactory.getLogger(JsonLogTransformer.class);

  private final boolean _continueOnError;
  private final JsonLogTransformerConfig _transformerConfig;
  private final DataType _indexableExtrasFieldType;
  private final DataType _unindexableExtrasFieldType;

  private Map<String, Object> _schemaTree;

  /**
   * Validates the schema against the given transformer's configuration.
   */
  public static void validateSchema(@Nonnull Schema schema, @Nonnull JsonLogTransformerConfig transformerConfig) {
    validateSchemaFieldNames(schema.getPhysicalColumnNames(), transformerConfig);

    String indexableExtrasFieldName = transformerConfig.getIndexableExtrasField();
    getAndValidateExtrasFieldType(schema, indexableExtrasFieldName);
    String unindexableExtrasFieldName = transformerConfig.getUnindexableExtrasField();
    if (null != unindexableExtrasFieldName) {
      getAndValidateExtrasFieldType(schema, indexableExtrasFieldName);
    }

    validateSchemaAndCreateTree(schema);
  }

  /**
   * Validates that none of the schema fields have names that conflict with the transformer's configuration.
   */
  private static void validateSchemaFieldNames(Set<String> schemaFields, JsonLogTransformerConfig transformerConfig) {
    // Validate that none of the columns in the schema end with unindexableFieldSuffix
    String unindexableFieldSuffix = transformerConfig.getUnindexableFieldSuffix();
    if (null != unindexableFieldSuffix) {
      for (String field : schemaFields) {
        Preconditions.checkState(!field.endsWith(unindexableFieldSuffix), "Field '%s' has no-index suffix '%s'", field,
            unindexableFieldSuffix);
      }
    }

    // Validate that none of the columns in the schema end overlap with the fields in fieldPathsToDrop
    Set<String> fieldPathsToDrop = transformerConfig.getFieldPathsToDrop();
    if (null != fieldPathsToDrop) {
      Set<String> fieldIntersection = new HashSet<>(schemaFields);
      fieldIntersection.retainAll(fieldPathsToDrop);
      Preconditions.checkState(fieldIntersection.isEmpty(), "Fields in schema overlap with fieldPathsToDrop");
    }
  }

  /**
   * @return The field type for the given extras field
   */
  private static DataType getAndValidateExtrasFieldType(Schema schema, @Nonnull String extrasFieldName) {
    FieldSpec fieldSpec = schema.getFieldSpecFor(extrasFieldName);
    Preconditions.checkState(null != fieldSpec, "Field '%s' doesn't exist in schema", extrasFieldName);
    DataType fieldDataType = fieldSpec.getDataType();
    Preconditions.checkState(DataType.JSON == fieldDataType || DataType.STRING == fieldDataType,
        "Field '%s' has unsupported type %s", fieldDataType.toString());
    return fieldDataType;
  }

  /**
   * Validates the schema with a JsonLogTransformerConfig instance and creates a tree representing the fields in the
   * schema to be used when transforming input records. For instance, the field "a.b" in the schema would be
   * un-flattened into "{a: b: null}" in the tree, allowing us to more easily process records containing the latter.
   * @throws IllegalArgumentException if schema validation fails
   */
  private static Map<String, Object> validateSchemaAndCreateTree(@Nonnull Schema schema)
      throws IllegalArgumentException {
    Set<String> schemaFields = schema.getPhysicalColumnNames();

    Map<String, Object> schemaTree = new HashMap<>();
    List<String> subKeys = new ArrayList<>();
    for (String field : schemaFields) {
      int keySeparatorIdx = field.indexOf(JsonUtils.KEY_SEPARATOR);
      if (-1 == keySeparatorIdx) {
        // Not a flattened key
        schemaTree.put(field, null);
        continue;
      }

      subKeys.clear();
      getAndValidateSubKeys(field, keySeparatorIdx, subKeys);

      // Add all sub-keys except the leaf to the tree
      Map<String, Object> currentNode = schemaTree;
      for (int i = 0; i < subKeys.size() - 1; i++) {
        String subKey = subKeys.get(i);

        Map<String, Object> childNode;
        if (currentNode.containsKey(subKey)) {
          childNode = (Map<String, Object>) currentNode.get(subKey);
          if (null == childNode) {
            throw new IllegalArgumentException(
                "Cannot handle field '" + String.join(JsonUtils.KEY_SEPARATOR, subKeys.subList(0, i + 1))
                    + "' which overlaps with another field in the schema.");
          }
        } else {
          childNode = new HashMap<>();
          currentNode.put(subKey, childNode);
        }
        currentNode = childNode;
      }
      // Add the leaf pointing at null
      String subKey = subKeys.get(subKeys.size() - 1);
      if (currentNode.containsKey(subKey)) {
        throw new IllegalArgumentException(
            "Cannot handle field '" + field + "' which overlaps with another field in the schema.");
      }
      currentNode.put(subKey, null);
    }

    return schemaTree;
  }

  /**
   * Given a JSON path (e.g. "k1.k2.k3"), returns all the sub-keys (e.g. ["k1", "k2", "k3"])
   * @param key The complete key
   * @param firstKeySeparatorIdx The index of the first key separator in {@code key}
   * @param subKeys Returns the sub-keys
   * @throws IllegalArgumentException if any sub-key is empty
   */
  private static void getAndValidateSubKeys(String key, int firstKeySeparatorIdx, List<String> subKeys)
      throws IllegalArgumentException {
    int subKeyBeginIdx = 0;
    int subKeyEndIdx = firstKeySeparatorIdx;
    int keyLength = key.length();
    while (true) {
      // Validate and add the sub-key
      String subKey = key.substring(subKeyBeginIdx, subKeyEndIdx);
      if (subKey.isEmpty()) {
        throw new IllegalArgumentException("Unsupported empty sub-key in '" + key + "'.");
      }
      subKeys.add(subKey);

      // Advance to the beginning of the next sub-key
      subKeyBeginIdx = subKeyEndIdx + 1;
      if (subKeyBeginIdx >= keyLength) {
        break;
      }

      // Find the end of the next sub-key
      int keySeparatorIdx = key.indexOf(JsonUtils.KEY_SEPARATOR, subKeyBeginIdx);
      if (-1 != keySeparatorIdx) {
        subKeyEndIdx = keySeparatorIdx;
      } else {
        subKeyEndIdx = key.length();
      }
    }
  }

  public JsonLogTransformer(TableConfig tableConfig, Schema schema) {
    if (null == tableConfig.getIngestionConfig() || null == tableConfig.getIngestionConfig()
        .getJsonLogTransformerConfig()) {
      _continueOnError = false;
      _transformerConfig = null;
      _indexableExtrasFieldType = null;
      _unindexableExtrasFieldType = null;
      return;
    }

    _continueOnError = tableConfig.getIngestionConfig().isContinueOnError();
    _transformerConfig = tableConfig.getIngestionConfig().getJsonLogTransformerConfig();
    String indexableExtrasFieldName = _transformerConfig.getIndexableExtrasField();
    _indexableExtrasFieldType = getAndValidateExtrasFieldType(schema, indexableExtrasFieldName);
    String unindexableExtrasFieldName = _transformerConfig.getUnindexableExtrasField();
    _unindexableExtrasFieldType =
        null == unindexableExtrasFieldName ? null : getAndValidateExtrasFieldType(schema, unindexableExtrasFieldName);

    _schemaTree = validateSchemaAndCreateTree(schema);
  }

  @Override
  public boolean isNoOp() {
    return null == _transformerConfig;
  }

  @Nullable
  @Override
  public GenericRow transform(GenericRow record) {
    GenericRow outputRecord = new GenericRow();

    try {
      ExtraFieldsContainer extraFieldsContainer =
          new ExtraFieldsContainer(null != _transformerConfig.getUnindexableExtrasField());
      for (Map.Entry<String, Object> recordEntry : record.getFieldToValueMap().entrySet()) {
        String recordKey = recordEntry.getKey();
        Object recordValue = recordEntry.getValue();
        processField(_schemaTree, recordKey, recordKey, recordValue, extraFieldsContainer, outputRecord);
      }
      putExtrasField(_transformerConfig.getIndexableExtrasField(), _indexableExtrasFieldType,
          extraFieldsContainer.getIndexableExtras(), outputRecord);
      putExtrasField(_transformerConfig.getUnindexableExtrasField(), _unindexableExtrasFieldType,
          extraFieldsContainer.getUnindexableExtras(), outputRecord);
    } catch (Exception e) {
      if (!_continueOnError) {
        throw e;
      }
      _logger.debug("Couldn't transform record: {}", record.toString(), e);
      outputRecord.putValue(GenericRow.INCOMPLETE_RECORD_KEY, true);
    }

    return outputRecord;
  }

  /**
   * Processes a field from the record and either:
   * <ul>
   *   <li>Drops it if it's in fieldPathsToDrop</li>
   *   <li>Adds it to the output record if it's special or exists in the schema</li>
   *   <li>Adds it to one of the extras fields</li>
   * </ul>
   * <p>
   * This method works recursively to build the output record. It is similar to {@code addIndexableField} except it
   * handles fields which exist in the schema.
   * @param schemaNode The current node in the schema tree
   * @param keyJsonPath The JSON path (without the "$." prefix) of the current field
   * @param key
   * @param value
   * @param extraFieldsContainer A container for the "extras" fields corresponding to this node.
   * @param outputRecord Returns the record after transformation
   */
  private void processField(Map<String, Object> schemaNode, String keyJsonPath, String key, Object value,
      ExtraFieldsContainer extraFieldsContainer, GenericRow outputRecord) {

    if (StreamDataDecoderImpl.isSpecialKeyType(key)) {
      outputRecord.putValue(key, value);
      return;
    }

    Set<String> fieldPathsToDrop = _transformerConfig.getFieldPathsToDrop();
    if (null != fieldPathsToDrop && fieldPathsToDrop.contains(keyJsonPath)) {
      return;
    }

    String unindexableFieldSuffix = _transformerConfig.getUnindexableFieldSuffix();
    if (null != unindexableFieldSuffix && key.endsWith(unindexableFieldSuffix)) {
      extraFieldsContainer.addUnindexableEntry(key, value);
      return;
    }

    if (!schemaNode.containsKey(key)) {
      addIndexableField(keyJsonPath, key, value, extraFieldsContainer);
      return;
    }

    Map<String, Object> childSchemaNode = (Map<String, Object>) schemaNode.get(key);
    boolean storeUnindexableExtras = _transformerConfig.getUnindexableExtrasField() != null;
    if (null == childSchemaNode) {
      if (!(value instanceof Map) || null == unindexableFieldSuffix) {
        outputRecord.putValue(keyJsonPath, value);
      } else {
        // The field's value is a map which could contain a no-index field, so we need to keep traversing the map
        ExtraFieldsContainer container = new ExtraFieldsContainer(storeUnindexableExtras);
        addIndexableField(keyJsonPath, key, value, container);
        Map<String, Object> indexableFields = container.getIndexableExtras();
        outputRecord.putValue(keyJsonPath, indexableFields.get(key));
        Map<String, Object> unindexableFields = container.getUnindexableExtras();
        if (null != unindexableFields) {
          extraFieldsContainer.addUnindexableEntry(key, unindexableFields.get(key));
        }
      }
    } else {
      if (!(value instanceof Map)) {
        _logger.debug("Record doesn't match schema: Schema node '{}' is a map but record value is a {}", keyJsonPath,
            value.getClass().getName());
        extraFieldsContainer.addIndexableEntry(key, value);
      } else {
        ExtraFieldsContainer childExtraFieldsContainer = new ExtraFieldsContainer(storeUnindexableExtras);
        Map<String, Object> valueAsMap = (Map<String, Object>) value;
        for (Map.Entry<String, Object> entry : valueAsMap.entrySet()) {
          String childKey = entry.getKey();
          processField(childSchemaNode, keyJsonPath + JsonUtils.KEY_SEPARATOR + childKey, childKey, entry.getValue(),
              childExtraFieldsContainer, outputRecord);
        }
        extraFieldsContainer.addChild(key, childExtraFieldsContainer);
      }
    }
  }

  /**
   * Adds an indexable field to the given {@code ExtrasFieldsContainer}.
   * <p>
   * This method is similar to {@code processField} except it doesn't handle fields which exist in the schema.
   */
  void addIndexableField(String recordJsonPath, String key, Object value, ExtraFieldsContainer extraFieldsContainer) {
    Set<String> fieldPathsToDrop = _transformerConfig.getFieldPathsToDrop();
    if (null != fieldPathsToDrop && fieldPathsToDrop.contains(recordJsonPath)) {
      return;
    }

    String unindexableFieldSuffix = _transformerConfig.getUnindexableFieldSuffix();
    if (null != unindexableFieldSuffix && key.endsWith(unindexableFieldSuffix)) {
      extraFieldsContainer.addUnindexableEntry(key, value);
      return;
    }

    boolean storeUnindexableExtras = _transformerConfig.getUnindexableExtrasField() != null;
    if (!(value instanceof Map)) {
      extraFieldsContainer.addIndexableEntry(key, value);
    } else {
      ExtraFieldsContainer childExtraFieldsContainer = new ExtraFieldsContainer(storeUnindexableExtras);
      Map<String, Object> valueAsMap = (Map<String, Object>) value;
      for (Map.Entry<String, Object> entry : valueAsMap.entrySet()) {
        String childKey = entry.getKey();
        addIndexableField(recordJsonPath + JsonUtils.KEY_SEPARATOR + childKey, childKey, entry.getValue(),
            childExtraFieldsContainer);
      }
      extraFieldsContainer.addChild(key, childExtraFieldsContainer);
    }
  }

  /**
   * Converts (if necessary) and adds the given extras field to the output record
   */
  private void putExtrasField(String fieldName, DataType fieldType, Map<String, Object> field,
      GenericRow outputRecord) {
    if (null == field) {
      return;
    }

    switch (fieldType) {
      case JSON:
        outputRecord.putValue(fieldName, field);
        break;
      case STRING:
        try {
          outputRecord.putValue(fieldName, JsonUtils.objectToString(field));
        } catch (JsonProcessingException e) {
          throw new RuntimeException("Failed to convert '" + fieldName + "' to string", e);
        }
        break;
      default:
        throw new UnsupportedOperationException("Cannot convert '" + fieldName + "' to " + fieldType.name());
    }
  }
}

/**
 * A class to encapsulate the "extras" fields (indexableExtras and unindexableExtras) at a node in the record (when
 * viewed as a tree).
 */
class ExtraFieldsContainer {
  private Map<String, Object> _indexableExtras = null;
  private Map<String, Object> _unindexableExtras = null;
  private final boolean _storeUnindexableExtras;

  ExtraFieldsContainer(boolean storeUnindexableExtras) {
    _storeUnindexableExtras = storeUnindexableExtras;
  }

  public Map<String, Object> getIndexableExtras() {
    return _indexableExtras;
  }

  public Map<String, Object> getUnindexableExtras() {
    return _unindexableExtras;
  }

  /**
   * Adds the given kv-pair to the indexable extras field
   */
  public void addIndexableEntry(String key, Object value) {
    if (null == _indexableExtras) {
      _indexableExtras = new HashMap<>();
    }
    _indexableExtras.put(key, value);
  }

  /**
   * Adds the given kv-pair to the unindexable extras field (if any)
   */
  public void addUnindexableEntry(String key, Object value) {
    if (!_storeUnindexableExtras) {
      return;
    }
    if (null == _unindexableExtras) {
      _unindexableExtras = new HashMap<>();
    }
    _unindexableExtras.put(key, value);
  }

  /**
   * Given a container corresponding to a child node, attach the extras from the child node to the extras in this node
   * at the given key.
   */
  public void addChild(String key, ExtraFieldsContainer child) {
    Map<String, Object> childIndexableFields = child.getIndexableExtras();
    if (null != childIndexableFields) {
      addIndexableEntry(key, childIndexableFields);
    }

    Map<String, Object> childUnindexableFields = child.getUnindexableExtras();
    if (null != childUnindexableFields) {
      addUnindexableEntry(key, childUnindexableFields);
    }
  }
}
