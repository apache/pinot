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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.utils.Base64Utils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.SchemaConformingTransformerConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.stream.StreamDataDecoderImpl;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This transformer transforms records with varied structures so that they can be stored in a Pinot table.
 * Since the records do not have uniform structure, it is impractical to store each field in its own table column.
 * In high level, if a field exists in the table schema, this transformer puts the value to the corresponding column.
 * For those fields which do not exist in the table schema, it stores them in a type of catchall field in a json map.
 * For example, consider this record:
 * <pre>
 * {
 *   "a": 1,
 *   "b": "2",
 *   "c": {
 *     "d": 3,
 *     "x": {
 *       "y": 9,
 *     }
 *   }
 * }
 * </pre>
 * And let's say the table's schema is:
 * <ul>
 *   <li>a</li>
 *   <li>c</li>
 *   <li>c.d</li>
 * </ul>
 * <p>
 * The record would be transformed into the following (refer to {@link SchemaConformingTransformerConfig} for
 * default constant values) where json_data is the catch-all field:
 * <pre>
 * {
 *   "a": 1,
 *   "c.d": 3,
 *   "json_data": {
 *     "b": "2",
 *     "c": {
 *       "x": {
 *         "y": 9
 *       }
 *     }
 *   }
 * }
 * Apart from the basic transformation above, this transformer today also does the following additional tasks (which in
 * future can be decoupled from this transformer):
 *    1. Put all field + value pair in a special column "_mergedTextIndex" to facilitate full text indexing and search.
 *    This extra step can be enabled via mergedTextIndexFieldSpec.
 *    2. Allow users to tag certain fields in the input record not to be included in the catch-all field.
 * </pre>
 * <p>
 */
public class SchemaConformingTransformer implements RecordTransformer {
  private static final Logger _logger = LoggerFactory.getLogger(SchemaConformingTransformer.class);
  private static final int MAXIMUM_LUCENE_DOCUMENT_SIZE = 32766;
  private static final List<String> MERGED_TEXT_INDEX_SUFFIX_TO_EXCLUDE = Arrays.asList("_logtype", "_dictionaryVars",
      "_encodedVars");

  private final boolean _continueOnError;
  private final DataType _indexableExtrasFieldType;
  private final DataType _unindexableExtrasFieldType;
  private final DimensionFieldSpec _mergedTextIndexFieldSpec;
  private final SchemaConformingTransformerConfig _transformerConfig;
  @Nullable
  ServerMetrics _serverMetrics = null;
  private SchemaTreeNode _schemaTree;
  @Nullable
  private PinotMeter _realtimeMergedTextIndexTruncatedDocumentSizeMeter = null;
  private String _tableName;
  private int _jsonKeyValueSeparatorByteCount;
  private long _mergedTextIndexDocumentBytesCount = 0L;
  private long _mergedTextIndexDocumentCount = 0L;

  public SchemaConformingTransformer(TableConfig tableConfig, Schema schema) {
    if (null == tableConfig.getIngestionConfig() || null == tableConfig.getIngestionConfig()
        .getSchemaConformingTransformerConfig()) {
      _continueOnError = false;
      _transformerConfig = null;
      _indexableExtrasFieldType = null;
      _unindexableExtrasFieldType = null;
      _mergedTextIndexFieldSpec = null;
      return;
    }

    _continueOnError = tableConfig.getIngestionConfig().isContinueOnError();
    _transformerConfig = tableConfig.getIngestionConfig().getSchemaConformingTransformerConfig();
    String indexableExtrasFieldName = _transformerConfig.getIndexableExtrasField();
    _indexableExtrasFieldType =
        indexableExtrasFieldName == null ? null : getAndValidateExtrasFieldType(schema,
            indexableExtrasFieldName);
    String unindexableExtrasFieldName = _transformerConfig.getUnindexableExtrasField();
    _unindexableExtrasFieldType =
        unindexableExtrasFieldName == null ? null : getAndValidateExtrasFieldType(schema,
            unindexableExtrasFieldName);
    _mergedTextIndexFieldSpec = schema.getDimensionSpec(_transformerConfig.getMergedTextIndexField());
    _tableName = tableConfig.getTableName();
    _schemaTree = validateSchemaAndCreateTree(schema, _transformerConfig);
    _serverMetrics = ServerMetrics.get();
    _jsonKeyValueSeparatorByteCount = _transformerConfig.getJsonKeyValueSeparator()
        .getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
  }

  /**
   * Validates the schema against the given transformer's configuration.
   */
  public static void validateSchema(@Nonnull Schema schema,
      @Nonnull SchemaConformingTransformerConfig transformerConfig) {
    validateSchemaFieldNames(schema.getPhysicalColumnNames(), transformerConfig);

    String indexableExtrasFieldName = transformerConfig.getIndexableExtrasField();
    if (null != indexableExtrasFieldName) {
      getAndValidateExtrasFieldType(schema, indexableExtrasFieldName);
    }
    String unindexableExtrasFieldName = transformerConfig.getUnindexableExtrasField();
    if (null != unindexableExtrasFieldName) {
      getAndValidateExtrasFieldType(schema, indexableExtrasFieldName);
    }

    Map<String, String> columnNameToJsonKeyPathMap = transformerConfig.getColumnNameToJsonKeyPathMap();
    for (Map.Entry<String, String> entry : columnNameToJsonKeyPathMap.entrySet()) {
      String columnName = entry.getKey();
      FieldSpec fieldSpec = schema.getFieldSpecFor(entry.getKey());
      Preconditions.checkState(null != fieldSpec, "Field '%s' doesn't exist in schema", columnName);
    }
    Set<String> preserveFieldNames = transformerConfig.getFieldPathsToPreserveInput();
    for (String preserveFieldName : preserveFieldNames) {
      Preconditions.checkState(
          columnNameToJsonKeyPathMap.containsValue(preserveFieldName)
              || schema.getFieldSpecFor(preserveFieldName) != null,
          "Preserved path '%s' doesn't exist in columnNameToJsonKeyPathMap or schema", preserveFieldName);
    }

    validateSchemaAndCreateTree(schema, transformerConfig);
  }

  /**
   * Heuristic filter to detect whether a byte array is longer than a specified length and contains only base64
   * characters so that we treat it as encoded binary data.
   * @param bytes array to check
   * @param minLength byte array shorter than this length will not be treated as encoded binary data
   * @return true if the input bytes is base64 encoded binary data by the heuristic above, false otherwise
   */
  public static boolean base64ValueFilter(final byte[] bytes, int minLength) {
    return bytes.length >= minLength && Base64Utils.isBase64IgnoreTrailingPeriods(bytes);
  }

  /**
   * Validates that none of the schema fields have names that conflict with the transformer's configuration.
   */
  private static void validateSchemaFieldNames(Set<String> schemaFields,
      SchemaConformingTransformerConfig transformerConfig) {
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
   * Validates the schema with a {@link SchemaConformingTransformerConfig} instance and creates a tree representing
   * the fields in the schema to be used when transforming input records. Refer to {@link SchemaTreeNode} for details.
   * @throws IllegalArgumentException if schema validation fails in:
   * <ul>
   *   <li>One of the fields in the schema has a name which when interpreted as a JSON path, corresponds to an object
   *   with an empty sub-key. E.g., the field name "a..b" corresponds to the JSON {"a": {"": {"b": ...}}}</li>
   * </ul>
   */
  private static SchemaTreeNode validateSchemaAndCreateTree(@Nonnull Schema schema,
      @Nonnull SchemaConformingTransformerConfig transformerConfig)
      throws IllegalArgumentException {
    Set<String> schemaFields = schema.getPhysicalColumnNames();
    Map<String, String> jsonKeyPathToColumnNameMap = new HashMap<>();
    for (Map.Entry<String, String> entry : transformerConfig.getColumnNameToJsonKeyPathMap().entrySet()) {
      String columnName = entry.getKey();
      String jsonKeyPath = entry.getValue();
      schemaFields.remove(columnName);
      schemaFields.add(jsonKeyPath);
      jsonKeyPathToColumnNameMap.put(jsonKeyPath, columnName);
    }

    SchemaTreeNode rootNode = new SchemaTreeNode("", null, schema);
    List<String> subKeys = new ArrayList<>();
    for (String field : schemaFields) {
      SchemaTreeNode currentNode = rootNode;
      int keySeparatorIdx = field.indexOf(JsonUtils.KEY_SEPARATOR);
      if (-1 == keySeparatorIdx) {
        // Not a flattened key
        currentNode = rootNode.getAndCreateChild(field, schema);
      } else {
        subKeys.clear();
        getAndValidateSubKeys(field, keySeparatorIdx, subKeys);
        for (String subKey : subKeys) {
          SchemaTreeNode childNode = currentNode.getAndCreateChild(subKey, schema);
          currentNode = childNode;
        }
      }
      currentNode.setColumn(jsonKeyPathToColumnNameMap.get(field), schema);
    }

    return rootNode;
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

  @Override
  public boolean isNoOp() {
    return null == _transformerConfig;
  }

  @Nullable
  @Override
  public GenericRow transform(GenericRow record) {
    GenericRow outputRecord = new GenericRow();
    Map<String, Object> mergedTextIndexMap = new HashMap<>();

    try {
      Deque<String> jsonPath = new ArrayDeque<>();
      ExtraFieldsContainer extraFieldsContainer =
          new ExtraFieldsContainer(null != _transformerConfig.getUnindexableExtrasField());
      for (Map.Entry<String, Object> recordEntry : record.getFieldToValueMap().entrySet()) {
        String recordKey = recordEntry.getKey();
        Object recordValue = recordEntry.getValue();
        jsonPath.addLast(recordKey);
        ExtraFieldsContainer currentFieldsContainer =
            processField(_schemaTree, jsonPath, recordValue, true, outputRecord, mergedTextIndexMap);
        extraFieldsContainer.addChild(currentFieldsContainer);
        jsonPath.removeLast();
      }
      putExtrasField(_transformerConfig.getIndexableExtrasField(), _indexableExtrasFieldType,
          extraFieldsContainer.getIndexableExtras(), outputRecord);
      putExtrasField(_transformerConfig.getUnindexableExtrasField(), _unindexableExtrasFieldType,
          extraFieldsContainer.getUnindexableExtras(), outputRecord);

      // Generate merged text index. This optional step puts all field + value pairs in the input record in a special
      // column "_mergedTextIndex" to perform full text indexing and search.
      if (null != _mergedTextIndexFieldSpec && !mergedTextIndexMap.isEmpty()) {
        List<String> luceneDocuments = getLuceneDocumentsFromMergedTextIndexMap(mergedTextIndexMap);
        if (_mergedTextIndexFieldSpec.isSingleValueField()) {
          outputRecord.putValue(_mergedTextIndexFieldSpec.getName(), String.join(" ", luceneDocuments));
        } else {
          outputRecord.putValue(_mergedTextIndexFieldSpec.getName(), luceneDocuments);
        }
      }
    } catch (Exception e) {
      if (!_continueOnError) {
        throw e;
      }
      _logger.error("Couldn't transform record: {}", record.toString(), e);
      outputRecord.putValue(GenericRow.INCOMPLETE_RECORD_KEY, true);
    }

    return outputRecord;
  }

  /**
   * The method traverses the record and schema tree at the same time. It would check the specs of record key/value
   * pairs with the corresponding schema tree node and {#link SchemaConformingTransformerConfig}. Finally drop or put
   * them into the output record with the following logics:
   * Taking example:
   * {
   *   "a": 1,
   *   "b": {
   *     "c": 2,
   *     "d": 3,
   *     "d_noIdx": 4
   *   }
   *   "b_noIdx": {
   *     "c": 5,
   *     "d": 6,
   *   }
   * }
   * with column "a", "b", "b.c" in schema
   * There are two types of output:
   *  - flattened keys with values, e.g.,
   *    - keyPath as column and value as leaf node, e.g., "a": 1, "b.c": 2. However, "b" is not a leaf node, so it would
   *    be skipped
   *    - __mergedTestIdx storing ["1:a", "2:b.c", "3:b.d"] as a string array
   *  - structured Json format, e.g.,
   *    - indexableFields/json_data: {"a": 1, "b": {"c": 2, "d": 3}}
   *    - unindexableFields/json_data_noIdx: {"b": {"d_noIdx": 4} ,"b_noIdx": {"c": 5, "d": 6}}
   * Expected behavior:
   *  - If the current key is special, it would be added to the outputRecord and skip subtree
   *  - If the keyJsonPath is in fieldPathsToDrop, it and its subtree would be skipped
   *  - At leaf node (base case in recursion):
   *    - Parse keyPath and value and add as flattened result to outputRecord
   *    - Return structured fields as ExtraFieldsContainer
   *   (leaf node is defined as node not as "Map" type. Leaf node is possible to be collection of or array of "Map". But
   *   for simplicity, we still treat it as leaf node and do not traverse its children)
   *  - For non-leaf node
   *    - Construct ExtraFieldsContainer based on children's result and return
   *
   * @param parentNode The parent node in the schema tree which might or might not has a child with the given key. If
   *                  parentNode is null, it means the current key is out of the schema tree.
   * @param jsonPath The key json path split by "."
   * @param value The value of the current field
   * @param isIndexable Whether the current field is indexable
   * @param outputRecord The output record updated during traverse
   * @param mergedTextIndexMap The merged text index map updated during traverse
   * @return ExtraFieldsContainer carries the indexable and unindexable fields of the current node as well as its
   * subtree
   */
  private ExtraFieldsContainer processField(SchemaTreeNode parentNode, Deque<String> jsonPath, Object value,
      boolean isIndexable, GenericRow outputRecord, Map<String, Object> mergedTextIndexMap) {
    // Common variables
    boolean storeIndexableExtras = _transformerConfig.getIndexableExtrasField() != null;
    boolean storeUnindexableExtras = _transformerConfig.getUnindexableExtrasField() != null;
    String key = jsonPath.peekLast();
    ExtraFieldsContainer extraFieldsContainer = new ExtraFieldsContainer(storeUnindexableExtras);

    // Base case
    if (StreamDataDecoderImpl.isSpecialKeyType(key) || GenericRow.isSpecialKeyType(key)) {
      outputRecord.putValue(key, value);
      return extraFieldsContainer;
    }

    String keyJsonPath = String.join(".", jsonPath);

    Set<String> fieldPathsToDrop = _transformerConfig.getFieldPathsToDrop();
    if (null != fieldPathsToDrop && fieldPathsToDrop.contains(keyJsonPath)) {
      return extraFieldsContainer;
    }

    SchemaTreeNode currentNode =
        parentNode == null ? null : parentNode.getChild(key, _transformerConfig.isUseAnonymousDotInFieldNames());
    if (_transformerConfig.getFieldPathsToPreserveInput().contains(keyJsonPath)
        || _transformerConfig.getFieldPathsToPreserveInputWithIndex().contains(keyJsonPath)) {
      if (currentNode != null) {
        outputRecord.putValue(currentNode.getColumnName(), currentNode.getValue(value));
      } else {
        outputRecord.putValue(keyJsonPath, value);
      }
      if (_transformerConfig.getFieldPathsToPreserveInputWithIndex().contains(keyJsonPath)) {
        flattenAndAddToMergedTextIndexMap(mergedTextIndexMap, keyJsonPath, value);
      }
      return extraFieldsContainer;
    }
    String unindexableFieldSuffix = _transformerConfig.getUnindexableFieldSuffix();
    isIndexable = isIndexable && (null == unindexableFieldSuffix || !key.endsWith(unindexableFieldSuffix));

    // return in advance to truncate the subtree if nothing left to be added
    if (currentNode == null && !storeIndexableExtras && !storeUnindexableExtras) {
      return extraFieldsContainer;
    }

    if (value == null) {
      return extraFieldsContainer;
    }
    if (!(value instanceof Map)) {
      // leaf node
      if (!isIndexable) {
        extraFieldsContainer.addUnindexableEntry(key, value);
      } else {
        if (null != currentNode && currentNode.isColumn()) {
          // In schema
          outputRecord.putValue(currentNode.getColumnName(), currentNode.getValue(value));
          if (_transformerConfig.getFieldsToDoubleIngest().contains(keyJsonPath)) {
            extraFieldsContainer.addIndexableEntry(key, value);
          }
          mergedTextIndexMap.put(currentNode.getColumnName(), value);
        } else {
          // The field is not mapped to one of the dedicated columns in the Pinot table schema. Thus it will be put
          // into the extraField column of the table.
          if (storeIndexableExtras) {
            if (!_transformerConfig.getFieldPathsToSkipStorage().contains(keyJsonPath)) {
              extraFieldsContainer.addIndexableEntry(key, value);
            }
            mergedTextIndexMap.put(keyJsonPath, value);
          }
        }
      }
      return extraFieldsContainer;
    }
    // Traverse the subtree
    Map<String, Object> valueAsMap = (Map<String, Object>) value;
    for (Map.Entry<String, Object> entry : valueAsMap.entrySet()) {
      jsonPath.addLast(entry.getKey());
      ExtraFieldsContainer childContainer =
          processField(currentNode, jsonPath, entry.getValue(), isIndexable, outputRecord, mergedTextIndexMap);
      extraFieldsContainer.addChild(key, childContainer);
      jsonPath.removeLast();
    }
    return extraFieldsContainer;
  }

  /**
   * Generate a Lucene document based on the provided key-value pair.
   * The index document follows this format: "val" + jsonKeyValueSeparator + "key".
   * @param kv                               used to generate text index documents
   * @param indexDocuments                   a list to store the generated index documents
   * @param mergedTextIndexDocumentMaxLength which we enforce via truncation during document generation
   */
  public void generateTextIndexLuceneDocument(Map.Entry<String, Object> kv, List<String> indexDocuments,
      Integer mergedTextIndexDocumentMaxLength) {
    String key = kv.getKey();
    // To avoid redundant leading and tailing '"', only convert to JSON string if the value is a list or an array
    if (kv.getValue() instanceof Collection || kv.getValue() instanceof Object[]) {
      // Add the entire array or collection as one string to the Lucene doc.
      try {
        addLuceneDoc(indexDocuments, mergedTextIndexDocumentMaxLength, key, JsonUtils.objectToString(kv.getValue()));
        // To enable array contains search, we also add each array element with the key value pair to the Lucene doc.
        // Currently it only supports 1 level flattening, any element deeper than 1 level will still stay nested.
        if (kv.getValue() instanceof Collection) {
          for (Object o : (Collection) kv.getValue()) {
            addLuceneDoc(indexDocuments, mergedTextIndexDocumentMaxLength, key, JsonUtils.objectToString(o));
          }
        } else if (kv.getValue() instanceof Object[]) {
          for (Object o : (Object[]) kv.getValue()) {
            addLuceneDoc(indexDocuments, mergedTextIndexDocumentMaxLength, key, JsonUtils.objectToString(o));
          }
        }
      } catch (JsonProcessingException e) {
        addLuceneDoc(indexDocuments, mergedTextIndexDocumentMaxLength, key, kv.getValue().toString());
      }
      return;
    }

    // If the value is a single value
    addLuceneDoc(indexDocuments, mergedTextIndexDocumentMaxLength, key, kv.getValue().toString());
  }

  private void addLuceneDoc(List<String> indexDocuments, Integer mergedTextIndexDocumentMaxLength, String key,
      String val) {
    if (key.length() + _jsonKeyValueSeparatorByteCount > MAXIMUM_LUCENE_DOCUMENT_SIZE) {
      _logger.error("The provided key's length is too long, text index document cannot be truncated");
      return;
    }

    // Truncate the value to ensure the generated index document is less or equal to mergedTextIndexDocumentMaxLength
    // The value length should be the mergedTextIndexDocumentMaxLength minus key length, and then minus the byte length
    // of ":" or the specified Json key value separator character
    int valueTruncationLength = mergedTextIndexDocumentMaxLength - _jsonKeyValueSeparatorByteCount - key.length();
    if (val.length() > valueTruncationLength) {
      _realtimeMergedTextIndexTruncatedDocumentSizeMeter = _serverMetrics
          .addMeteredTableValue(_tableName, ServerMeter.REALTIME_MERGED_TEXT_IDX_TRUNCATED_DOCUMENT_SIZE,
              key.length() + _jsonKeyValueSeparatorByteCount + val.length(),
              _realtimeMergedTextIndexTruncatedDocumentSizeMeter);
      val = val.substring(0, valueTruncationLength);
    }

    _mergedTextIndexDocumentBytesCount += key.length() + _jsonKeyValueSeparatorByteCount + val.length();
    _mergedTextIndexDocumentCount += 1;
    _serverMetrics.setValueOfTableGauge(_tableName, ServerGauge.REALTIME_MERGED_TEXT_IDX_DOCUMENT_AVG_LEN,
        _mergedTextIndexDocumentBytesCount / _mergedTextIndexDocumentCount);

    addKeyValueToDocuments(indexDocuments, key, val, _transformerConfig.isReverseTextIndexKeyValueOrder(),
        _transformerConfig.isOptimizeCaseInsensitiveSearch());
  }

  private void flattenAndAddToMergedTextIndexMap(Map<String, Object> mergedTextIndexMap, String key, Object value) {
    String unindexableFieldSuffix = _transformerConfig.getUnindexableFieldSuffix();
    if (null != unindexableFieldSuffix && key.endsWith(unindexableFieldSuffix)) {
      return;
    }
    if (value instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) value;
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        flattenAndAddToMergedTextIndexMap(mergedTextIndexMap, key + "." + entry.getKey(), entry.getValue());
      }
    } else {
      mergedTextIndexMap.put(key, value);
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

  private List<String> getLuceneDocumentsFromMergedTextIndexMap(Map<String, Object> mergedTextIndexMap) {
    final Integer mergedTextIndexDocumentMaxLength = _transformerConfig.getMergedTextIndexDocumentMaxLength();
    final @Nullable
    List<String> luceneDocuments = new ArrayList<>();
    mergedTextIndexMap.entrySet().stream().filter(kv -> null != kv.getKey() && null != kv.getValue())
        .filter(kv -> !_transformerConfig.getMergedTextIndexPathToExclude().contains(kv.getKey())).filter(
        kv -> !base64ValueFilter(kv.getValue().toString().getBytes(),
            _transformerConfig.getMergedTextIndexBinaryDocumentDetectionMinLength())).filter(
        kv -> !MERGED_TEXT_INDEX_SUFFIX_TO_EXCLUDE.stream()
            .anyMatch(suffix -> kv.getKey().endsWith(suffix))).forEach(kv -> {
      generateTextIndexLuceneDocument(kv, luceneDocuments, mergedTextIndexDocumentMaxLength);
    });
    return luceneDocuments;
  }

  private void addKeyValueToDocuments(List<String> documents, String key, String value, boolean addInReverseOrder,
      boolean addCaseInsensitiveVersion) {
    addKeyValueToDocumentWithOrder(documents, key, value, addInReverseOrder);

    // To optimize the case insensitive search, add the lower case version if applicable
    // Note that we only check the value as Key is always case-sensitive search
    if (addCaseInsensitiveVersion && value.chars().anyMatch(Character::isUpperCase)) {
      addKeyValueToDocumentWithOrder(documents, key, value.toLowerCase(Locale.ENGLISH), addInReverseOrder);
    }
  }

  private void addKeyValueToDocumentWithOrder(List<String> documents, String key, String value,
      boolean addInReverseOrder) {
    // Not doing refactor here to avoid allocating new intermediate string
    if (addInReverseOrder) {
      documents.add(_transformerConfig.getMergedTextIndexBeginOfDocAnchor() + value
          + _transformerConfig.getJsonKeyValueSeparator() + key
          + _transformerConfig.getMergedTextIndexEndOfDocAnchor());
    } else {
      documents.add(_transformerConfig.getMergedTextIndexBeginOfDocAnchor() + key
          + _transformerConfig.getJsonKeyValueSeparator() + value
          + _transformerConfig.getMergedTextIndexEndOfDocAnchor());
    }
  }
}

/**
 * SchemaTreeNode represents the tree node when we construct the schema tree. The node could be either leaf node or
 * non-leaf node. Both types of node could hold the volumn as a column in the schema.
 * For example, the schema with columns a, b, c, d.e, d.f, x.y, x.y.z, x.y.w will have the following tree structure:
 * root -- a*
 *      -- b*
 *      -- c*
 *      -- d -- e*
 *           -- f*
 *      -- x* -- y* -- z*
 *                  -- w*
 * where node with "*" could represent a valid column in the schema.
 */
class SchemaTreeNode {
  private boolean _isColumn;
  private final Map<String, SchemaTreeNode> _children;
  // Taking the example of key "x.y.z", the keyName will be "z" and the parentPath will be "x.y"
  // Root node would have keyName as "" and parentPath as null
  // Root node's children will have keyName as the first level key and parentPath as ""
  @Nonnull
  private final String _keyName;
  @Nullable
  private String _columnName;
  @Nullable
  private final String _parentPath;
  private FieldSpec _fieldSpec;

  public SchemaTreeNode(String keyName, String parentPath, Schema schema) {
    _keyName = keyName;
    _parentPath = parentPath;
    _fieldSpec = schema.getFieldSpecFor(getJsonKeyPath());
    _children = new HashMap<>();
  }

  public boolean isColumn() {
    return _isColumn;
  }

  public void setColumn(String columnName, Schema schema) {
    if (columnName == null) {
      _columnName = getJsonKeyPath();
    } else {
      _columnName = columnName;
      _fieldSpec = schema.getFieldSpecFor(columnName);
    }
    _isColumn = true;
  }

  public boolean hasChild(String key) {
    return _children.containsKey(key);
  }

  /**
   * If does not have the child node, add a child node to the current node and return the child node.
   * If the child node already exists, return the existing child node.
   * @param key
   * @return
   */
  public SchemaTreeNode getAndCreateChild(String key, Schema schema) {
    SchemaTreeNode child = _children.get(key);
    if (child == null) {
      child = new SchemaTreeNode(key, getJsonKeyPath(), schema);
      _children.put(key, child);
    }
    return child;
  }

  private SchemaTreeNode getChild(String key) {
    return _children.get(key);
  }

  public SchemaTreeNode getChild(String key, boolean useAnonymousDot) {
    if (useAnonymousDot && key.contains(".")) {
      SchemaTreeNode node = this;
      for (String subKey : key.split("\\.")) {
        if (node != null) {
          node = node.getChild(subKey);
        } else {
          return null;
        }
      }
      return node;
    } else {
      return getChild(key);
    }
  }

  public String getKeyName() {
    return _keyName;
  }

  public String getColumnName() {
    return _columnName;
  }

  public Object getValue(Object value) {
    // In {#link DataTypeTransformer}, for a field type as SingleValueField, it does not allow the input value as a
    // collection or array. To prevent the error, we serialize the value to a string if the field is a string type.
    if (_fieldSpec != null && _fieldSpec.getDataType() == DataType.STRING && _fieldSpec.isSingleValueField()) {
      try {
        if (value instanceof Collection) {
          return JsonUtils.objectToString(value);
        }
        if (value instanceof Object[]) {
          return JsonUtils.objectToString(Arrays.asList((Object[]) value));
        }
        if (value instanceof Map) {
          return JsonUtils.objectToString(value);
        }
      } catch (JsonProcessingException e) {
        return value.toString();
      }
    }
    return value;
  }

  public String getJsonKeyPath() {
    if (_parentPath == null || _parentPath.isEmpty()) {
      return _keyName;
    }
    return _parentPath + JsonUtils.KEY_SEPARATOR + _keyName;
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
    if (key == null && value instanceof Map) {
      // If the key is null, it means that the value is a map that should be merged with the indexable extras
      _indexableExtras.putAll((Map<String, Object>) value);
    } else if (_indexableExtras.containsKey(key) && _indexableExtras.get(key) instanceof Map && value instanceof Map) {
      // If the key already exists in the indexable extras and both the existing value and the new value are maps,
      // merge the two maps
      ((Map<String, Object>) _indexableExtras.get(key)).putAll((Map<String, Object>) value);
    } else {
      _indexableExtras.put(key, value);
    }
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
    if (key == null && value instanceof Map) {
      // If the key is null, it means that the value is a map that should be merged with the unindexable extras
      _unindexableExtras.putAll((Map<String, Object>) value);
    } else if (_unindexableExtras.containsKey(key) && _unindexableExtras.get(key) instanceof Map
        && value instanceof Map) {
      // If the key already exists in the uindexable extras and both the existing value and the new value are maps,
      // merge the two maps
      ((Map<String, Object>) _unindexableExtras.get(key)).putAll((Map<String, Object>) value);
    } else {
      _unindexableExtras.put(key, value);
    }
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

  public void addChild(ExtraFieldsContainer child) {
    addChild(null, child);
  }
}
