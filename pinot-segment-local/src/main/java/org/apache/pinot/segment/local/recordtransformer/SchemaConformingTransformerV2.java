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
import org.apache.pinot.spi.config.table.ingestion.SchemaConformingTransformerV2Config;
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
 * This transformer evolves from {@link SchemaConformingTransformer} and is designed to support extra cases for
 * better text searching:
 *   - Support over-lapping schema fields, in which case it could support schema column "a" and "a.b" at the same time.
 *     And it only allows primitive type fields to be the value.
 *   - Extract flattened key-value pairs as mergedTextIndex for better text searching.
 * <p>
 * For example, consider this record:
 * <pre>
 * {
 *   "a": 1,
 *   "b": "2",
 *   "c": {
 *     "d": 3,
 *     "e_noindex": 4,
 *     "f_noindex": {
 *       "g": 5
 *      },
 *     "x": {
 *       "y": 9,
 *       "z_noindex": 10
 *     }
 *   }
 *   "h_noindex": "6",
 *   "i_noindex": {
 *     "j": 7,
 *     "k": 8
 *   }
 * }
 * </pre>
 * And let's say the table's schema contains these fields:
 * <ul>
 *   <li>a</li>
 *   <li>c</li>
 *   <li>c.d</li>
 * </ul>
 * <p>
 * The record would be transformed into the following (refer to {@link SchemaConformingTransformerV2Config} for
 *  * default constant values):
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
 *   "json_data_no_idx": {
 *     "c": {
 *       "e_noindex": 4,
 *       "f_noindex": {
 *         "g": 5
 *       },
 *       "x": {
 *         "z_noindex": 10
 *       }
 *     },
 *     "h_noindex": "6",
 *     "i_noindex": {
 *       "j": 7,
 *       "k": 8
 *     }
 *   },
 *   "__mergedTextIndex": [
 *     "1:a", "2:b", "3:c.d", "9:c.x.y"
 *   ]
 * }
 * </pre>
 * <p>
 * The "__mergedTextIndex" could filter and manipulate the data based on the configuration in
 * {@link SchemaConformingTransformerV2Config}.
 */
public class SchemaConformingTransformerV2 implements RecordTransformer {
  private static final Logger _logger = LoggerFactory.getLogger(SchemaConformingTransformerV2.class);
  private static final int MAXIMUM_LUCENE_DOCUMENT_SIZE = 32766;
  private static final List<String> MERGED_TEXT_INDEX_SUFFIX_TO_EXCLUDE = Arrays.asList("_logtype", "_dictionaryVars",
      "_encodedVars");

  private final boolean _continueOnError;
  private final SchemaConformingTransformerV2Config _transformerConfig;
  private final DataType _indexableExtrasFieldType;
  private final DataType _unindexableExtrasFieldType;
  private final DimensionFieldSpec _mergedTextIndexFieldSpec;
  @Nullable
  ServerMetrics _serverMetrics = null;
  private SchemaTreeNode _schemaTree;
  @Nullable
  private PinotMeter _realtimeMergedTextIndexTruncatedDocumentSizeMeter = null;
  private String _tableName;
  private int _jsonKeyValueSeparatorByteCount;
  private long _mergedTextIndexDocumentBytesCount = 0L;
  private long _mergedTextIndexDocumentCount = 0L;

  public SchemaConformingTransformerV2(TableConfig tableConfig, Schema schema) {
    if (null == tableConfig.getIngestionConfig() || null == tableConfig.getIngestionConfig()
        .getSchemaConformingTransformerV2Config()) {
      _continueOnError = false;
      _transformerConfig = null;
      _indexableExtrasFieldType = null;
      _unindexableExtrasFieldType = null;
      _mergedTextIndexFieldSpec = null;
      return;
    }

    _continueOnError = tableConfig.getIngestionConfig().isContinueOnError();
    _transformerConfig = tableConfig.getIngestionConfig().getSchemaConformingTransformerV2Config();
    String indexableExtrasFieldName = _transformerConfig.getIndexableExtrasField();
    _indexableExtrasFieldType =
        indexableExtrasFieldName == null ? null : SchemaConformingTransformer.getAndValidateExtrasFieldType(schema,
            indexableExtrasFieldName);
    String unindexableExtrasFieldName = _transformerConfig.getUnindexableExtrasField();
    _unindexableExtrasFieldType =
        unindexableExtrasFieldName == null ? null : SchemaConformingTransformer.getAndValidateExtrasFieldType(schema,
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
      @Nonnull SchemaConformingTransformerV2Config transformerConfig) {
    validateSchemaFieldNames(schema.getPhysicalColumnNames(), transformerConfig);

    String indexableExtrasFieldName = transformerConfig.getIndexableExtrasField();
    if (null != indexableExtrasFieldName) {
      SchemaConformingTransformer.getAndValidateExtrasFieldType(schema, indexableExtrasFieldName);
    }
    String unindexableExtrasFieldName = transformerConfig.getUnindexableExtrasField();
    if (null != unindexableExtrasFieldName) {
      SchemaConformingTransformer.getAndValidateExtrasFieldType(schema, indexableExtrasFieldName);
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
      SchemaConformingTransformerV2Config transformerConfig) {
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
   * Validates the schema with a {@link SchemaConformingTransformerV2Config} instance and creates a tree representing
   * the fields in the schema to be used when transforming input records. Refer to {@link SchemaTreeNode} for details.
   * @throws IllegalArgumentException if schema validation fails in:
   * <ul>
   *   <li>One of the fields in the schema has a name which when interpreted as a JSON path, corresponds to an object
   *   with an empty sub-key. E.g., the field name "a..b" corresponds to the JSON {"a": {"": {"b": ...}}}</li>
   * </ul>
   */
  private static SchemaTreeNode validateSchemaAndCreateTree(@Nonnull Schema schema,
      @Nonnull SchemaConformingTransformerV2Config transformerConfig)
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
        SchemaConformingTransformer.getAndValidateSubKeys(field, keySeparatorIdx, subKeys);
        for (String subKey : subKeys) {
          SchemaTreeNode childNode = currentNode.getAndCreateChild(subKey, schema);
          currentNode = childNode;
        }
      }
      currentNode.setColumn(jsonKeyPathToColumnNameMap.get(field), schema);
    }

    return rootNode;
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

      // Generate merged text index
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
   * pairs with the corresponding schema tree node and {#link SchemaConformingTransformerV2Config}. Finally drop or put
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
