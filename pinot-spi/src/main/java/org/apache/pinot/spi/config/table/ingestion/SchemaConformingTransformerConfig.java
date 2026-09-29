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
package org.apache.pinot.spi.config.table.ingestion;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class SchemaConformingTransformerConfig extends BaseJsonConfig {
  @JsonPropertyDescription("Enable indexable extras")
  private boolean _enableIndexableExtras = true;

  @JsonPropertyDescription("Name of the field that should contain extra fields that are not part of the schema.")
  private String _indexableExtrasField = "json_data";

  @JsonPropertyDescription("Enable unindexable extras")
  private boolean _enableUnindexableExtras = true;

  @JsonPropertyDescription(
      "Like indexableExtrasField except it only contains fields with the suffix in unindexableFieldSuffix.")
  private String _unindexableExtrasField = "json_data_no_idx";

  @JsonPropertyDescription("The suffix of fields that must be stored in unindexableExtrasField")
  private String _unindexableFieldSuffix = "_noindex";

  @JsonPropertyDescription("Array of flattened (dot-delimited) object paths to drop")
  private Set<String> _fieldPathsToDrop = new HashSet<>();

  @JsonPropertyDescription("Array of flattened (dot-delimited) object paths not to traverse further and keep same as "
      + "input. This will also skip building mergedTextIndex for the field.")
  private Set<String> _fieldPathsToPreserveInput = new HashSet<>();

  @JsonPropertyDescription("Array of flattened (dot-delimited) object paths not to traverse further and keep same as "
      + "input. This will NOT skip building mergedTextIndex for the field.")
  private Set<String> _fieldPathsToPreserveInputWithIndex = new HashSet<>();

  @JsonPropertyDescription("Array of flattened (dot-delimited) object paths not to store but only build "
      + "mergedTextIndex for the field.")
  private Set<String> _fieldPathsToSkipStorage = Set.of("message");

  @JsonPropertyDescription("Map from customized meaningful column name to json key path")
  private Map<String, String> _columnNameToJsonKeyPathMap = new HashMap<>();

  @JsonPropertyDescription("mergedTextIndex field")
  private String _mergedTextIndexField = "__mergedTextIndex";

  @JsonPropertyDescription(
      "If set to true {'a.b': 'c'} will be indexed in the same way as {'a': {'b': 'c}}. Otherwise, "
          + "the former one will be ignored.")
  private Boolean _useAnonymousDotInFieldNames = true;

  @JsonPropertyDescription("Whether to store extra lower cases value:key pairs in __mergedTextIndex to optimize case "
      + "insensitive queries")
  private Boolean _optimizeCaseInsensitiveSearch = false;

  @JsonPropertyDescription("Whether to store key and value in reverse order, if true store as value:key, else store"
      + " as key:value")
  private Boolean _reverseTextIndexKeyValueOrder = true;

  @JsonPropertyDescription("mergedTextIndex document max length")
  private int _mergedTextIndexDocumentMaxLength = 32766;

  @JsonPropertyDescription("mergedTextIndex binary document detection minimum length")
  private Integer _mergedTextIndexBinaryDocumentDetectionMinLength = 512;

  @JsonPropertyDescription("Array of paths to exclude from merged text index.")
  private Set<String> _mergedTextIndexPathToExclude = new HashSet<>();

  @JsonPropertyDescription("Array of flattened (dot-delimited) path prefix to exclude from merged text index.")
  private Set<String> _mergedTextIndexPrefixToExclude = new HashSet<>();

  @JsonPropertyDescription("Anchor before merged text index value. Default is empty String")
  private String _mergedTextIndexBeginOfDocAnchor = "";

  @JsonPropertyDescription("Anchor after merged text index value. Default is empty String")
  private String _mergedTextIndexEndOfDocAnchor = "";

  @JsonPropertyDescription("Dedicated fields to double ingest into json_data column")
  private Set<String> _fieldsToDoubleIngest = new HashSet<>();

  @JsonPropertyDescription("Separator between key and value in json used in the Lucene index. Default is ':'.")
  private String _jsonKeyValueSeparator = ":";

  public SchemaConformingTransformerConfig() {
    // Default constructor
  }

  @JsonCreator
  public SchemaConformingTransformerConfig(
      @JsonProperty("enableIndexableExtras") @Nullable Boolean enableIndexableExtras,
      @JsonProperty("indexableExtrasField") @Nullable String indexableExtrasField,
      @JsonProperty("enableUnindexableExtras") @Nullable Boolean enableUnindexableExtras,
      @JsonProperty("unindexableExtrasField") @Nullable String unindexableExtrasField,
      @JsonProperty("unindexableFieldSuffix") @Nullable String unindexableFieldSuffix,
      @JsonProperty("fieldPathsToDrop") @Nullable Set<String> fieldPathsToDrop,
      @JsonProperty("fieldPathsToKeepSameAsInput") @Nullable Set<String> fieldPathsToPreserveInput,
      @JsonProperty("fieldPathsToKeepSameAsInputWithIndex") @Nullable Set<String> fieldPathsToPreserveInputWithIndex,
      @JsonProperty("fieldPathsToSkipStorage") @Nullable Set<String> fieldPathsToSkipStorage,
      @JsonProperty("columnNameToJsonKeyPathMap") @Nullable Map<String, String> columnNameToJsonKeyPathMap,
      @JsonProperty("mergedTextIndexField") @Nullable String mergedTextIndexFields,
      @JsonProperty("useAnonymousDotInFieldNames") @Nullable Boolean useAnonymousDotInFieldNames,
      @JsonProperty("optimizeCaseInsensitiveSearch") @Nullable Boolean optimizeCaseInsensitiveSearch,
      @JsonProperty("reverseTextIndexKeyValueOrder") @Nullable Boolean reverseTextIndexKeyValueOrder,
      @JsonProperty("mergedTextIndexDocumentMaxLength") @Nullable Integer mergedTextIndexDocumentMaxLength,
      @JsonProperty("mergedTextIndexBinaryTokenDetectionMinLength")
      @Nullable Integer mergedTextIndexBinaryTokenDetectionMinLength, // Deprecated, add it to be backward compatible
      @JsonProperty("mergedTextIndexBinaryDocumentDetectionMinLength")
      @Nullable Integer mergedTextIndexBinaryDocumentDetectionMinLength,
      @JsonProperty("mergedTextIndexPathToExclude") @Nullable Set<String> mergedTextIndexPathToExclude,
      @JsonProperty("mergedTextIndexPrefixToExclude") @Nullable Set<String> mergedTextIndexPrefixToExclude,
      @JsonProperty("fieldsToDoubleIngest") @Nullable Set<String> fieldsToDoubleIngest,
      @JsonProperty("jsonKeyValueSeparator") @Nullable String jsonKeyValueSeparator,
      @JsonProperty("mergedTextIndexBeginOfDocAnchor") @Nullable String mergedTextIndexBeginOfDocAnchor,
      @JsonProperty("mergedTextIndexEndOfDocAnchor") @Nullable String mergedTextIndexEndOfDocAnchor
  ) {
    setEnableIndexableExtras(enableIndexableExtras);
    setIndexableExtrasField(indexableExtrasField);
    setEnableUnindexableExtras(enableUnindexableExtras);
    setUnindexableExtrasField(unindexableExtrasField);
    setUnindexableFieldSuffix(unindexableFieldSuffix);
    setFieldPathsToDrop(fieldPathsToDrop);
    setFieldPathsToPreserveInput(fieldPathsToPreserveInput);
    setFieldPathsToPreserveInputWithIndex(fieldPathsToPreserveInputWithIndex);
    setFieldPathsToSkipStorage(fieldPathsToSkipStorage);
    setColumnNameToJsonKeyPathMap(columnNameToJsonKeyPathMap);

    setMergedTextIndexField(mergedTextIndexFields);
    setUseAnonymousDotInFieldNames(useAnonymousDotInFieldNames);
    setOptimizeCaseInsensitiveSearch(optimizeCaseInsensitiveSearch);
    setReverseTextIndexKeyValueOrder(reverseTextIndexKeyValueOrder);
    setMergedTextIndexDocumentMaxLength(mergedTextIndexDocumentMaxLength);
    mergedTextIndexBinaryDocumentDetectionMinLength = mergedTextIndexBinaryDocumentDetectionMinLength == null
        ? mergedTextIndexBinaryTokenDetectionMinLength : mergedTextIndexBinaryDocumentDetectionMinLength;
    setMergedTextIndexBinaryDocumentDetectionMinLength(mergedTextIndexBinaryDocumentDetectionMinLength);
    setMergedTextIndexPathToExclude(mergedTextIndexPathToExclude);
    setMergedTextIndexPrefixToExclude(mergedTextIndexPrefixToExclude);
    setFieldsToDoubleIngest(fieldsToDoubleIngest);
    setJsonKeyValueSeparator(jsonKeyValueSeparator);
    setMergedTextIndexBeginOfDocAnchor(mergedTextIndexBeginOfDocAnchor);
    setMergedTextIndexEndOfDocAnchor(mergedTextIndexEndOfDocAnchor);
  }

  public Boolean isEnableIndexableExtras() {
    return _enableIndexableExtras;
  }

  public SchemaConformingTransformerConfig setEnableIndexableExtras(Boolean enableIndexableExtras) {
    _enableIndexableExtras = enableIndexableExtras == null ? _enableIndexableExtras : enableIndexableExtras;
    return this;
  }

  public String getIndexableExtrasField() {
    return _enableIndexableExtras ? _indexableExtrasField : null;
  }

  public SchemaConformingTransformerConfig setIndexableExtrasField(String indexableExtrasField) {
    _indexableExtrasField = indexableExtrasField == null ? _indexableExtrasField : indexableExtrasField;
    return this;
  }

  public Boolean isEnableUnindexableExtras() {
    return _enableUnindexableExtras;
  }

  public SchemaConformingTransformerConfig setEnableUnindexableExtras(Boolean enableUnindexableExtras) {
    _enableUnindexableExtras = enableUnindexableExtras == null ? _enableUnindexableExtras : enableUnindexableExtras;
    return this;
  }

  public String getUnindexableExtrasField() {
    return _enableUnindexableExtras ? _unindexableExtrasField : null;
  }

  public SchemaConformingTransformerConfig setUnindexableExtrasField(String unindexableExtrasField) {
    _unindexableExtrasField = unindexableExtrasField == null ? _unindexableExtrasField : unindexableExtrasField;
    return this;
  }

  public String getUnindexableFieldSuffix() {
    return _unindexableFieldSuffix;
  }

  public SchemaConformingTransformerConfig setUnindexableFieldSuffix(String unindexableFieldSuffix) {
    _unindexableFieldSuffix = unindexableFieldSuffix == null ? _unindexableFieldSuffix : unindexableFieldSuffix;
    return this;
  }

  public Set<String> getFieldPathsToDrop() {
    return _fieldPathsToDrop;
  }

  public SchemaConformingTransformerConfig setFieldPathsToDrop(Set<String> fieldPathsToDrop) {
    _fieldPathsToDrop = fieldPathsToDrop == null ? _fieldPathsToDrop : fieldPathsToDrop;
    return this;
  }

  public Set<String> getFieldPathsToPreserveInput() {
    return _fieldPathsToPreserveInput;
  }

  public SchemaConformingTransformerConfig setFieldPathsToPreserveInput(Set<String> fieldPathsToPreserveInput) {
    _fieldPathsToPreserveInput = fieldPathsToPreserveInput == null ? _fieldPathsToPreserveInput
        : fieldPathsToPreserveInput;
    return this;
  }

  public Set<String> getFieldPathsToSkipStorage() {
    return _fieldPathsToSkipStorage;
  }

  public SchemaConformingTransformerConfig setFieldPathsToSkipStorage(Set<String> fieldPathsToSkipStorage) {
    _fieldPathsToSkipStorage = fieldPathsToSkipStorage == null ? _fieldPathsToSkipStorage : fieldPathsToSkipStorage;
    return this;
  }

  public Set<String> getFieldPathsToPreserveInputWithIndex() {
    return _fieldPathsToPreserveInputWithIndex;
  }

  public SchemaConformingTransformerConfig setFieldPathsToPreserveInputWithIndex(
      Set<String> fieldPathsToPreserveInputWithIndex) {
    _fieldPathsToPreserveInputWithIndex =
        fieldPathsToPreserveInputWithIndex == null ? _fieldPathsToPreserveInputWithIndex
            : fieldPathsToPreserveInputWithIndex;
    return this;
  }

  public Map<String, String> getColumnNameToJsonKeyPathMap() {
    return _columnNameToJsonKeyPathMap;
  }

  public SchemaConformingTransformerConfig setColumnNameToJsonKeyPathMap(
      Map<String, String> columnNameToJsonKeyPathMap) {
    _columnNameToJsonKeyPathMap = columnNameToJsonKeyPathMap == null
        ? _columnNameToJsonKeyPathMap : columnNameToJsonKeyPathMap;
    return this;
  }

  public String getMergedTextIndexField() {
    return _mergedTextIndexField;
  }

  public SchemaConformingTransformerConfig setMergedTextIndexField(String mergedTextIndexField) {
    _mergedTextIndexField = mergedTextIndexField == null ? _mergedTextIndexField : mergedTextIndexField;
    return this;
  }

  public Boolean isUseAnonymousDotInFieldNames() {
    return _useAnonymousDotInFieldNames;
  }

  public SchemaConformingTransformerConfig setUseAnonymousDotInFieldNames(Boolean useAnonymousDotInFieldNames) {
    _useAnonymousDotInFieldNames = useAnonymousDotInFieldNames == null ? _useAnonymousDotInFieldNames
        : useAnonymousDotInFieldNames;
    return this;
  }

  public Boolean isOptimizeCaseInsensitiveSearch() {
    return _optimizeCaseInsensitiveSearch;
  }

  public SchemaConformingTransformerConfig setOptimizeCaseInsensitiveSearch(Boolean optimizeCaseInsensitiveSearch) {
    _optimizeCaseInsensitiveSearch = optimizeCaseInsensitiveSearch == null ? _optimizeCaseInsensitiveSearch
        : optimizeCaseInsensitiveSearch;
    return this;
  }

  public Boolean isReverseTextIndexKeyValueOrder() {
    return _reverseTextIndexKeyValueOrder;
  }

  public SchemaConformingTransformerConfig setReverseTextIndexKeyValueOrder(Boolean reverseTextIndexKeyValueOrder) {
    _reverseTextIndexKeyValueOrder = reverseTextIndexKeyValueOrder == null ? _reverseTextIndexKeyValueOrder
        : reverseTextIndexKeyValueOrder;
    return this;
  }

  public Integer getMergedTextIndexDocumentMaxLength() {
    return _mergedTextIndexDocumentMaxLength;
  }

  public SchemaConformingTransformerConfig setMergedTextIndexDocumentMaxLength(
      Integer mergedTextIndexDocumentMaxLength
  ) {
    _mergedTextIndexDocumentMaxLength = mergedTextIndexDocumentMaxLength == null
        ? _mergedTextIndexDocumentMaxLength : mergedTextIndexDocumentMaxLength;
    return this;
  }

  public Integer getMergedTextIndexBinaryDocumentDetectionMinLength() {
    return _mergedTextIndexBinaryDocumentDetectionMinLength;
  }

  public SchemaConformingTransformerConfig setMergedTextIndexBinaryDocumentDetectionMinLength(
      Integer mergedTextIndexBinaryDocumentDetectionMinLength) {
    _mergedTextIndexBinaryDocumentDetectionMinLength = mergedTextIndexBinaryDocumentDetectionMinLength == null
        ? _mergedTextIndexBinaryDocumentDetectionMinLength : mergedTextIndexBinaryDocumentDetectionMinLength;
    return this;
  }

  public Set<String> getMergedTextIndexPathToExclude() {
    return _mergedTextIndexPathToExclude;
  }

  public SchemaConformingTransformerConfig setMergedTextIndexPathToExclude(Set<String> mergedTextIndexPathToExclude) {
    _mergedTextIndexPathToExclude = mergedTextIndexPathToExclude == null
        ? _mergedTextIndexPathToExclude : mergedTextIndexPathToExclude;
    return this;
  }

  public Set<String> getMergedTextIndexPrefixToExclude() {
    return _mergedTextIndexPrefixToExclude;
  }

  public SchemaConformingTransformerConfig setMergedTextIndexPrefixToExclude(
      Set<String> mergedTextIndexPrefixToExclude) {
    _mergedTextIndexPrefixToExclude = mergedTextIndexPrefixToExclude == null
        ? _mergedTextIndexPrefixToExclude : mergedTextIndexPrefixToExclude;
    return this;
  }

  public Set<String> getFieldsToDoubleIngest() {
    return _fieldsToDoubleIngest;
  }

  public SchemaConformingTransformerConfig setFieldsToDoubleIngest(Set<String> fieldsToDoubleIngest) {
    _fieldsToDoubleIngest = fieldsToDoubleIngest == null ? _fieldsToDoubleIngest : fieldsToDoubleIngest;
    return this;
  }

  public String getJsonKeyValueSeparator() {
    return _jsonKeyValueSeparator;
  }

  public void setJsonKeyValueSeparator(@Nullable String jsonKeyValueSeparator) {
    _jsonKeyValueSeparator = jsonKeyValueSeparator == null ? ":" : jsonKeyValueSeparator;
  }

  public String getMergedTextIndexBeginOfDocAnchor() {
    return _mergedTextIndexBeginOfDocAnchor;
  }

  public SchemaConformingTransformerConfig setMergedTextIndexBeginOfDocAnchor(
      String mergedTextIndexBeginOfDocAnchor) {
    _mergedTextIndexBeginOfDocAnchor = mergedTextIndexBeginOfDocAnchor == null
        ? _mergedTextIndexBeginOfDocAnchor : mergedTextIndexBeginOfDocAnchor;
    return this;
  }

  public String getMergedTextIndexEndOfDocAnchor() {
    return _mergedTextIndexEndOfDocAnchor;
  }

  public SchemaConformingTransformerConfig setMergedTextIndexEndOfDocAnchor(String mergedTextIndexEndOfDocAnchor) {
    _mergedTextIndexEndOfDocAnchor = mergedTextIndexEndOfDocAnchor == null
        ? _mergedTextIndexEndOfDocAnchor : mergedTextIndexEndOfDocAnchor;
    return this;
  }
}
