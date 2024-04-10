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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;

public class SchemaConformingTransformerV2Config extends BaseJsonConfig {
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

  @JsonPropertyDescription("Map from customized meaningful column name to json key path")
  private Map<String, String> _columnNameToJsonKeyPathMap = new HashMap<>();

  @JsonPropertyDescription("mergedTextIndex field")
  private String _mergedTextIndexField = "__mergedTextIndex";

  @JsonPropertyDescription("mergedTextIndex document max length")
  private int _mergedTextIndexDocumentMaxLength = 32766;

  @JsonPropertyDescription(
      "Recall that merged text index document is in the format of <value:key>. "
          + "The mergedTextIndex shingling overlap length refers to the "
          + "maximum search length of the value that will yield results with "
          + "100% accuracy. If the value is null, shingle index will be turned off "
          + "and the value will be truncated such that the document is equal to "
          + "_mergedTextIndexDocumentMaxLength"
  )
  private @Nullable Integer _mergedTextIndexShinglingOverlapLength = null;

  @JsonPropertyDescription("mergedTextIndex binary document detection minimum length")
  private Integer _mergedTextIndexBinaryDocumentDetectionMinLength = 512;

  @JsonPropertyDescription("Array of paths to exclude from merged text index.")
  private Set<String> _mergedTextIndexPathToExclude = new HashSet<>();

  // TODO: set default value from CLPRewriter once it open sourced
  @JsonPropertyDescription("Array of suffix to exclude from merged text index.")
  private List<String> _mergedTextIndexSuffixToExclude = Arrays.asList("_logtype", "_dictionaryVars", "_encodedVars");

  @JsonPropertyDescription("Dedicated fields to double ingest into json_data column")
  private Set<String> _fieldsToDoubleIngest = new HashSet<>();

  @JsonCreator
  public SchemaConformingTransformerV2Config(
      @JsonProperty("enableIndexableExtras") @Nullable Boolean enableIndexableExtras,
      @JsonProperty("indexableExtrasField") String indexableExtrasField,
      @JsonProperty("enableUnindexableExtras") @Nullable Boolean enableUnindexableExtras,
      @JsonProperty("unindexableExtrasField") @Nullable String unindexableExtrasField,
      @JsonProperty("unindexableFieldSuffix") @Nullable String unindexableFieldSuffix,
      @JsonProperty("fieldPathsToDrop") @Nullable Set<String> fieldPathsToDrop,
      @JsonProperty("fieldPathsToKeepSameAsInput") @Nullable Set<String> fieldPathsToPreserveInput,
      @JsonProperty("mergedTextIndexField") @Nullable String mergedTextIndexField,
      @JsonProperty("mergedTextIndexDocumentMaxLength") @Nullable Integer mergedTextIndexDocumentMaxLength,
      @JsonProperty("mergedTextIndexShinglingOverlapLength") @Nullable Integer mergedTextIndexShinglingOverlapLength,
      @JsonProperty("mergedTextIndexBinaryDocumentDetectionMinLength")
      @Nullable Integer mergedTextIndexBinaryDocumentDetectionMinLength,
      @JsonProperty("mergedTextIndexPathToExclude") @Nullable Set<String> mergedTextIndexPathToExclude,
      @JsonProperty("fieldsToDoubleIngest") @Nullable Set<String> fieldsToDoubleIngest
  ) {
    setEnableIndexableExtras(enableIndexableExtras);
    setIndexableExtrasField(indexableExtrasField);
    setEnableUnindexableExtras(enableUnindexableExtras);
    setUnindexableExtrasField(unindexableExtrasField);
    setUnindexableFieldSuffix(unindexableFieldSuffix);
    setFieldPathsToDrop(fieldPathsToDrop);
    setFieldPathsToPreserveInput(fieldPathsToPreserveInput);

    setMergedTextIndexField(mergedTextIndexField);
    setMergedTextIndexDocumentMaxLength(mergedTextIndexDocumentMaxLength);
    setMergedTextIndexShinglingDocumentOverlapLength(mergedTextIndexShinglingOverlapLength);
    setMergedTextIndexBinaryDocumentDetectionMinLength(mergedTextIndexBinaryDocumentDetectionMinLength);
    setMergedTextIndexPathToExclude(mergedTextIndexPathToExclude);
    setFieldsToDoubleIngest(fieldsToDoubleIngest);
  }

  public SchemaConformingTransformerV2Config setEnableIndexableExtras(Boolean enableIndexableExtras) {
    _enableIndexableExtras = enableIndexableExtras == null ? _enableUnindexableExtras : enableIndexableExtras;
    return this;
  }

  public String getIndexableExtrasField() {
    return _enableIndexableExtras ? _indexableExtrasField : null;
  }

  public SchemaConformingTransformerV2Config setIndexableExtrasField(String indexableExtrasField) {
    _indexableExtrasField = indexableExtrasField == null ? _indexableExtrasField : indexableExtrasField;
    return this;
  }

  public SchemaConformingTransformerV2Config setEnableUnindexableExtras(Boolean enableUnindexableExtras) {
    _enableUnindexableExtras = enableUnindexableExtras == null ? _enableUnindexableExtras : enableUnindexableExtras;
    return this;
  }

  public String getUnindexableExtrasField() {
    return _enableUnindexableExtras ? _unindexableExtrasField : null;
  }

  public SchemaConformingTransformerV2Config setUnindexableExtrasField(String unindexableExtrasField) {
    _unindexableExtrasField = unindexableExtrasField == null ? _unindexableExtrasField : unindexableExtrasField;
    return this;
  }

  public String getUnindexableFieldSuffix() {
    return _unindexableFieldSuffix;
  }

  public SchemaConformingTransformerV2Config setUnindexableFieldSuffix(String unindexableFieldSuffix) {
    _unindexableFieldSuffix = unindexableFieldSuffix == null ? _unindexableFieldSuffix : unindexableFieldSuffix;
    return this;
  }

  public Set<String> getFieldPathsToDrop() {
    return _fieldPathsToDrop;
  }

  public SchemaConformingTransformerV2Config setFieldPathsToDrop(Set<String> fieldPathsToDrop) {
    _fieldPathsToDrop = fieldPathsToDrop == null ? _fieldPathsToDrop : fieldPathsToDrop;
    return this;
  }

  public Set<String> getFieldPathsToPreserveInput() {
    return _fieldPathsToPreserveInput;
  }

  public SchemaConformingTransformerV2Config setFieldPathsToPreserveInput(Set<String> fieldPathsToPreserveInput) {
    _fieldPathsToPreserveInput = fieldPathsToPreserveInput == null ? _fieldPathsToPreserveInput
        : fieldPathsToPreserveInput;
    return this;
  }

  public Map<String, String> getColumnNameToJsonKeyPathMap() {
    return _columnNameToJsonKeyPathMap;
  }

  public SchemaConformingTransformerV2Config setColumnNameToJsonKeyPathMap(
      Map<String, String> columnNameToJsonKeyPathMap) {
    _columnNameToJsonKeyPathMap = columnNameToJsonKeyPathMap == null
        ? _columnNameToJsonKeyPathMap : columnNameToJsonKeyPathMap;
    return this;
  }

  public String getMergedTextIndexField() {
    return _mergedTextIndexField;
  }

  public SchemaConformingTransformerV2Config setMergedTextIndexField(String mergedTextIndexField) {
    _mergedTextIndexField = mergedTextIndexField == null ? _mergedTextIndexField : mergedTextIndexField;
    return this;
  }

  public Integer getMergedTextIndexDocumentMaxLength() {
    return _mergedTextIndexDocumentMaxLength;
  }

  public SchemaConformingTransformerV2Config setMergedTextIndexDocumentMaxLength(
      Integer mergedTextIndexDocumentMaxLength
  ) {
    _mergedTextIndexDocumentMaxLength = mergedTextIndexDocumentMaxLength == null
        ? _mergedTextIndexDocumentMaxLength : mergedTextIndexDocumentMaxLength;
    return this;
  }

  public Integer getMergedTextIndexShinglingOverlapLength() {
    return _mergedTextIndexShinglingOverlapLength;
  }

  public SchemaConformingTransformerV2Config setMergedTextIndexShinglingDocumentOverlapLength(
      Integer mergedTextIndexShinglingOverlapLength) {
    _mergedTextIndexShinglingOverlapLength = mergedTextIndexShinglingOverlapLength;
    return this;
  }

  public Integer getMergedTextIndexBinaryDocumentDetectionMinLength() {
    return _mergedTextIndexBinaryDocumentDetectionMinLength;
  }

  public SchemaConformingTransformerV2Config setMergedTextIndexBinaryDocumentDetectionMinLength(
      Integer mergedTextIndexBinaryDocumentDetectionMinLength) {
    _mergedTextIndexBinaryDocumentDetectionMinLength = mergedTextIndexBinaryDocumentDetectionMinLength == null
        ? _mergedTextIndexBinaryDocumentDetectionMinLength : mergedTextIndexBinaryDocumentDetectionMinLength;
    return this;
  }

  public Set<String> getMergedTextIndexPathToExclude() {
    return _mergedTextIndexPathToExclude;
  }

  public List<String> getMergedTextIndexSuffixToExclude() {
    return _mergedTextIndexSuffixToExclude;
  }

  public SchemaConformingTransformerV2Config setMergedTextIndexPathToExclude(Set<String> mergedTextIndexPathToExclude) {
    _mergedTextIndexPathToExclude = mergedTextIndexPathToExclude == null
        ? _mergedTextIndexPathToExclude : mergedTextIndexPathToExclude;
    return this;
  }

  public Set<String> getFieldsToDoubleIngest() {
    return _fieldsToDoubleIngest;
  }

  public SchemaConformingTransformerV2Config setFieldsToDoubleIngest(Set<String> fieldsToDoubleIngest) {
    _fieldsToDoubleIngest = fieldsToDoubleIngest == null ? _fieldsToDoubleIngest : fieldsToDoubleIngest;
    return this;
  }
}
