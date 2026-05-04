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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class FieldConfig extends BaseJsonConfig {
  public static final String BLOOM_FILTER_COLUMN_KEY = "createBloomFilter";
  public static final String ON_HEAP_DICTIONARY_COLUMN_KEY = "useOnHeapDictionary";
  public static final String VAR_LENGTH_DICTIONARY_COLUMN_KEY = "useVarLengthDictionary";
  public static final String DERIVE_NUM_DOCS_PER_CHUNK_RAW_INDEX_KEY = "deriveNumDocsPerChunkForRawIndex";
  public static final String RAW_INDEX_WRITER_VERSION = "rawIndexWriterVersion";
  public static final String IS_SEGMENT_PARTITIONED_COLUMN_KEY = "isSegmentPartitioned";

  public static final String TEXT_INDEX_REALTIME_READER_REFRESH_KEY = "textIndexRealtimeReaderRefreshThreshold";
  // Lucene creates a query result cache if this option is enabled
  // the cache improves performance of repeatable queries
  public static final String TEXT_INDEX_ENABLE_QUERY_CACHE = "enableQueryCacheForTextIndex";
  public static final String TEXT_INDEX_USE_AND_FOR_MULTI_TERM_QUERIES = "useANDForMultiTermTextIndexQueries";
  public static final String TEXT_INDEX_NO_RAW_DATA = "noRawDataForTextIndex";
  public static final String TEXT_INDEX_RAW_VALUE = "rawValueForTextIndex";
  public static final String TEXT_INDEX_DEFAULT_RAW_VALUE = "n";
  public static final String TEXT_INDEX_STOP_WORD_INCLUDE_KEY = "stopWordInclude";
  public static final String TEXT_INDEX_STOP_WORD_EXCLUDE_KEY = "stopWordExclude";
  public static final String TEXT_INDEX_CASE_SENSITIVE_KEY = "caseSensitive";
  public static final String TEXT_INDEX_LUCENE_USE_COMPOUND_FILE = "luceneUseCompoundFile";
  public static final String TEXT_INDEX_LUCENE_MAX_BUFFER_SIZE_MB = "luceneMaxBufferSizeMB";
  public static final String TEXT_INDEX_LUCENE_ANALYZER_CLASS = "luceneAnalyzerClass";
  public static final String TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARGS = "luceneAnalyzerClassArgs";
  public static final String TEXT_INDEX_LUCENE_ANALYZER_CLASS_ARG_TYPES = "luceneAnalyzerClassArgTypes";
  public static final String TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS = "luceneQueryParserClass";
  public static final String TEXT_INDEX_LUCENE_USE_LBS_MERGE_POLICY = "luceneUseLogByteSizeMergePolicy";
  public static final String TEXT_INDEX_LUCENE_DOC_ID_TRANSLATOR_MODE = "luceneDocIdTranslatorMode";
  public static final String TEXT_INDEX_DEFAULT_LUCENE_ANALYZER_CLASS =
      "org.apache.lucene.analysis.standard.StandardAnalyzer";
  public static final String TEXT_INDEX_DEFAULT_LUCENE_QUERY_PARSER_CLASS =
      "org.apache.lucene.queryparser.classic.QueryParser";
  public static final String TEXT_INDEX_STOP_WORD_SEPERATOR = ",";
  // Config to disable forward index
  public static final String FORWARD_INDEX_DISABLED = "forwardIndexDisabled";
  public static final String DEFAULT_FORWARD_INDEX_DISABLED = Boolean.FALSE.toString();
  public static final String TEXT_INDEX_ENABLE_PREFIX_SUFFIX_PHRASE_QUERIES =
      "enablePrefixSuffixMatchingInPhraseQueries";
  public static final String TEXT_INDEX_LUCENE_REUSE_MUTABLE_INDEX = "reuseMutableIndex";
  public static final String TEXT_INDEX_LUCENE_NRT_CACHING_DIRECTORY_BUFFER_SIZE =
      "luceneNRTCachingDirectoryMaxBufferSizeMB";
  private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.instance;

  private final String _name;
  private final EncodingType _encodingType;
  private final JsonNode _indexes;
  private final JsonNode _tierOverwrites;
  private final CompressionCodec _compressionCodec;
  private final Map<String, String> _properties;
  private final TimestampConfig _timestampConfig;

  @Deprecated
  public FieldConfig(String name, EncodingType encodingType, @Nullable IndexType indexType,
      @Nullable CompressionCodec compressionCodec, @Nullable Map<String, String> properties) {
    this(name, encodingType, indexType, null, compressionCodec, null, null, properties, null);
  }

  public FieldConfig(String name, EncodingType encodingType, @Nullable List<IndexType> indexTypes,
      @Nullable CompressionCodec compressionCodec, @Nullable Map<String, String> properties) {
    this(name, encodingType, null, indexTypes, compressionCodec, null, null, properties, null);
  }

  @Deprecated
  public FieldConfig(String name, EncodingType encodingType, @Nullable IndexType indexType,
      @Nullable List<IndexType> indexTypes, @Nullable CompressionCodec compressionCodec,
      @Nullable TimestampConfig timestampConfig, @Nullable Map<String, String> properties) {
    this(name, encodingType, indexType, indexTypes, compressionCodec, timestampConfig, null, properties, null);
  }

  @JsonCreator
  public FieldConfig(@JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = "encodingType") EncodingType encodingType,
      @JsonProperty(value = "indexType") @Nullable IndexType indexType,
      @JsonProperty(value = "indexTypes") @Nullable List<IndexType> indexTypes,
      @JsonProperty(value = "compressionCodec") @Nullable CompressionCodec compressionCodec,
      @JsonProperty(value = "timestampConfig") @Nullable TimestampConfig timestampConfig,
      @JsonProperty(value = "indexes") @Nullable JsonNode indexes,
      @JsonProperty(value = "properties") @Nullable Map<String, String> properties,
      @JsonProperty(value = "tierOverwrites") @Nullable JsonNode tierOverwrites) {
    Preconditions.checkArgument(name != null, "'name' must be configured");
    _name = name;
    _encodingType = encodingType == null ? EncodingType.DICTIONARY : encodingType;
    _compressionCodec = compressionCodec;
    _timestampConfig = timestampConfig;
    _properties = properties;
    _indexes = normalizeIndexes(name, indexType, indexTypes, indexes);
    _tierOverwrites = tierOverwrites == null ? NullNode.getInstance() : tierOverwrites;
  }

  // If null, we will create dictionary encoded forward index by default
  public enum EncodingType {
    RAW, DICTIONARY
  }

  // If null, there won't be any index
  // NOTE: TIMESTAMP is ignored. In order to create TIMESTAMP index, configure 'timestampConfig' instead.
  public enum IndexType {
    INVERTED("inverted"),
    SORTED("sorted"),
    TEXT("text"),
    FST("fst"),
    IFST("ifst"),
    H3("h3"),
    JSON("json"),
    TIMESTAMP("timestamp"),
    VECTOR("vector"),
    RANGE("range");

    private final String _prettyName;

    IndexType(String prettyName) {
      _prettyName = prettyName;
    }

    public String getPrettyName() {
      return _prettyName;
    }
  }

  public enum CompressionCodec {
    //@formatter:off
    PASS_THROUGH(true, false),
    SNAPPY(true, false),
    ZSTANDARD(true, false),
    LZ4(true, false),
    GZIP(true, false),

    // For MV dictionary encoded forward index, add a second level dictionary encoding for the multi-value entries
    MV_ENTRY_DICT(false, true),

    // CLP is a special type of compression codec that isn't generally applicable to all RAW columns and has a special
    // handling for log lines (see {@link CLPForwardIndexCreatorV1} and {@link CLPForwardIndexCreatorV2)
    CLP(false, false),
    CLPV2(false, false),
    CLPV2_ZSTD(false, false),
    CLPV2_LZ4(false, false),

    DELTA(false, false),
    DELTADELTA(false, false);

    //@formatter:on

    private final boolean _applicableToRawIndex;
    private final boolean _applicableToDictEncodedIndex;

    CompressionCodec(boolean applicableToRawIndex, boolean applicableToDictEncodedIndex) {
      _applicableToRawIndex = applicableToRawIndex;
      _applicableToDictEncodedIndex = applicableToDictEncodedIndex;
    }

    public boolean isApplicableToRawIndex() {
      return _applicableToRawIndex;
    }

    public boolean isApplicableToDictEncodedIndex() {
      return _applicableToDictEncodedIndex;
    }
  }

  public String getName() {
    return _name;
  }

  public EncodingType getEncodingType() {
    return _encodingType;
  }

  @Nullable
  @Deprecated
  @JsonIgnore
  public IndexType getIndexType() {
    List<IndexType> indexTypes = getIndexTypes();
    return indexTypes.isEmpty() ? null : indexTypes.get(0);
  }

  @Deprecated
  @JsonIgnore
  public List<IndexType> getIndexTypes() {
    if (!_indexes.isObject()) {
      return Collections.emptyList();
    }

    List<IndexType> indexTypes = new ArrayList<>();
    ObjectNode indexes = (ObjectNode) _indexes;
    for (IndexType indexType : IndexType.values()) {
      JsonNode indexConfig = indexes.get(indexType.getPrettyName());
      if (indexConfig != null && !indexConfig.path("disabled").asBoolean(false)) {
        indexTypes.add(indexType);
      }
    }
    return indexTypes;
  }

  public JsonNode getIndexes() {
    return _indexes;
  }

  public JsonNode getTierOverwrites() {
    return _tierOverwrites;
  }

  @Nullable
  public CompressionCodec getCompressionCodec() {
    return _compressionCodec;
  }

  @Nullable
  public TimestampConfig getTimestampConfig() {
    return _timestampConfig;
  }

  @Nullable
  public Map<String, String> getProperties() {
    return _properties;
  }

  public static class Builder {
    private String _name;
    private EncodingType _encodingType;
    private List<IndexType> _indexTypes;
    private JsonNode _indexes;
    private CompressionCodec _compressionCodec;
    private Map<String, String> _properties;
    private TimestampConfig _timestampConfig;
    private JsonNode _tierOverwrites;

    public Builder(String name) {
      _name = name;
    }

    public Builder(FieldConfig other) {
      _name = other._name;
      _encodingType = other._encodingType;
      _indexes = other._indexes;
      _compressionCodec = other._compressionCodec;
      _properties = other._properties;
      _timestampConfig = other._timestampConfig;
      _tierOverwrites = other._tierOverwrites;
    }

    public Builder withIndexes(JsonNode indexes) {
      _indexes = indexes;
      return this;
    }

    public Builder withName(String name) {
      _name = name;
      return this;
    }

    public Builder withEncodingType(EncodingType encodingType) {
      _encodingType = encodingType;
      return this;
    }

    public Builder withIndexTypes(List<IndexType> indexTypes) {
      _indexTypes = indexTypes;
      return this;
    }

    public Builder withCompressionCodec(CompressionCodec compressionCodec) {
      _compressionCodec = compressionCodec;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      _properties = properties;
      return this;
    }

    public Builder withTimestampConfig(TimestampConfig timestampConfig) {
      _timestampConfig = timestampConfig;
      return this;
    }

    public Builder withTierOverwrites(JsonNode tierOverwrites) {
      _tierOverwrites = tierOverwrites;
      return this;
    }

    public FieldConfig build() {
      return new FieldConfig(_name, _encodingType, null, _indexTypes, _compressionCodec, _timestampConfig, _indexes,
          _properties, _tierOverwrites);
    }
  }

  private static JsonNode normalizeIndexes(String fieldName, @Nullable IndexType indexType,
      @Nullable List<IndexType> indexTypes, @Nullable JsonNode indexes) {
    Set<IndexType> legacyIndexTypes = collectLegacyIndexTypes(indexType, indexTypes);
    if (legacyIndexTypes.isEmpty()) {
      return indexes == null ? NullNode.getInstance() : indexes;
    }

    Preconditions.checkArgument(indexes == null || indexes.isNull() || indexes.isObject(),
        "'indexes' must be an object when used with legacy 'indexType'/'indexTypes' for column: %s", fieldName);

    ObjectNode normalizedIndexes =
        indexes != null && indexes.isObject() ? ((ObjectNode) indexes).deepCopy() : JSON_NODE_FACTORY.objectNode();
    for (IndexType legacyIndexType : legacyIndexTypes) {
      String indexKey = legacyIndexType.getPrettyName();
      Preconditions.checkArgument(!normalizedIndexes.has(indexKey),
          "Index '%s' is declared in both 'indexes' and legacy 'indexType'/'indexTypes' for column: %s", indexKey,
          fieldName);
      normalizedIndexes.set(indexKey, enabledIndexConfigNode());
    }
    return normalizedIndexes.size() == 0 ? NullNode.getInstance() : normalizedIndexes;
  }

  private static Set<IndexType> collectLegacyIndexTypes(@Nullable IndexType indexType,
      @Nullable List<IndexType> indexTypes) {
    Set<IndexType> legacyIndexTypes = new LinkedHashSet<>();
    if (indexType != null) {
      legacyIndexTypes.add(indexType);
    }
    if (indexTypes != null) {
      legacyIndexTypes.addAll(indexTypes);
    }
    return legacyIndexTypes;
  }

  private static ObjectNode enabledIndexConfigNode() {
    ObjectNode indexConfig = JSON_NODE_FACTORY.objectNode();
    indexConfig.put("disabled", false);
    return indexConfig;
  }
}
