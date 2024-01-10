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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
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
  public static final String TEXT_INDEX_LUCENE_USE_COMPOUND_FILE = "luceneUseCompoundFile";
  public static final String TEXT_INDEX_LUCENE_MAX_BUFFER_SIZE_MB = "luceneMaxBufferSizeMB";
  public static final String TEXT_INDEX_LUCENE_ANALYZER_CLASS = "luceneAnalyzerClass";
  public static final String TEXT_INDEX_DEFAULT_LUCENE_ANALYZER_CLASS
          = "org.apache.lucene.analysis.standard.StandardAnalyzer";
  public static final String TEXT_INDEX_STOP_WORD_SEPERATOR = ",";
  // "native" for native, default is Lucene
  public static final String TEXT_FST_TYPE = "fstType";
  public static final String TEXT_NATIVE_FST_LITERAL = "native";
  // Config to disable forward index
  public static final String FORWARD_INDEX_DISABLED = "forwardIndexDisabled";
  public static final String DEFAULT_FORWARD_INDEX_DISABLED = Boolean.FALSE.toString();

  private final String _name;
  private final EncodingType _encodingType;
  private final List<IndexType> _indexTypes;
  private final JsonNode _indexes;
  private final JsonNode _tierOverwrites;
  private final CompressionCodec _compressionCodec;
  private final Map<String, String> _properties;
  private final TimestampConfig _timestampConfig;

  @Deprecated
  public FieldConfig(String name, EncodingType encodingType, IndexType indexType, CompressionCodec compressionCodec,
      Map<String, String> properties) {
    this(name, encodingType, indexType, null, compressionCodec, null, null, properties, null);
  }

  public FieldConfig(String name, EncodingType encodingType, List<IndexType> indexTypes,
      CompressionCodec compressionCodec, Map<String, String> properties) {
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
    _encodingType = encodingType;
    _indexTypes = indexTypes != null ? indexTypes : (
        indexType == null ? Lists.newArrayList() : Lists.newArrayList(indexType));
    _compressionCodec = compressionCodec;
    _timestampConfig = timestampConfig;
    _properties = properties;
    _indexes = indexes == null ? NullNode.getInstance() : indexes;
    _tierOverwrites = tierOverwrites == null ? NullNode.getInstance() : tierOverwrites;
  }

  // If null, we will create dictionary encoded forward index by default
  public enum EncodingType {
    RAW, DICTIONARY
  }

  // If null, there won't be any index
  public enum IndexType {
    INVERTED, SORTED, TEXT, FST, H3, JSON, TIMESTAMP, VECTOR, RANGE
  }

  public enum CompressionCodec {
    PASS_THROUGH(true, false),
    SNAPPY(true, false),
    ZSTANDARD(true, false),
    LZ4(true, false),
    CLP(true, false),

    // For MV dictionary encoded forward index, add a second level dictionary encoding for the multi-value entries
    MV_ENTRY_DICT(false, true);

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
  public IndexType getIndexType() {
    return _indexTypes.size() > 0 ? _indexTypes.get(0) : null;
  }

  public List<IndexType> getIndexTypes() {
    return _indexTypes;
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
    @Nonnull
    private String _name;
    private EncodingType _encodingType;
    private List<IndexType> _indexTypes;
    private JsonNode _indexes;
    private CompressionCodec _compressionCodec;
    private Map<String, String> _properties;
    private TimestampConfig _timestampConfig;
    private JsonNode _tierOverwrites;

    public Builder(@Nonnull String name) {
      _name = name;
    }

    public Builder(FieldConfig other) {
      _name = other._name;
      _encodingType = other._encodingType;
      _indexTypes = other._indexTypes;
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
      return new FieldConfig(_name, _encodingType, null, _indexTypes, _compressionCodec, _timestampConfig,
          _indexes, _properties, _tierOverwrites);
    }
  }
}
