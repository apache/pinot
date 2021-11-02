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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
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

  private final String _name;
  private final EncodingType _encodingType;
  private final List<IndexType> _indexTypes;
  private final CompressionCodec _compressionCodec;
  private final Map<String, String> _properties;

  @Deprecated
  public FieldConfig(String name, EncodingType encodingType, IndexType indexType, CompressionCodec compressionCodec,
      Map<String, String> properties) {
    this(name, encodingType, indexType, null, compressionCodec, properties);
  }

  public FieldConfig(String name, EncodingType encodingType, List<IndexType> indexTypes,
      CompressionCodec compressionCodec, Map<String, String> properties) {
    this(name, encodingType, null, indexTypes, compressionCodec, properties);
  }

  @JsonCreator
  public FieldConfig(@JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = "encodingType") EncodingType encodingType,
      @JsonProperty(value = "indexType") @Nullable IndexType indexType,
      @JsonProperty(value = "indexTypes") @Nullable List<IndexType> indexTypes,
      @JsonProperty(value = "compressionCodec") @Nullable CompressionCodec compressionCodec,
      @JsonProperty(value = "properties") @Nullable Map<String, String> properties) {
    Preconditions.checkArgument(name != null, "'name' must be configured");
    _name = name;
    _encodingType = encodingType;
    _indexTypes = indexTypes != null ? indexTypes : (
        indexType == null ? Lists.newArrayList() : Lists.newArrayList(indexType));
    _compressionCodec = compressionCodec;
    _properties = properties;
  }

  // If null, we will create dictionary encoded forward index by default
  public enum EncodingType {
    RAW, DICTIONARY
  }

  // If null, there won't be any index
  public enum IndexType {
    INVERTED, SORTED, TEXT, FST, H3, JSON, RANGE
  }

  public enum CompressionCodec {
    PASS_THROUGH, SNAPPY, ZSTANDARD, LZ4
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

  @Nullable
  public CompressionCodec getCompressionCodec() {
    return _compressionCodec;
  }

  @Nullable
  public Map<String, String> getProperties() {
    return _properties;
  }
}
