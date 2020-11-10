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
import org.apache.pinot.spi.config.BaseJsonConfig;

import java.util.Map;
import javax.annotation.Nullable;


public class FieldConfig extends BaseJsonConfig {
  private final String _name;
  private final EncodingType _encodingType;
  private final IndexType _indexType;
  private final Map<String, String> _properties;

  public static String BLOOM_FILTER_COLUMN_KEY = "createBloomFilter";
  public static String ON_HEAP_DICTIONARY_COLUMN_KEY = "useOnHeapDictionary";
  public static String VAR_LENGTH_DICTIONARY_COLUMN_KEY = "useVarLengthDictionary";
  public static String DERIVE_NUM_DOCS_PER_CHUNK_RAW_INDEX_KEY = "deriveNumDocsPerChunkForRawIndex";
  public static String RAW_INDEX_WRITER_VERSION = "rawIndexWriterVersion";

  public static String TEXT_INDEX_REALTIME_READER_REFRESH_KEY = "textIndexRealtimeReaderRefreshThreshold";
  // Lucene creates a query result cache if this option is enabled
  // the cache improves performance of repeatable queries
  public static String TEXT_INDEX_ENABLE_QUERY_CACHE = "enableQueryCacheForTextIndex";
  public static String TEXT_INDEX_USE_AND_FOR_MULTI_TERM_QUERIES = "useANDForMultiTermTextIndexQueries";

  @JsonCreator
  public FieldConfig(@JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = "encodingType") @Nullable EncodingType encodingType,
      @JsonProperty(value = "indexType") @Nullable IndexType indexType,
      @JsonProperty(value = "properties") @Nullable Map<String, String> properties) {
    Preconditions.checkArgument(name != null, "'name' must be configured");
    _name = name;
    _encodingType = encodingType;
    _indexType = indexType;
    _properties = properties;
  }

  // If null, we will create dictionary encoded forward index by default
  public enum EncodingType {
    RAW, DICTIONARY
  }

  // If null, there won't be any index
  public enum IndexType {
    INVERTED, SORTED, TEXT
  }

  public String getName() {
    return _name;
  }

  @Nullable
  public EncodingType getEncodingType() {
    return _encodingType;
  }

  @Nullable
  public IndexType getIndexType() {
    return _indexType;
  }

  @Nullable
  public Map<String, String> getProperties() {
    return _properties;
  }
}
