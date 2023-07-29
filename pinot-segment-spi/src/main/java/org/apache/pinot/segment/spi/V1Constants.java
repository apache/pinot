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
package org.apache.pinot.segment.spi;

public class V1Constants {
  private V1Constants() {
  }

  public static final String SEGMENT_CREATION_META = "creation.meta";
  public static final String INDEX_MAP_FILE_NAME = "index_map";
  public static final String INDEX_FILE_NAME = "columns.psf";
  public static final String VALID_DOC_IDS_SNAPSHOT_FILE_NAME = "validdocids.bitmap.snapshot";
  public static final String TTL_WATERMARK_TABLE_PARTITION = "ttl.watermark.partition.";

  public static class Str {
    public static final char DEFAULT_STRING_PAD_CHAR = '\0';
  }

  public static class Dict {
    public static final String FILE_EXTENSION = ".dict";
  }

  public static class Indexes {
    public static final String UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION = ".sv.unsorted.fwd";
    public static final String SORTED_SV_FORWARD_INDEX_FILE_EXTENSION = ".sv.sorted.fwd";
    public static final String RAW_SV_FORWARD_INDEX_FILE_EXTENSION = ".sv.raw.fwd";
    public static final String RAW_MV_FORWARD_INDEX_FILE_EXTENSION = ".mv.raw.fwd";
    public static final String UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION = ".mv.fwd";
    public static final String BITMAP_INVERTED_INDEX_FILE_EXTENSION = ".bitmap.inv";
    public static final String BITMAP_RANGE_INDEX_FILE_EXTENSION = ".bitmap.range";
    public static final String FST_INDEX_FILE_EXTENSION = ".lucene.fst";
    public static final String JSON_INDEX_FILE_EXTENSION = ".json.idx";
    public static final String NATIVE_TEXT_INDEX_FILE_EXTENSION = ".nativetext.idx";
    public static final String H3_INDEX_FILE_EXTENSION = ".h3.idx";
    public static final String BLOOM_FILTER_FILE_EXTENSION = ".bloom";
    public static final String NULLVALUE_VECTOR_FILE_EXTENSION = ".bitmap.nullvalue";
    public static final String LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION = ".lucene.mapping";
    public static final String LUCENE_TEXT_INDEX_FILE_EXTENSION = ".lucene.index";
  }

  public static class MetadataKeys {
    public static final String METADATA_FILE_NAME = "metadata.properties";

    public static class Segment {
      public static final String SEGMENT_CREATOR_VERSION = "creator.version";
      public static final String SEGMENT_NAME = "segment.name";
      public static final String SEGMENT_VERSION = "segment.index.version";
      public static final String TABLE_NAME = "segment.table.name";
      public static final String DIMENSIONS = "segment.dimension.column.names";
      public static final String METRICS = "segment.metric.column.names";
      /**
       * The primary time column for the table. This will match the timeColumnName defined in the tableConfig.
       * In the Pinot schema, this column can be defined as either a TimeFieldSpec (which is deprecated) or
       * DateTimeFieldSpec
       */
      public static final String TIME_COLUMN_NAME = "segment.time.column.name";
      public static final String TIME_UNIT = "segment.time.unit";
      public static final String SEGMENT_START_TIME = "segment.start.time";
      public static final String SEGMENT_END_TIME = "segment.end.time";
      public static final String DATETIME_COLUMNS = "segment.datetime.column.names";
      public static final String SEGMENT_TOTAL_DOCS = "segment.total.docs";
      public static final String SEGMENT_PADDING_CHARACTER = "segment.padding.character";

      public static final String CUSTOM_SUBSET = "custom";

      public static class Realtime {
        public static final String START_OFFSET = "segment.realtime.startOffset";
        public static final String END_OFFSET = "segment.realtime.endOffset";
      }
    }

    public static class Column {
      public static final String CARDINALITY = "cardinality";
      public static final String TOTAL_DOCS = "totalDocs";
      public static final String DATA_TYPE = "dataType";
      public static final String BITS_PER_ELEMENT = "bitsPerElement";
      public static final String DICTIONARY_ELEMENT_SIZE = "lengthOfEachEntry";
      public static final String COLUMN_TYPE = "columnType";
      public static final String IS_SORTED = "isSorted";
      public static final String HAS_DICTIONARY = "hasDictionary";
      public static final String IS_SINGLE_VALUED = "isSingleValues";
      public static final String MAX_MULTI_VALUE_ELEMENTS = "maxNumberOfMultiValues";
      public static final String TOTAL_NUMBER_OF_ENTRIES = "totalNumberOfEntries";
      public static final String IS_AUTO_GENERATED = "isAutoGenerated";
      public static final String DEFAULT_NULL_VALUE = "defaultNullValue";
      public static final String MIN_VALUE = "minValue";
      public static final String MAX_VALUE = "maxValue";
      public static final String IS_MIN_MAX_VALUE_ESCAPED = "isMinMaxValueEscaped";
      public static final String MIN_MAX_VALUE_INVALID = "minMaxValueInvalid";
      public static final String PARTITION_FUNCTION = "partitionFunction";
      public static final String PARTITION_FUNCTION_CONFIG = "partitionFunctionConfig";
      public static final String NUM_PARTITIONS = "numPartitions";
      public static final String PARTITION_VALUES = "partitionValues";
      public static final String DATETIME_FORMAT = "datetimeFormat";
      public static final String DATETIME_GRANULARITY = "datetimeGranularity";

      public static final String COLUMN_PROPS_KEY_PREFIX = "column.";

      public static String getKeyFor(String column, String key) {
        return COLUMN_PROPS_KEY_PREFIX + column + "." + key;
      }
    }
  }
}
