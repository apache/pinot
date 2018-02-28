/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.creator.impl;

/**
 * Jun 30, 2014
 *
 */
public class V1Constants {
  public static final String QUERY_RHS_DELIMITER = "\t\t";
  public static final String SEGMENT_CREATION_META = "creation.meta";
  public static final String STAR_TREE_INDEX_DIR = "star-tree";
  public static final String STAR_TREE_INDEX_FILE = "star-tree.bin";
  public static final String STAR_TREE_ON_HEAP_INDEX_FILE = "star-tree-on-heap.bin";
  public static final String STAR_TREE_OFF_HEAP_INDEX_FILE = "star-tree-off-heap.bin";
  public static final String VERSIONS_FILE = "versions.vr";
  public static final String SEGMENT_DOWNLOAD_URL = "segment.download.url";
  public static final String SEGMENT_PUSH_TIME = "segment.push.time";
  public static final String SEGMENT_REFRESH_TIME = "segment.refresh.time";

  public static class Numbers {
    // null representatives
    public static final Integer NULL_INT = Integer.MIN_VALUE;
    public static final Long NULL_LONG = Long.MIN_VALUE;
    public static final Float NULL_FLOAT = Float.MIN_VALUE;
    public static final Double NULL_DOUBLE = Double.MIN_VALUE;

    public static final int INTEGER_SIZE = Integer.SIZE / Byte.SIZE;
    public static final int LONG_SIZE = Long.SIZE / Byte.SIZE;
    public static final int FLOAT_SIZE = Float.SIZE / Byte.SIZE;
    public static final int DOUBLE_SIZE = Double.SIZE / Byte.SIZE;
  }

  public static class Str {
    public static final char DEFAULT_STRING_PAD_CHAR = '\0';
    public static final char LEGACY_STRING_PAD_CHAR = '%';
    public static final java.lang.String CHAR_SET = "UTF-8";
    public static final String NULL_STRING = "nil";
    public static final String NULL_BOOLEAN = Boolean.toString(false);
  }

  public static class Dict {
    public static final String FILE_EXTENSION = ".dict";
  }

  public static class Indexes {
    public static final String UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION = ".sv.unsorted.fwd";
    public static final String SORTED_SV_FORWARD_INDEX_FILE_EXTENSION = ".sv.sorted.fwd";
    public static final String RAW_SV_FORWARD_INDEX_FILE_EXTENSION = ".sv.raw.fwd";
    public static final String UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION = ".mv.fwd";
    public static final String BITMAP_INVERTED_INDEX_FILE_EXTENSION = ".bitmap.inv";
  }

  public static class MetadataKeys {
    public static final String METADATA_FILE_NAME = "metadata.properties";

    public static class StarTree {
      public static final String STAR_TREE_ENABLED = "startree.enabled";
      public static final String STAR_TREE_SPLIT_ORDER = "startree.split.order";
      public static final String STAR_TREE_MAX_LEAF_RECORDS = "startree.max.leaf.records";
      public static final String STAR_TREE_SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS =
          "startree.skip.star.node.creation.for.dimensions";
      public static final String STAR_TREE_SKIP_MATERIALIZATION_FOR_DIMENSIONS =
          "star.tree.skip.materialization.for.dimensions";
      public static final String STAR_TREE_SKIP_MATERIALIZATION_CARDINALITY =
          "star.tree.skip.materialization.cardinality";
    }

    public static class Segment {
      public static final String SEGMENT_CREATOR_VERSION = "creator.version";
      public static final String SEGMENT_NAME = "segment.name";
      public static final String SEGMENT_VERSION = "segment.index.version";
      public static final String TABLE_NAME = "segment.table.name";
      public static final String DIMENSIONS = "segment.dimension.column.names";
      public static final String METRICS = "segment.metric.column.names";
      public static final String UNKNOWN_COLUMNS = "segment.unknown.column.names";
      public static final String TIME_COLUMN_NAME = "segment.time.column.name";
      public static final String TIME_UNIT = "segment.time.unit";
      public static final String TIME_INTERVAL = "segment.time.interval";
      public static final String DATETIME_COLUMNS = "segment.datetime.column.names";
      public static final String CUSTOM_PROPERTIES_PREFIX = "segment.custom";
      public static final String SEGMENT_TOTAL_DOCS = "segment.total.docs";
      public static final String SEGMENT_TOTAL_RAW_DOCS = "segment.total.raw.docs";
      public static final String SEGMENT_TOTAL_AGGREGATE_DOCS = "segment.total.aggregate.docs";
      public static final String SEGMENT_CRC = "segment.crc";
      public static final String SEGMENT_CREATION_TIME = "segment.creation.time";
      public static final String SEGMENT_PADDING_CHARACTER = "segment.padding.character";
      public static final String SEGMENT_TOTAL_ERRORS = "segment.total.errors";
      public static final String SEGMENT_TOTAL_NULLS = "segment.total.nulls";
      public static final String SEGMENT_TOTAL_CONVERSIONS = "segment.total.conversions";
      public static final String SEGMENT_TOTAL_NULL_COLS = "segment.total.null.cols";
      public static final String SEGMENT_HLL_LOG2M = "segment.hll.log2m";

      // not using currently
      public static final String SEGMENT_INDEX_TYPE = "segment.index.type";
      public static final String SEGMENT_START_TIME = "segment.start.time";
      public static final String SEGMENT_END_TIME = "segment.end.time";
      public static final String SEGMENT_TIME_GRANULARITY = "segment.time.granularity";
    }

    public static class Column {
      public static final String CARDINALITY = "cardinality";
      public static final String TOTAL_DOCS = "totalDocs";
      public static final String TOTAL_RAW_DOCS = "totalRawDocs";
      public static final String TOTAL_AGG_DOCS = "totalAggDocs";
      public static final String DATA_TYPE = "dataType";
      public static final String BITS_PER_ELEMENT = "bitsPerElement";
      public static final String DICTIONARY_ELEMENT_SIZE = "lengthOfEachEntry";
      public static final String COLUMN_TYPE = "columnType";
      public static final String IS_SORTED = "isSorted";
      public static final String HAS_NULL_VALUE = "hasNullValue";
      public static final String HAS_DICTIONARY = "hasDictionary";
      public static final String HAS_INVERTED_INDEX = "hasInvertedIndex";
      public static final String IS_SINGLE_VALUED = "isSingleValues";
      public static final String MAX_MULTI_VALUE_ELEMTS = "maxNumberOfMultiValues";
      public static final String TOTAL_NUMBER_OF_ENTRIES = "totalNumberOfEntries";
      public static final String IS_AUTO_GENERATED = "isAutoGenerated";
      public static final String DEFAULT_NULL_VALUE = "defaultNullValue";
      public static final String DERIVED_METRIC_TYPE = "derivedMetricType";
      public static final String ORIGIN_COLUMN = "originColumn";
      public static final String MIN_VALUE = "minValue";
      public static final String MAX_VALUE = "maxValue";
      public static final String PARTITION_FUNCTION = "partitionFunction";
      public static final String NUM_PARTITIONS = "numPartitions";
      public static final String PARTITION_VALUES = "partitionValues";
      public static final String DATETIME_FORMAT = "datetimeFormat";
      public static final String DATETIME_GRANULARITY = "datetimeGranularity";


      private static final String COLUMN_PROPS_KEY_PREFIX = "column.";
      public static String getKeyFor(String column, String key) {
        return COLUMN_PROPS_KEY_PREFIX + column + "." + key;
      }
    }
  }
}
