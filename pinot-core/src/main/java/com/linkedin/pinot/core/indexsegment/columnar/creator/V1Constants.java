package com.linkedin.pinot.core.indexsegment.columnar.creator;

/**
 * Jun 30, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class V1Constants {
  public static final String VERSIONS_FILE = "versions.vr";
  public static final String VERSION = "segment,index.version";
  public static final String SEGMENT_DOWNLOAD_URL = "segment.download.url";

  public static class Numbers {
    // null representatives
    public static final Integer NULL_INT = Integer.MIN_VALUE;
    public static final Long NULL_LONG = Long.MIN_VALUE;
    public static final Float NULL_FLOAT = Float.MIN_VALUE;
    public static final Double NULL_DOUBLE = Double.MIN_VALUE;
  }

  public static class Str {
    public static final char STRING_PAD_CHAR = '%';
    public static final java.lang.String CHAR_SET = "UTF-8";
    public static final String NULL_STRING = "null";
  }

  public static class Idx {
    public static final int[] SORTED_INDEX_COLUMN_SIZE = new int[] { 4, 4 };
  }

  public static class Dict {
    public static final int[] INT_DICTIONARY_COL_SIZE = new int[] { 4 };
    public static final int[] LONG_DICTIONARY_COL_SIZE = new int[] { 8 };
    public static final int[] FOLAT_DICTIONARY_COL_SIZE = new int[] { 4 };
    public static final int[] DOUBLE_DICTIONARY_COL_SIZE = new int[] { 8 };
    public static final String FILE_EXTENTION = ".dict";
  }

  public static class Indexes {
    public static final String SORTED_FWD_IDX_FILE_EXTENTION = ".sorted.fwd";
    public static final String UN_SORTED_FWD_IDX_FILE_EXTENTION = ".unSorted.fwd";
    public static final String MULTI_VALUE_FWD_IDX_FILE_EXTENTION = ".multi.fwd";
    public static final String BITMAP_INVERTED_INDEX_FILE_EXTENSION = ".bitmap.inv";
    public static final String INTARRAY_INVERTED_INDEX_FILE_EXTENSION = ".intArray.inv";
  }

  public static class MetadataKeys {
    public static final String METADATA_FILE_NAME = "metadata.properties";

    public static class Segment {
      public static final String RESOURCE_NAME = "segment.resource.name";
      public static final String TABLE_NAME = "segment.table.name";
      public static final String DIMENSIONS = "segment.dimension.column.names";
      public static final String METRICS = "segment.metric.column.names";
      public static final String UNKNOWN_COLUMNS = "segment.unknown.column.names";
      public static final String TIME_COLUMN_NAME = "segment.time.column.name";
      public static final String TIME_UNIT = "segment.time.unit";
      public static final String TIME_INTERVAL = "segment.time.interval";
      public static final String CUSTOM_PROPERTIES_PREFIX = "segment.custom";
      public static final String SEGMENT_TOTAL_DOCS = "segment.total.docs";

      // not using currently
      public static final String SEGMENT_INDEX_TYPE = "segment.index.type";
    }

    public static class Column {
      public static final String CARDINALITY = "cardinality";
      public static final String TOTAL_DOCS = "todalDocs";
      public static final String DATA_TYPE = "dataType";
      public static final String BITS_PER_ELEMENT = "bitsPerElement";
      public static final String DICTIONARY_ELEMENT_SIZE = "lengthOfEachEntry";
      public static final String COLUMN_TYPE = "columnType";
      public static final String HAS_INVERTED_INDEX = "hasInvertedIndex";

      public static final String IS_SORTED = "isSorted";
      public static final String IS_SINGLE_VALUED = "isSingleValues";
      public static final String COLUMN_PROPS_KEY_PREFIX = "column.";

      public static String getKeyFor(String column, String key) {
        return COLUMN_PROPS_KEY_PREFIX + column + "." + key;
      }
    }
  }

  public static class Helix {
    public static final String PREFIX_OF_BROKER_RESOURCE_TAG = "broker_";
    public static final String BROKER_RESOURCE_INSTANCE = "brokerResource";
    public static final String UNTAGGED_SERVER_INSTANCE = "server_untagged";
    public static final String UNTAGGED_BROKER_INSTANCE = "broker_untagged";

    public static class DataSource {
      public static final String RESOURCE_NAME = "resourceName";
      public static final String TABLE_NAME = "tableName";
      public static final String TIME_COLUMN_NAME = "timeColumnName";
      public static final String TIME_TYPE = "timeType";
      public static final String NUMBER_OF_DATA_INSTANCES = "numberOfDataInstances";
      public static final String NUMBER_OF_COPIES = "numberOfCopies";
      public static final String RETENTION_TIME_UNIT = "retentionTimeUnit";
      public static final String RETENTION_TIME_VALUE = "retentionTimeValue";
      public static final String PUSH_FREQUENCY = "pushFrequency";
      public static final String SEGMENT_ASSIGNMENT_STRATEGY = "segmentAssignmentStrategy";
      public static final String BROKER_TAG_NAME = "brokerTagName";
      public static final String NUMBER_OF_BROKER_INSTANCES = "numberOfBrokerInstances";
    }

    public static final String KEY_OF_SEGMENT_ASSIGNMENT_STRATEGY = "segmentAssignmentStrategy";
    public static final String DEFAULT_SEGMENT_ASSIGNMENT_STRATEGY = "BalanceNumSegmentAssignmentStrategy";

  }
}
