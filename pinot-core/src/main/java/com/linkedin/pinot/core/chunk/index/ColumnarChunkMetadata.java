package com.linkedin.pinot.core.chunk.index;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadata;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 12, 2014
 */

public class ColumnarChunkMetadata implements SegmentMetadata {

  private final PropertiesConfiguration segmentMetadataPropertiesConfiguration;

  private static Logger LOGGER = Logger.getLogger(ColumnarSegmentMetadata.class);
  private final Map<String, ChunkColumnMetadata> columnMetadataMap;
  private Schema _segmentDataSchema;
  private String _segmentName;
  private final Set<String> allColumns;
  private final Schema schema;

  public ColumnarChunkMetadata(File metadataFile) throws ConfigurationException {
    segmentMetadataPropertiesConfiguration = new PropertiesConfiguration(metadataFile);
    columnMetadataMap = new HashMap<String, ChunkColumnMetadata>();
    allColumns = new HashSet<String>();
    schema = new Schema();
    init();
  }

  public Set<String> getAllColumns() {
    return allColumns;
  }

  private void init() {
    final Iterator<String> metrics = segmentMetadataPropertiesConfiguration.getList(V1Constants.MetadataKeys.Segment.METRICS).iterator();
    while (metrics.hasNext()) {
      final String columnName = metrics.next();
      if (columnName.trim().length() > 0) {
        allColumns.add(columnName);
      }
    }

    final Iterator<String> dimensions =
        segmentMetadataPropertiesConfiguration.getList(V1Constants.MetadataKeys.Segment.DIMENSIONS).iterator();
    while (dimensions.hasNext()) {
      final String columnName = dimensions.next();
      if (columnName.trim().length() > 0) {
        allColumns.add(columnName);
      }
    }

    final Iterator<String> unknowns =
        segmentMetadataPropertiesConfiguration.getList(V1Constants.MetadataKeys.Segment.UNKNOWN_COLUMNS).iterator();
    while (unknowns.hasNext()) {
      final String columnName = unknowns.next();
      if (columnName.trim().length() > 0) {
        allColumns.add(columnName);
      }
    }

    final Iterator<String> timeStamps =
        segmentMetadataPropertiesConfiguration.getList(V1Constants.MetadataKeys.Segment.TIME_COLUMN_NAME).iterator();
    while (timeStamps.hasNext()) {
      final String columnName = timeStamps.next();
      if (columnName.trim().length() > 0) {
        allColumns.add(columnName);
      }
    }

    _segmentDataSchema = new Schema();

    _segmentName = segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Segment.SEGMENT_NAME);

    for (final String column : allColumns) {
      columnMetadataMap.put(column, extractColumnMetadataFor(column));
    }

    for (final String column : columnMetadataMap.keySet()) {
      schema.addSchema(column, columnMetadataMap.get(column).toFieldSpec());
    }
  }

  private ChunkColumnMetadata extractColumnMetadataFor(String column) {
    final int cardinality =
        segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.CARDINALITY));
    final int totalDocs =
        segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.TOTAL_DOCS));
    final DataType dataType =
        DataType.valueOf(segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.DATA_TYPE)));
    final int bitsPerElement =
        segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT));
    final int stringColumnMaxLength =
        segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.DICTIONARY_ELEMENT_SIZE));

    final FieldType fieldType =
        FieldType.valueOf(segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.COLUMN_TYPE)));
    final boolean isSorted =
        segmentMetadataPropertiesConfiguration.getBoolean(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.IS_SORTED));

    final boolean hasInvertedIndex =
        segmentMetadataPropertiesConfiguration.getBoolean(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.HAS_INVERTED_INDEX));

    final boolean insSingleValue =
        segmentMetadataPropertiesConfiguration.getBoolean(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED));

    final int maxNumberOfMultiValues =
        segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.MAX_MULTI_VALUE_ELEMTS));

    return new ChunkColumnMetadata(column, cardinality, totalDocs, dataType, bitsPerElement, stringColumnMaxLength, fieldType, isSorted,
        hasInvertedIndex, insSingleValue, maxNumberOfMultiValues);
  }

  public ChunkColumnMetadata getColumnMetadataFor(String column) {
    return columnMetadataMap.get(column);
  }

  public Map<String, ChunkColumnMetadata> getColumnMetadataMap() {
    return columnMetadataMap;
  }

  @Override
  public String getResourceName() {
    return (String) segmentMetadataPropertiesConfiguration.getProperty(V1Constants.MetadataKeys.Segment.RESOURCE_NAME);
  }

  @Override
  public String getTableName() {
    return (String) segmentMetadataPropertiesConfiguration.getProperty(V1Constants.MetadataKeys.Segment.TABLE_NAME);
  }

  @Override
  public String getIndexType() {
    return IndexType.columnar.toString();
  }

  @Override
  public Duration getTimeGranularity() {
    return null;
  }

  @Override
  public Interval getTimeInterval() {
    return null;
  }

  @Override
  public String getCrc() {
    return null;
  }

  @Override
  public String getVersion() {
    return SegmentVersion.v1.toString();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public String getShardingKey() {
    return null;
  }

  @Override
  public int getTotalDocs() {
    return segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_DOCS);
  }

  @Override
  public String getIndexDir() {
    return null;
  }

  @Override
  public String getName() {
    return _segmentName;
  }

  @Override
  public Map<String, String> toMap() {
    final Map<String, String> ret = new HashMap<String, String>();
    ret.put(V1Constants.MetadataKeys.Segment.RESOURCE_NAME, getResourceName());
    ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_DOCS, String.valueOf(getTotalDocs()));
    ret.put(V1Constants.VERSION, getVersion());
    ret.put(V1Constants.MetadataKeys.Segment.TABLE_NAME, getTableName());
    return ret;
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    final String newLine = System.getProperty("line.separator");

    result.append(this.getClass().getName());
    result.append(" Object {");
    result.append(newLine);

    //determine fields declared in this class only (no fields of superclass)
    final Field[] fields = this.getClass().getDeclaredFields();

    //print field names paired with their values
    for (final Field field : fields) {
      result.append("  ");
      try {
        result.append(field.getName());
        result.append(": ");
        //requires access to private field:
        result.append(field.get(this));
      } catch (final IllegalAccessException ex) {
        System.out.println(ex);
      }
      result.append(newLine);
    }
    result.append("}");

    return result.toString();
  }

}
