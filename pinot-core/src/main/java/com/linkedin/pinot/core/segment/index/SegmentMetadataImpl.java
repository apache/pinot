package com.linkedin.pinot.core.segment.index;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.ZNRecord;
import org.apache.log4j.Logger;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 12, 2014
 */

public class SegmentMetadataImpl implements SegmentMetadata {

  private final PropertiesConfiguration _segmentMetadataPropertiesConfiguration;

  private static Logger LOGGER = Logger.getLogger(SegmentMetadataImpl.class);
  private final Map<String, ColumnMetadata> _columnMetadataMap;
  private Schema _segmentDataSchema;
  private String _segmentName;
  private final Set<String> _allColumns;
  private final Schema _schema;
  private final String _indexDir;
  private long _crc = Long.MIN_VALUE;
  private long _creationTime = Long.MIN_VALUE;
  private Interval _timeInterval;
  private Duration _timeGranularity;
  private long _pushTime = Long.MIN_VALUE;
  private long _refreshTime = Long.MIN_VALUE;

  public SegmentMetadataImpl(File indexDir) throws ConfigurationException, IOException {
    LOGGER.info("SegmentMetadata location: " + indexDir);
    if (indexDir.isDirectory()) {
      _segmentMetadataPropertiesConfiguration =
          new PropertiesConfiguration(new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    } else {
      _segmentMetadataPropertiesConfiguration = new PropertiesConfiguration(indexDir);
    }

    _columnMetadataMap = new HashMap<String, ColumnMetadata>();
    _allColumns = new HashSet<String>();
    _schema = new Schema();
    _indexDir = new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME).getAbsoluteFile().getParent();
    init();
    loadCreationMeta(new File(indexDir, V1Constants.SEGMENT_CREATION_META));
    setTimeIntervalAndGranularity();
  }

  public SegmentMetadataImpl(ZNRecord record) {
    final Map<String, String> configs = record.getSimpleFields();
    _segmentMetadataPropertiesConfiguration = new PropertiesConfiguration();
    for (final Entry<String, String> entry : configs.entrySet()) {
      _segmentMetadataPropertiesConfiguration.addProperty(entry.getKey(), entry.getValue());
    }
    if (configs.containsKey(V1Constants.MetadataKeys.Segment.SEGMENT_CRC)) {
      _crc = Long.parseLong(configs.get(V1Constants.MetadataKeys.Segment.SEGMENT_CRC));
    }
    if (configs.containsKey(V1Constants.MetadataKeys.Segment.SEGMENT_CREATION_TIME)) {
      _creationTime = Long.parseLong(configs.get(V1Constants.MetadataKeys.Segment.SEGMENT_CREATION_TIME));
    }
    if (configs.containsKey(V1Constants.SEGMENT_PUSH_TIME)) {
      try {
        _pushTime = Long.parseLong(configs.get(V1Constants.SEGMENT_PUSH_TIME));
      } catch (NumberFormatException nfe) {
        LOGGER.warn("Invalid data while loading push time", nfe);
      }
    }
    if (configs.containsKey(V1Constants.SEGMENT_REFRESH_TIME)) {
      try {
        _refreshTime = Long.parseLong(configs.get(V1Constants.SEGMENT_REFRESH_TIME));
      } catch (NumberFormatException nfe) {
        LOGGER.warn("Invalid data while loading refresh time", nfe);
      }
    }
    setTimeIntervalAndGranularity();
    _columnMetadataMap = null;
    _segmentName = record.getId();
    _schema = new Schema();
    _allColumns = new HashSet<String>();
    _indexDir = null;

  }

  private void setTimeIntervalAndGranularity() {
    if (_segmentMetadataPropertiesConfiguration.containsKey(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME) &&
        _segmentMetadataPropertiesConfiguration.containsKey(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME) &&
        _segmentMetadataPropertiesConfiguration.containsKey(V1Constants.MetadataKeys.Segment.TIME_UNIT)) {

      try {
        TimeUnit segmentTimeUnit = TimeUnit.valueOf(_segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Segment.TIME_UNIT));
        _timeGranularity = new Duration(segmentTimeUnit.toMillis(1));
        String startTimeString = _segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME);
        String endTimeString = _segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME);
        _timeInterval = new Interval(segmentTimeUnit.toMillis(Long.parseLong(startTimeString)), segmentTimeUnit.toMillis(Long.parseLong(endTimeString)));
      } catch (Exception e) {
        _timeInterval = null;
        _timeGranularity = null;
      }
    }
  }

  private void loadCreationMeta(File crcFile) throws IOException {
    if (crcFile.exists()) {
      final DataInputStream ds = new DataInputStream(new FileInputStream(crcFile));
      _crc = ds.readLong();
      _creationTime = ds.readLong();
      ds.close();
    }
  }

  public Set<String> getAllColumns() {
    return _allColumns;
  }

  private void init() {
    final Iterator<String> metrics =
        _segmentMetadataPropertiesConfiguration.getList(V1Constants.MetadataKeys.Segment.METRICS).iterator();
    while (metrics.hasNext()) {
      final String columnName = metrics.next();
      if (columnName.trim().length() > 0) {
        _allColumns.add(columnName);
      }
    }

    final Iterator<String> dimensions =
        _segmentMetadataPropertiesConfiguration.getList(V1Constants.MetadataKeys.Segment.DIMENSIONS).iterator();
    while (dimensions.hasNext()) {
      final String columnName = dimensions.next();
      if (columnName.trim().length() > 0) {
        _allColumns.add(columnName);
      }
    }

    final Iterator<String> unknowns =
        _segmentMetadataPropertiesConfiguration.getList(V1Constants.MetadataKeys.Segment.UNKNOWN_COLUMNS).iterator();
    while (unknowns.hasNext()) {
      final String columnName = unknowns.next();
      if (columnName.trim().length() > 0) {
        _allColumns.add(columnName);
      }
    }

    final Iterator<String> timeStamps =
        _segmentMetadataPropertiesConfiguration.getList(V1Constants.MetadataKeys.Segment.TIME_COLUMN_NAME).iterator();
    while (timeStamps.hasNext()) {
      final String columnName = timeStamps.next();
      if (columnName.trim().length() > 0) {
        _allColumns.add(columnName);
      }
    }

    _segmentDataSchema = new Schema();

    _segmentName = _segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Segment.SEGMENT_NAME);

    for (final String column : _allColumns) {
      _columnMetadataMap.put(column, extractColumnMetadataFor(column));
    }

    for (final String column : _columnMetadataMap.keySet()) {
      _schema.addSchema(column, _columnMetadataMap.get(column).toFieldSpec());
    }
  }

  private ColumnMetadata extractColumnMetadataFor(String column) {
    final int cardinality =
        _segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.CARDINALITY));
    final int totalDocs =
        _segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.TOTAL_DOCS));
    final DataType dataType =
        DataType.valueOf(_segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Column.getKeyFor(
            column, V1Constants.MetadataKeys.Column.DATA_TYPE)));
    final int bitsPerElement =
        _segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT));
    final int stringColumnMaxLength =
        _segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.DICTIONARY_ELEMENT_SIZE));

    final FieldType fieldType =
        FieldType.valueOf(_segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Column.getKeyFor(
            column, V1Constants.MetadataKeys.Column.COLUMN_TYPE)));
    final boolean isSorted =
        _segmentMetadataPropertiesConfiguration.getBoolean(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.IS_SORTED));

    final boolean hasInvertedIndex =
        _segmentMetadataPropertiesConfiguration.getBoolean(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.HAS_INVERTED_INDEX));

    final boolean insSingleValue =
        _segmentMetadataPropertiesConfiguration.getBoolean(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED));

    final int maxNumberOfMultiValues =
        _segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.MAX_MULTI_VALUE_ELEMTS));

    final boolean hasNulls =
        _segmentMetadataPropertiesConfiguration.getBoolean(V1Constants.MetadataKeys.Column.getKeyFor(column,
            V1Constants.MetadataKeys.Column.HAS_NULL_VALUE));

    return new ColumnMetadata(column, cardinality, totalDocs, dataType, bitsPerElement, stringColumnMaxLength,
        fieldType, isSorted, hasInvertedIndex, insSingleValue, maxNumberOfMultiValues, hasNulls);
  }

  public ColumnMetadata getColumnMetadataFor(String column) {
    return _columnMetadataMap.get(column);
  }

  public Map<String, ColumnMetadata> getColumnMetadataMap() {
    return _columnMetadataMap;
  }

  @Override
  public String getResourceName() {
    return (String) _segmentMetadataPropertiesConfiguration.getProperty(V1Constants.MetadataKeys.Segment.RESOURCE_NAME);
  }

  @Override
  public String getTableName() {
    return (String) _segmentMetadataPropertiesConfiguration.getProperty(V1Constants.MetadataKeys.Segment.TABLE_NAME);
  }

  @Override
  public String getIndexType() {
    return IndexType.COLUMNAR.toString();
  }

  @Override
  public Duration getTimeGranularity() {
    return _timeGranularity;
  }

  @Override
  public Interval getTimeInterval() {
    return _timeInterval;
  }

  @Override
  public String getCrc() {
    return String.valueOf(_crc);
  }

  @Override
  public String getVersion() {
    return SegmentVersion.v1.toString();
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public String getShardingKey() {
    return null;
  }

  @Override
  public int getTotalDocs() {
    return _segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_DOCS);
  }

  @Override
  public String getIndexDir() {
    return _indexDir;
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
    ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_NAME, getName());
    ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_CRC, getCrc());
    ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_CREATION_TIME, getIndexCreationTime() + "");
    ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME,
        _segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME));
    ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME,
        _segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME));
    ret.put(V1Constants.MetadataKeys.Segment.TIME_UNIT,
        _segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Segment.TIME_UNIT));

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
        result.append("ERROR");
      }
      result.append(newLine);
    }
    result.append("}");

    return result.toString();
  }

  @Override
  public long getIndexCreationTime() {
    return _creationTime;
  }

  @Override
  public long getPushTime() {
    return _pushTime;
  }

  @Override
  public long getRefreshTime() {
    return _refreshTime;
  }
}
