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
package com.linkedin.pinot.core.segment.index;

import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.StarTreeMetadata;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.startree.hll.HllConstants;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringEscapeUtils;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.TIME_UNIT;


public class SegmentMetadataImpl implements SegmentMetadata {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMetadataImpl.class);

  private final PropertiesConfiguration _segmentMetadataPropertiesConfiguration;
  private final File _metadataFile;
  private final Map<String, ColumnMetadata> _columnMetadataMap;
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
  private SegmentVersion _segmentVersion;
  private boolean _hasStarTree;
  private StarTreeMetadata _starTreeMetadata = null;
  private String _creatorName;
  private char _paddingCharacter = V1Constants.Str.DEFAULT_STRING_PAD_CHAR;
  private int _hllLog2m = HllConstants.DEFAULT_LOG2M;

  public SegmentMetadataImpl(File indexDir) throws ConfigurationException, IOException {
    LOGGER.debug("SegmentMetadata location: {}", indexDir);
    if (indexDir.isDirectory()) {
      _metadataFile = new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    } else {
      _metadataFile = indexDir;
    }
    if (!_metadataFile.exists()) {
      String logMessage = String.format("Metadata file: %s does not exist in directory: %s", _metadataFile, indexDir);
      LOGGER.error(logMessage);
      throw new RuntimeException(logMessage);
    }
    _segmentMetadataPropertiesConfiguration = new PropertiesConfiguration(_metadataFile);
    _columnMetadataMap = new HashMap<String, ColumnMetadata>();
    _allColumns = new HashSet<String>();
    _schema = new Schema();
    _indexDir = new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME).getAbsoluteFile().getParent();
    init();
    loadCreationMeta(new File(indexDir, V1Constants.SEGMENT_CREATION_META));
    setTimeIntervalAndGranularity();
    LOGGER.debug("loaded metadata for {}", indexDir.getName());
  }

  public SegmentMetadataImpl(OfflineSegmentZKMetadata offlineSegmentZKMetadata) {
    _segmentMetadataPropertiesConfiguration = new PropertiesConfiguration();

    _segmentMetadataPropertiesConfiguration.addProperty(Segment.SEGMENT_CREATOR_VERSION, null);

    _segmentMetadataPropertiesConfiguration.addProperty(Segment.SEGMENT_PADDING_CHARACTER, V1Constants.Str.DEFAULT_STRING_PAD_CHAR);

    _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME,
        Long.toString(offlineSegmentZKMetadata.getStartTime()));
    _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME,
        Long.toString(offlineSegmentZKMetadata.getEndTime()));
    _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.TABLE_NAME,
        offlineSegmentZKMetadata.getTableName());

    final TimeUnit timeUnit = offlineSegmentZKMetadata.getTimeUnit();
    if (timeUnit != null) {
      _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.TIME_UNIT,
          timeUnit.toString());
    } else {
      _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.TIME_UNIT, null);
    }

    _segmentMetadataPropertiesConfiguration.addProperty(Segment.SEGMENT_TOTAL_DOCS,
        offlineSegmentZKMetadata.getTotalRawDocs());

    _crc = offlineSegmentZKMetadata.getCrc();
    _creationTime = offlineSegmentZKMetadata.getCreationTime();
    _pushTime = offlineSegmentZKMetadata.getPushTime();
    _refreshTime = offlineSegmentZKMetadata.getRefreshTime();
    setTimeIntervalAndGranularity();
    _columnMetadataMap = null;
    _segmentName = offlineSegmentZKMetadata.getSegmentName();
    _schema = new Schema();
    _allColumns = new HashSet<String>();
    _indexDir = null;
    _metadataFile = null;
  }

  public SegmentMetadataImpl(RealtimeSegmentZKMetadata segmentMetadata) {

    _segmentMetadataPropertiesConfiguration = new PropertiesConfiguration();

    _segmentMetadataPropertiesConfiguration.addProperty(Segment.SEGMENT_CREATOR_VERSION, null);

    _segmentMetadataPropertiesConfiguration.addProperty(Segment.SEGMENT_PADDING_CHARACTER, V1Constants.Str.DEFAULT_STRING_PAD_CHAR);

    _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME,
        Long.toString(segmentMetadata.getStartTime()));
    _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME,
        Long.toString(segmentMetadata.getEndTime()));
    _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.TABLE_NAME,
        segmentMetadata.getTableName());

    final TimeUnit timeUnit = segmentMetadata.getTimeUnit();
    if (timeUnit != null) {
      _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.TIME_UNIT,
          timeUnit.toString());
    } else {
      _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.TIME_UNIT, null);
    }

    _segmentMetadataPropertiesConfiguration.addProperty(Segment.SEGMENT_TOTAL_DOCS, segmentMetadata.getTotalRawDocs());

    _crc = segmentMetadata.getCrc();
    _creationTime = segmentMetadata.getCreationTime();
    setTimeIntervalAndGranularity();
    _columnMetadataMap = null;
    _segmentName = segmentMetadata.getSegmentName();
    _schema = new Schema();
    _allColumns = new HashSet<String>();
    _indexDir = null;
    _metadataFile = null;
  }

  public SegmentMetadataImpl(RealtimeSegmentZKMetadata segmentMetadata, Schema schema) {
    this(segmentMetadata);
    setSchema(schema);
  }

  public PropertiesConfiguration getSegmentMetadataPropertiesConfiguration() {
    return _segmentMetadataPropertiesConfiguration;
  }

  private void setSchema(Schema schema) {
    for (String columnName : schema.getColumnNames()) {
      _schema.addField(schema.getFieldSpecFor(columnName));
    }
  }

  private void setTimeIntervalAndGranularity() {
    if (_segmentMetadataPropertiesConfiguration.containsKey(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME)
        && _segmentMetadataPropertiesConfiguration.containsKey(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME)
        && _segmentMetadataPropertiesConfiguration.containsKey(V1Constants.MetadataKeys.Segment.TIME_UNIT)) {

      try {
        TimeUnit segmentTimeUnit =
            TimeUtils.timeUnitFromString(_segmentMetadataPropertiesConfiguration.getString(TIME_UNIT));
        _timeGranularity = new Duration(segmentTimeUnit.toMillis(1));
        String startTimeString =
            _segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME);
        String endTimeString =
            _segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME);
        _timeInterval = new Interval(segmentTimeUnit.toMillis(Long.parseLong(startTimeString)),
            segmentTimeUnit.toMillis(Long.parseLong(endTimeString)));
      } catch (Exception e) {
        LOGGER.warn("Caught exception while setting time interval and granularity", e);
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
    if (_segmentMetadataPropertiesConfiguration.containsKey(Segment.SEGMENT_CREATOR_VERSION)) {
      _creatorName = _segmentMetadataPropertiesConfiguration.getString(Segment.SEGMENT_CREATOR_VERSION);
    }

    if (_segmentMetadataPropertiesConfiguration.containsKey(Segment.SEGMENT_PADDING_CHARACTER)) {
      String padding = _segmentMetadataPropertiesConfiguration.getString(Segment.SEGMENT_PADDING_CHARACTER);
      _paddingCharacter = StringEscapeUtils.unescapeJava(padding).charAt(0);
    }

    String versionString = _segmentMetadataPropertiesConfiguration
        .getString(V1Constants.MetadataKeys.Segment.SEGMENT_VERSION, SegmentVersion.v1.toString());
    _segmentVersion = SegmentVersion.valueOf(versionString);

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

    // Column Metadata
    for (final String column : _allColumns) {
      _columnMetadataMap.put(column,
          ColumnMetadata.fromPropertiesConfiguration(column, _segmentMetadataPropertiesConfiguration));
    }

    // Segment Name
    _segmentName = _segmentMetadataPropertiesConfiguration.getString(Segment.SEGMENT_NAME);

    // Set hll log2m
    _hllLog2m = _segmentMetadataPropertiesConfiguration.getInt(Segment.SEGMENT_HLL_LOG2M, HllConstants.DEFAULT_LOG2M);

    // StarTree config here
    _hasStarTree = _segmentMetadataPropertiesConfiguration.getBoolean(
        MetadataKeys.StarTree.STAR_TREE_ENABLED, false);
    if (_hasStarTree) {
      initStarTreeMetadata();
    }

    // Build Schema
    for (final String column : _columnMetadataMap.keySet()) {
      _schema.addField(_columnMetadataMap.get(column).getFieldSpec());
    }
  }

  /**
   * Reads and initializes the star tree metadata from segment metadata properties.
   */
  private void initStarTreeMetadata() {
    _starTreeMetadata = new StarTreeMetadata();

    // Build Derived Column Map
    Map<String, String> hllOriginToDerivedColumnMap = new HashMap<>();
    for (final ColumnMetadata columnMetadata : _columnMetadataMap.values()) {
      MetricFieldSpec.DerivedMetricType derivedMetricType = columnMetadata.getDerivedMetricType();
      if (derivedMetricType != null) {
        switch (derivedMetricType) {
          case HLL:
            hllOriginToDerivedColumnMap.put(columnMetadata.getOriginColumnName(), columnMetadata.getColumnName());
            break;
          default:
            throw new IllegalArgumentException(
                columnMetadata.getDerivedMetricType() + " type is not supported in building derived columns.");
        }
      }
    }
    _starTreeMetadata.setHllOriginToDerivedColumnMap(hllOriginToDerivedColumnMap);

    // Set the maxLeafRecords
    String maxLeafRecordsString =
        _segmentMetadataPropertiesConfiguration.getString(
            MetadataKeys.StarTree.STAR_TREE_MAX_LEAF_RECORDS);
    if (maxLeafRecordsString != null) {
      _starTreeMetadata.setMaxLeafRecords(Long.valueOf(maxLeafRecordsString));
    }

    // Set the splitOrder
    Iterator<String> iterator =
        _segmentMetadataPropertiesConfiguration.getList(MetadataKeys.StarTree.STAR_TREE_SPLIT_ORDER).iterator();
    List<String> splitOrder = new ArrayList<String>();
    while (iterator.hasNext()) {
      final String splitColumn = iterator.next();
      splitOrder.add(splitColumn);
    }
    _starTreeMetadata.setDimensionsSplitOrder(splitOrder);

    // Set dimensions for which star node creation is to be skipped.
    iterator = _segmentMetadataPropertiesConfiguration.getList(
        MetadataKeys.StarTree.STAR_TREE_SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS).iterator();
    List<String> skipStarNodeCreationForDimensions = new ArrayList<String>();
    while (iterator.hasNext()) {
      final String column = iterator.next();
      skipStarNodeCreationForDimensions.add(column);
    }
    _starTreeMetadata.setSkipStarNodeCreationForDimensions(skipStarNodeCreationForDimensions);

    // Set dimensions for which to skip materialization.
    iterator = _segmentMetadataPropertiesConfiguration.getList(
        MetadataKeys.StarTree.STAR_TREE_SKIP_MATERIALIZATION_FOR_DIMENSIONS).iterator();
    List<String> skipMaterializationForDimensions = new ArrayList<String>();

    while (iterator.hasNext()) {
      final String column = iterator.next();
      skipMaterializationForDimensions.add(column);
    }
    _starTreeMetadata.setSkipMaterializationForDimensions(skipMaterializationForDimensions);

    // Skip skip materialization cardinality.
    String skipMaterializationCardinalityString = _segmentMetadataPropertiesConfiguration.getString(
        MetadataKeys.StarTree.STAR_TREE_SKIP_MATERIALIZATION_CARDINALITY);
    if (skipMaterializationCardinalityString != null) {
      _starTreeMetadata.setSkipMaterializationCardinality(Long.valueOf(skipMaterializationCardinalityString));
    }
  }

  public ColumnMetadata getColumnMetadataFor(String column) {
    return _columnMetadataMap.get(column);
  }

  public Map<String, ColumnMetadata> getColumnMetadataMap() {
    return _columnMetadataMap;
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
    return _segmentVersion.toString();
  }

  public SegmentVersion getSegmentVersion() {
    return _segmentVersion;
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
  public int getTotalRawDocs() {
    return _segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_RAW_DOCS, getTotalDocs());
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
    ret.put(V1Constants.MetadataKeys.Segment.TABLE_NAME, getTableName());
    ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_DOCS, String.valueOf(getTotalDocs()));
    ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_VERSION, getVersion());
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

    // determine fields declared in this class only (no fields of superclass)
    final Field[] fields = this.getClass().getDeclaredFields();

    // print field names paired with their values
    for (final Field field : fields) {
      result.append("  ");
      try {
        result.append(field.getName());
        result.append(": ");
        // requires access to private field:
        result.append(field.get(this));
      } catch (final IllegalAccessException ex) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Caught exception while trying to access field {}", field, ex);
        }
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

  @Override
  public boolean hasDictionary(String columnName) {
    return _columnMetadataMap.get(columnName).hasDictionary();
  }

  @Override
  public boolean close() {
    return false;
  }

  @Override
  public boolean hasStarTree() {
    return _hasStarTree;
  }

  @Override
  public StarTreeMetadata getStarTreeMetadata() {
    return _starTreeMetadata;
  }

  @Override
  public String getForwardIndexFileName(String column, String segmentVersion) {
    ColumnMetadata columnMetadata = getColumnMetadataFor(column);
    StringBuilder fileNameBuilder = new StringBuilder(column);
    // starting v2 we will append the forward index files with version
    // if (!SegmentVersion.v1.toString().equalsIgnoreCase(segmentVersion)) {
    // fileNameBuilder.append("_").append(segmentVersion);
    // }
    if (columnMetadata.isSingleValue()) {
      if (columnMetadata.isSorted()) {
        fileNameBuilder.append(V1Constants.Indexes.SORTED_FWD_IDX_FILE_EXTENTION);
      } else {
        fileNameBuilder.append(V1Constants.Indexes.UN_SORTED_SV_FWD_IDX_FILE_EXTENTION);
      }
    } else {
      fileNameBuilder.append(V1Constants.Indexes.UN_SORTED_MV_FWD_IDX_FILE_EXTENTION);
    }
    return fileNameBuilder.toString();
  }

  @Override
  public String getDictionaryFileName(String column, String segmentVersion) {
    return column + V1Constants.Dict.FILE_EXTENTION;
  }

  @Override
  public String getBitmapInvertedIndexFileName(String column, String segmentVersion) {
    return column + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION;
  }

  @Nullable @Override public String getCreatorName() {
    return _creatorName;
  }

  @Override public Character getPaddingCharacter() {
    return _paddingCharacter;
  }

  @Override
  public int getHllLog2m() {
    return _hllLog2m;
  }

}
